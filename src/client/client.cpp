#include "client/client.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <list>
#include <ratio>
#include <utility>

#include <netinet/in.h>

#include "api/api.h"
#include "network/manager.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/switch.h"
#include "util/util.h"

#include <hdr/hdr_histogram.h>

namespace zip {
namespace client {

/** create a logger for this file */
static zip::util::logger logger("client");

client::client(
        zip::network::manager& manager,
        std::string order,
        uint64_t client_id,
        uint64_t shard_id,
        unsigned int cpu_id,
        unsigned long rate
): client_id_(client_id), shard_id_(shard_id), manager_(manager) {
//   zipkat_get_buffers_(manager.get_buffers(zip::consts::PAGE_SIZE, zip::consts::rdma::NUM_RECEIVE_BUFFERS)) {
    // verify arguments for correctness
    ZIP_ASSERT(client_id < zip::consts::MAX_CLIENTS, "invalid client ID");
    ZIP_ASSERT(shard_id < zip::consts::NUM_SHARDS, "invalid shard ID");
    logger.info("Start ziplog client-", client_id, ", cpu_id=", cpu_id, ", rate=", rate, ", connect to order ", order);

#ifdef COLOCATED_ZIPKAT
#if 0
    // TODO: static CONSTANT
    const int kNumQPs = 5;
    const int get_QP = 1;
    //const int get_QP = client_id % (kNumQPs - 1) + 1;
    logger.info("Start ziplog client-", client_id, ", 5 recv queue, get_QP=", get_QP);
    auto queues = manager_.create_recv_queues(kNumQPs);
    recv_queue_ = std::move(queues.at(0));

    // -1 to skip queue-0, which is for InsertAfter
    for (int i = 1; i < kNumQPs; ++i) {
        if (i == get_QP) zipkat_get_recv_queue_ = std::move(queues.at(i));
        else dumb_queues_.emplace_back(std::move(queues.at(i)));
    }
#else
    logger.info("Start ziplog client-", client_id, ", only one recv queue");
    recv_queue_ = std::move(manager_.create_recv_queues(1).front());
#endif
#else
    // create the network receive queue
    recv_queue_ = std::move(manager_.create_recv_queues(1).front());
#endif

    // calculate the number of slots for the client per epoch
    auto slots = zip::util::nearest_power_of_two(rate * zip::util::time_in_us(zip::consts::EPOCH_DURATION) / std::micro::den);

    // initialise the struct which is client's first message
    intro_.message_type = zip::api::CLIENT_INTRO;
    intro_.client_id = client_id;
    intro_.shard_id = shard_id;
    intro_.num_slots = std::max(zip::consts::MIN_SLOTS, slots);

    // connect with the ordering service
    auto [ip, port] = zip::util::split_address(order);
    auto [send_queue, buffer, length] = manager.connect(ip, port, &intro_, intro_.length());
    order_ = std::move(send_queue);

    // verify the ordering server's intro message
    auto& intro = *static_cast<zip::api::order_intro*>(buffer.get());
    ZIP_ASSERT(length == intro.length(), "length of the received packet is invalid");
    ZIP_ASSERT(intro.message_type == zip::api::ORDER_INTRO, "received an unknown type of message (", intro.message_type, ")");
    logger.info("Established connection with the ordering service");

#ifdef COLOCATED_ZIPKAT
    // start the thread for processing client requests
    threads_.emplace_back(std::thread(&client::loop, this));
    zip::util::pin_thread(threads_.back(), cpu_id);
#ifdef ZIPKAT_SEPARATE_THREAD
    threads_.emplace_back(std::thread(&client::zipkat_get_loop, this));
    zip::util::pin_thread(threads_.back(), cpu_id + 2); // avoid hyper-thread
#endif
#else
    // start the thread for processing client requests
    thread_ = std::thread(&client::loop, this);
    zip::util::pin_thread(thread_, cpu_id);
#endif
}

client::~client() {
    // set the stop signal and wait for the thread
    stop_.store(true, std::memory_order_relaxed);
#ifdef COLOCATED_ZIPKAT
    for (auto& t : threads_)
        t.join();
#else
    thread_.join();
#endif
}

void client::insert_after(request& request) {
    // acquire a lock for the client
    while (!busy_.test_and_set(std::memory_order_relaxed));

    // prepare the request to send
    auto& req = request.buffer->as<zip::api::storage_insert_after>();
    ZIP_ASSERT(req.data_length <= zip::consts::MAX_PAYLOAD, "payload is too large");
    req.message_type = zip::api::STORAGE_INSERT_AFTER;
    req.client_id = client_id_;

    // set the request variables and wait for it to be accepted
    request.response.store(-1, std::memory_order_relaxed);
    request_.store(&request, std::memory_order_release);
    while (request_.load(std::memory_order_relaxed) != nullptr);

    // release the client
    busy_.clear(std::memory_order_relaxed);
}

#ifdef COLOCATED_ZIPKAT
void client::zipkat_get(zipkat_get_request& request) {
    // acquire a lock for the client
    while (!zipkat_get_busy_.test_and_set(std::memory_order_relaxed));

    // set the request variables and wait for it to be accepted
    request.timestamp.store(-1, std::memory_order_relaxed);
    zipkat_get_request_.store(&request, std::memory_order_release);
    while (zipkat_get_request_.load(std::memory_order_relaxed) != nullptr);

    // release the client
    zipkat_get_busy_.clear(std::memory_order_relaxed);
}
#endif

namespace detail {

/**
 * This struct stores the information about an epoch.
 */
struct epoch {

    /** timestamp of the beginning of the epoch */
    std::chrono::high_resolution_clock::time_point begin;

    /** index at the start of the epoch */
    unsigned long start;

    /** index of the end of the epoch */
    unsigned long end;

};

/** constant for first epoch */
static constexpr epoch FIRST {std::chrono::high_resolution_clock::time_point::min(), 0, 0};

/** constant for the last epoch */
static constexpr epoch LAST {std::chrono::high_resolution_clock::time_point::max(), 0, 0};

/**
 * This struct stores the data of an outstanding request.
 */
struct outstanding {

    /** location to store the response GSN */
    std::atomic<uint64_t>* response;

    /** the temporary GSN received in an ACK */
    uint64_t gsn;

    /** number of ACKs needed to deliver */
    unsigned long num_acks;

    /** number of slots advanced in the request */
    unsigned long advanced;

#ifdef COLOCATED_ZIPKAT
    /** application return */
    uint64_t* application_return;
#endif

    uint64_t global_client_id;
};

#ifdef COLOCATED_ZIPKAT
struct outstanding_zipkat_get {
    /** zipkat get request */
    zip::client::client::zipkat_get_request* request;

    /** per-get-thread unique message id for identifying the same request */
    unsigned long mid;

    /** the temporary GSN received in an ACK */
    uint64_t gsn;

    /** the temporary value received in an ACK */
    std::string value;

    /** number of ACKs needed to deliver */
    unsigned long num_acks;
};
#endif

} // namespace detail

void client::send_to_replicas(zip::network::buffer& buffer, unsigned long length, unsigned int dest, bool sync) {
    std::shared_lock l(queue_lock_);
    for (auto& [_, queue]: replica_queues_) queue.send(buffer, length, dest, sync);
}
void client::send_to_replicas(void* buffer, unsigned long length, unsigned int dest, bool sync) {
    std::shared_lock l(queue_lock_);
    for (auto& [_, queue]: replica_queues_) queue.send(buffer, length, dest, sync);
}

/**
 * This method processes this client's requests in a loop.
 */
void client::loop() {
    // latest GSN that was delivered
    uint64_t latest_gsn = std::numeric_limits<uint64_t>::max();

    // number of client and subscriber queues for the storage servers
    unsigned int client_queues, destination;
#ifdef COLOCATED_ZIPKAT
    unsigned int zipkat_get_dest;
#endif

    // vector of storage server replica IDs
    std::vector<uint64_t> replicas;

    // vector of storage server network send queues
    std::vector<zip::network::send_queue> queues;

    // state whether to continue the processing loop
    bool order_finished = false, stopped = false, end = false;

    // number of slots consumed and assigned to this client
    unsigned long consumed = 0, assigned = 0;

    // tracking the epochs assigned to this client
    detail::epoch current = detail::FIRST, next = detail::LAST;

    // create a message for sending no-ops
    zip::api::storage_insert_after nop;
    nop.message_type = zip::api::STORAGE_INSERT_AFTER;
    nop.client_id = client_id_;
    nop.data_length = 0;
    nop.global_client_id = 8888;

    // create a message for sending an end request
    zip::api::client_finished finished;
    finished.message_type = zip::api::CLIENT_FINISHED;
    finished.client_id = client_id_;

    zip::api::zipkat_get g;
    g.message_type = zip::api::ZIPKAT_GET;

    // state for the currently outstanding requests
    std::list<detail::outstanding> requests;

#ifdef ZIP_MEASURE
    // initialise the histogram
    hdr_histogram* hist;
    hdr_init(1, 10000, 3, &hist);
    int hdr_count = 0;
    auto start = std::chrono::high_resolution_clock::now();

    hdr_histogram* hist2;
    hdr_init(1, 10000, 3, &hist2);
    int hdr_count2 = 0;
    auto start2 = std::chrono::high_resolution_clock::now();
#endif

#ifdef COLOCATED_ZIPKAT
#ifndef ZIPKAT_SEPARATE_THREAD
    // state for the currently outstanding requests
    std::list<detail::outstanding_zipkat_get> zipkat_get_requests;
    unsigned long get_id = 0;
#endif
#endif

    std::vector<zip::network::send_queue> redundant_queues;

    // keep processing in a loop until shutdown
    while (!end) {
        // check if we need to stop processing
        if (!stopped && (stopped = stop_.load(std::memory_order_relaxed))) {
            // send the finish request to the storage servers
            logger.info("client-", client_id_, " Send BYE");
            send_to_replicas(&finished, finished.length(), destination, /* synchronize */true);
            logger.info("client-", client_id_, " Send BYE done");
        }

        // check if we need to switch to the next epoch
        auto now = std::chrono::high_resolution_clock::now();
        if (next.begin <= now) {
            current = next;
            next = detail::LAST;
        }

#if 1
#ifdef COLOCATED_ZIPKAT
#ifndef ZIPKAT_SEPARATE_THREAD
        if (!stopped && !replicas.empty()) {
            auto get_request = zipkat_get_request_.load(std::memory_order_acquire);
            if (get_request) {
                zipkat_get_request_.store(nullptr, std::memory_order_release);
                zipkat_get_requests.emplace_back(detail::outstanding_zipkat_get {get_request, get_id, -1UL, std::string(), zip::util::more_than_half(replicas.size())});

                g.client_id = client_id_;
                g.mid = get_id;
                g.gsn = 0;
                g.data_length = get_request->key.length();
                std::memcpy(g.key, get_request->key.c_str(), g.data_length);
                send_to_replicas(&g, g.length(), zipkat_get_dest, /* synchronize */false);
                get_id++;
            }
        }
#endif
#endif
#endif

        // based on where we are in the epoch we
        // need to calculate where we should be
        auto ideal = current.start + (current.end - current.start) * zip::util::time_in_us(now - current.begin)
            / zip::util::time_in_us(zip::consts::EPOCH_DURATION);

        // if we can send a request now and we are behind
        // in the log position then send a request
        if (!stopped && !replicas.empty() && consumed < ideal) {
            // check if we have a pending request to send
            auto request = request_.load(std::memory_order_acquire);
            auto advanced = ideal - consumed;

            // there is a client request available, send that
            if (request != nullptr) {
                // save the response location and release the client
                request_.store(nullptr, std::memory_order_relaxed);

                // prepare the request to send
                auto& req = request->buffer->as<zip::api::storage_insert_after>();
                req.num_slots = advanced;

                // create an entry for the outgoing request
                logger.trace("Sending a client request client_id=", req.client_id, ", data_length=", req.data_length, ", advancing by ", advanced, " slots with `gsn_after` ", req.gsn_after, " to dest=", destination, ", global_client_id=", req.global_client_id);
#ifdef COLOCATED_ZIPKAT
                requests.emplace_back(detail::outstanding {&(request->response), std::numeric_limits<uint64_t>::max(), zip::util::more_than_half(replicas.size()), advanced, &(request->application_return), req.global_client_id});
#else
                requests.emplace_back(detail::outstanding {&(request->response), std::numeric_limits<uint64_t>::max(), zip::util::more_than_half(replicas.size()), advanced});
#endif
                consumed += advanced;

                // send the request
                send_to_replicas(*(request->buffer), req.length(), destination, /* synchronize */false);
            }
            // there is no client request but there are no outstanding
            // requests either, so send a no-op
            else if (requests.empty()) {
                // create an outstanding request for the no-op
                logger.trace("Sending a no-op request advancing by ", advanced, " slots to dest", destination);
                requests.emplace_back(detail::outstanding {nullptr, std::numeric_limits<uint64_t>::max(), zip::util::more_than_half(replicas.size()), advanced, nullptr, 8888});
                nop.num_slots = advanced;
                consumed += advanced;

                // send the no-op
                send_to_replicas(&nop, nop.length(), destination, /* synchronize */false);
            }
        }

        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue_,
            [&] (zip::api::client_storage_info& info) {
                // connect to all the servers in the list
                for (unsigned long i = 0; i < info.num_servers; i++) {
                    // verify the server's shard ID
                    auto [send_queues, buffer, length] = manager_.connect(info.addresses[i], &intro_, intro_.length(), 0);

                    // verify the server's intro message
                    auto& intro = *static_cast<zip::api::storage_intro*>(buffer.get());
                    ZIP_ASSERT_EQ(length, intro.length(), "length of the received packet is invalid ", length, ", ", intro.length());
                    ZIP_ASSERT_EQ(intro.message_type, zip::api::STORAGE_INTRO, "received an unknown type of message (", intro.message_type, ")");
                    ZIP_ASSERT_EQ(intro.shard_id, shard_id_, "unknown shard ID");
                    logger.info("Established connection with storage server (", shard_id_, ":", intro.replica_id, "), send_queues.size()=", send_queues.size());

                    // get the number of read and write queues
                    if (replicas.empty()) {
                        client_queues = intro.client_queues;
                        // assign queue destination for handling InsertAfter
#ifdef SUBSCRIBER_THREAD_HANDLE_INSERT_AFTER
                        ZIP_ASSERT(client_queues == 0, "client_queues should be zero when SUBSCRIBER_THREAD_HANDLE_INSERT_AFTER");
                        destination = 1 + client_id_ % intro.subscriber_queues;
#else
                        destination = 1 + client_id_ % client_queues;
#endif

                        // assign queue destination for handling Get request
#ifdef GET_THREAD_HANDLE_GET
                        zipkat_get_dest = 1 + client_queues + client_id_ % intro.get_queues;
#elif SUBSCRIBER_THREAD_HANDLE_GET

#ifdef SUBSCRIBER_THREAD_HANDLE_INSERT_AFTER
                        zipkat_get_dest = 1 + client_id_ % intro.subscriber_queues;
#else
                        zipkat_get_dest = 1 + client_queues + client_id_ % intro.subscriber_queues;
#endif

#else // INSERT_AFTER_THREAD_HANDLE_GET
                        zipkat_get_dest = destination;

#endif
                        logger.info("client_queues=", client_queues, ", destination=", destination, ", zipkat_get_dest=", zipkat_get_dest);
#if (defined COLOCATED_ZIPKAT && defined ZIPKAT_SEPARATE_THREAD)
                        zipkat_get_destination_.store(zipkat_get_dest, std::memory_order_release);
#endif
                     } else {
                         ZIP_ASSERT(client_queues == intro.client_queues, "invalid number of read queues");
                     }

                     // add the new replica to the state
                     auto it = std::find(replicas.begin(), replicas.end(), intro.replica_id);
                     ZIP_ASSERT(it == replicas.end(), "replica ID already exists");
                     replicas.emplace_back(intro.replica_id);
#ifdef COLOCATED_ZIPKAT
                     std::unique_lock l(queue_lock_);
                     // add the new storage server to the state
                     ZIP_ASSERT_ZERO(replica_queues_.count(intro.replica_id), "replica ID already exists");
                     replica_queues_[intro.replica_id] = std::move(send_queues.front());
#ifdef ZIPKAT_SEPARATE_THREAD
                     get_send_queues_.emplace_back(std::move(send_queues.at(1)));
                     for (int i = 2; i < send_queues.size(); ++i)
                         redundant_queues.emplace_back(std::move(send_queues.at(i)));
#else
                     for (int i = 1; i < send_queues.size(); ++i)
                         redundant_queues.emplace_back(std::move(send_queues.at(i)));
#endif
#else
                     queues.emplace_back(std::move(send_queues.front()));
#endif
                }
            },
            [&] ([[maybe_unused]] zip::api::order_finished& bye) {
                // mark the order connection as finalised
                logger.info("Ended connection with ordering service");
                ZIP_ASSERT(!order_finished, "finalise message already received");
                order_finished = true;

                // check if the storage server connections have
                // been finalised, and if so, end this loop
                end = true;
            },
            [&] (zip::api::storage_finished& bye) {
                // remove the storage server from the state
                std::unique_lock l(queue_lock_);
                ZIP_ASSERT_EQ(bye.shard_id, shard_id_, "unknown shard ID");
                ZIP_ASSERT(replica_queues_.count(bye.replica_id) > 0, "unknown replica ID");
                logger.info("Ended connection with storage server (", bye.shard_id, ":", bye.replica_id, ")");
                replica_queues_.erase(bye.replica_id);

                // check if the storage server connections have
                // been finalised, and if so, ask ordering service
                // to finalise
                if (replica_queues_.empty()) order_.send(&finished, finished.length(), true);
            },
            [&] (zip::api::client_new_epoch& epoch) {
                // update the epoch state
                next = {epoch.begin, assigned, assigned += epoch.num_slots};
            },
            [&] (zip::api::client_insert_ack& ack) {
                // handle a client insert ack
                logger.trace("Received ACK from server (", shard_id_, ":", ack.replica_id, ") with GSN ", ack.gsn, ", latest_gsn=", latest_gsn, ", requests.size=", requests.size(), ", global_client_id=", ack.global_client_id);
                // logger.trace("Received ACK from server (", shard_id_, ":", ack.replica_id, ") with GSN ", ack.gsn);

                // if this ACK is for an old request then skip
                //if (latest_gsn != std::numeric_limits<uint64_t>::max() && ack.gsn <= latest_gsn) return;

                // find the request for which this ACK is received
#ifndef SUBSCRIBER_SEND_IA_ACK
                auto pred = [&] (auto& elem) {
                    return (elem.global_client_id == ack.global_client_id) &&
                           (elem.gsn == std::numeric_limits<uint64_t>::max() || ack.gsn == elem.gsn);
                };
#else
                auto pred = [&] (auto& elem) { return elem.gsn == std::numeric_limits<uint64_t>::max() || ack.gsn == elem.gsn;};
#endif

                auto it = std::find_if(requests.begin(), requests.end(), pred);
                if (it != requests.end()) {
                    ZIP_ASSERT(ack.global_client_id == it->global_client_id, "global_client_id unmatched ", ack.global_client_id, " vs ", it->global_client_id);
                    // save the GSN for this request
                    if (it->gsn == std::numeric_limits<uint64_t>::max()) {
                        it->gsn = ack.gsn;
#ifdef COLOCATED_ZIPKAT
                        if (it->response) *it->application_return = ack.app_return;
#endif
                    } else {
                        ZIP_ASSERT(it->gsn == ack.gsn, "unmatched gsn ", it->gsn, " vs ", ack.gsn);
                    }

                    // if all the ACKs for this request
                    // are received, then remove it
                    if (--(it->num_acks) == 0) {
                        // set the response for the client
                        if (it->response != nullptr) {
#ifdef COLOCATED_ZIPKAT
#ifdef ZIP_MEASURE
                            const auto end = std::chrono::high_resolution_clock::now();
                            hdr_record_value(hist, zip::util::time_in_us(end - start));
                            start = end;                                                
                            if (++hdr_count == 100000) {                                 
                                hdr_count = 0;                                          
                                auto lat_50 = hdr_value_at_percentile(hist, 50);         
                                auto lat_99 = hdr_value_at_percentile(hist, 99);         
                                auto lat_999 = hdr_value_at_percentile(hist, 99.9);
                                auto mean = hdr_mean(hist);
                                logger.info("Client-insert_after (", client_id_, ") statistics: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);
                        }
#endif
                            logger.trace("set application_return=", ack.app_return, ", response=", it->gsn);
                            ZIP_ASSERT(*it->application_return == ack.app_return, "commit result unmatched");
#endif
                            it->response->store(it->gsn, std::memory_order_relaxed);
                        }

                        // update state for the delivered ACK
                        consumed += (ack.closed - it->advanced);
                        latest_gsn = it->gsn;

                        // erase the request
                        requests.erase(it);
                    }
                }
            },
#if 0
            [&] (zip::api::zipkat_get_response& ) {
                ZIP_ASSERT(false, "wrong recv queue");
            }
#else
            [&] (zip::api::zipkat_get_response& get_ack) {
                //logger.info("Received zipkat_get_response from server (", shard_id_, ":", get_ack.replica_id, ") with GSN ", get_ack.gsn, " mid=", get_ack.mid, ", client_id=", get_ack.client_id, ", data_length=", get_ack.data_length);

                auto it = zipkat_get_requests.begin();
                for (; it != zipkat_get_requests.end(); it++) {
                    auto req = it->request;
                    //logger.trace("ack.mid=", get_ack.mid, ", it->mid=", it->mid, ", it->gsn=", it->gsn, ", ack.gsn=", get_ack.gsn, ", num_acks=", it->num_acks);
                    if (get_ack.mid == it->mid) {
                        if ((it->gsn == -1) || (it->gsn < get_ack.gsn)) {
                            it->gsn = get_ack.gsn;
                            // TODO: enable value?
                            //it->value = std::string((char*)ack.value, ack.data_length);
                        }

                        // if all the ACKs for this request are received, then remove it
                        if (--(it->num_acks) == 0) {
#ifdef ZIP_MEASURE
                            const auto end2 = std::chrono::high_resolution_clock::now();
                            hdr_record_value(hist2, zip::util::time_in_us(end2 - start2));
                            start2 = end2;
                            if (++hdr_count2 == 100000) {                                 
                                hdr_count2 = 0;                                          
                                auto lat_50 = hdr_value_at_percentile(hist2, 50);
                                auto lat_99 = hdr_value_at_percentile(hist2, 99);
                                auto lat_999 = hdr_value_at_percentile(hist2, 99.9);
                                auto mean = hdr_mean(hist2);
                                logger.info("Client-get (", client_id_, ") statistics: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);
                            }
#endif
                            // return and erase the request
                            // TODO: enable value?
                            //req->value = std::move(it->value);
                            req->timestamp.store(it->gsn, std::memory_order_release);
                            zipkat_get_requests.erase(it);
                        }
    
                        // exit the loop
                        break;
                    }
                }
            }
#endif
        );

#if 0
#ifdef COLOCATED_ZIPKAT
#ifndef ZIPKAT_SEPARATE_THREAD
        zip::util::recv_apply(logger, zipkat_get_recv_queue_,
            [&] (zip::api::zipkat_get_response& get_ack) {
                //logger.info("Received zipkat_get_response from server (", shard_id_, ":", get_ack.replica_id, ") with GSN ", get_ack.gsn, " mid=", get_ack.mid, ", data_length=", get_ack.data_length);

                auto it = zipkat_get_requests.begin();
                for (; it != zipkat_get_requests.end(); it++) {
                    auto req = it->request;
                    //logger.trace("ack.mid=", get_ack.mid, ", it->mid=", it->mid, ", it->gsn=", it->gsn, ", ack.gsn=", get_ack.gsn, ", num_acks=", it->num_acks);
                    if (get_ack.mid == it->mid) {
                        if ((it->gsn == -1) || (it->gsn < get_ack.gsn)) {
                            it->gsn = get_ack.gsn;
                            // TODO: enable value?
                            //it->value = std::string((char*)ack.value, ack.data_length);
                        }

                        // if all the ACKs for this request are received, then remove it
                        if (--(it->num_acks) == 0) {
#ifdef ZIP_MEASURE
                            const auto end2 = std::chrono::high_resolution_clock::now();
                            hdr_record_value(hist2, zip::util::time_in_us(end2 - start2));
                            start2 = end2;
                            if (++hdr_count2 == 100000) {                                 
                                hdr_count2 = 0;                                          
                                auto lat_50 = hdr_value_at_percentile(hist2, 50);
                                auto lat_99 = hdr_value_at_percentile(hist2, 99);
                                auto lat_999 = hdr_value_at_percentile(hist2, 99.9);
                                auto mean = hdr_mean(hist2);
                                logger.info("Client-get (", client_id_, ") statistics: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);
                            }
#endif
                            // return and erase the request
                            // TODO: enable value?
                            //req->value = std::move(it->value);
                            req->timestamp.store(it->gsn, std::memory_order_release);
                            zipkat_get_requests.erase(it);
                        }
    
                        // exit the loop
                        break;
                    }
                }
            },
            [&] (zip::api::client_insert_ack& ) {
                ZIP_ASSERT(false, "client_insert_ack in wrong queue (zipkat_get_recv_queue_)");
            },
        );
#endif
#endif
#endif
    }
}

#if (defined COLOCATED_ZIPKAT && ZIPKAT_SEPARATE_THREAD)
/**
 * This method processes this client's zipkat get requests in a loop.
 */
void client::zipkat_get_loop() {
    // state for the currently outstanding requests
    std::list<detail::outstanding_zipkat_get> zipkat_get_requests;

#ifdef ZIP_MEASURE
    // initialise the histogram
    hdr_histogram* hist;
    hdr_init(1, 10000, 3, &hist);
    int hdr_count = 0;
    auto start = std::chrono::high_resolution_clock::now();
#endif

    zip::api::zipkat_get g;
    g.message_type = zip::api::ZIPKAT_GET;

    unsigned long get_id = 0;

    while (!stop_.load(std::memory_order_relaxed)) {
        size_t num_replicas = 0;
        {
            std::shared_lock l(queue_lock_);
            num_replicas = replica_queues_.size();
        }
        if (num_replicas == 0) continue;

        auto get_request = zipkat_get_request_.load(std::memory_order_acquire);
        if (get_request) {
            zipkat_get_request_.store(nullptr, std::memory_order_release);
            zipkat_get_requests.emplace_back(detail::outstanding_zipkat_get {get_request, get_id, -1UL, std::string(), zip::util::more_than_half(num_replicas)});
            g.client_id = client_id_;
            g.mid = get_id;
            g.gsn = 0;
            g.data_length = get_request->key.length();
            std::memcpy(g.key, get_request->key.c_str(), g.data_length);
            get_id++;
            {
                std::shared_lock l(queue_lock_);
                for (auto& queue: get_send_queues_)
                    queue.send(&g, g.length(), zipkat_get_destination_, /* sync */false);
            }
            // send_to_replicas(*(get_request->buffer), req.length(), zipkat_get_destination_, /* synchronize */false);
        }

        zip::util::recv_apply(logger, zipkat_get_recv_queue_,
            [&] (zip::api::zipkat_get_response& get_ack) {
                logger.trace("ZIPKAT_SEPARATE_THREAD Received zipkat_get_response from server (", shard_id_, ":", get_ack.replica_id, ") with GSN ", get_ack.gsn, " mid=", get_ack.mid, ", data_length=", get_ack.data_length);

                auto it = zipkat_get_requests.begin();
                for (; it != zipkat_get_requests.end(); it++) {
                    auto req = it->request;
                    logger.trace("ack.mid=", get_ack.mid, ", it->mid=", it->mid, ", it->gsn=", it->gsn, ", ack.gsn=", get_ack.gsn, ", num_acks=", it->num_acks);
                    if (get_ack.mid == it->mid) {
                        if ((it->gsn == -1) || (it->gsn < get_ack.gsn)) {
                            it->gsn = get_ack.gsn;
                            // TODO: enable value?
                            //it->value = std::string((char*)ack.value, ack.data_length);
                        }

                        // if all the ACKs for this request are received, then remove it
                        if (--(it->num_acks) == 0) {
#ifdef ZIP_MEASURE
                            const auto end = std::chrono::high_resolution_clock::now();
                            hdr_record_value(hist, zip::util::time_in_us(end - start));
                            start = end;
                            if (++hdr_count == 100000) {                                 
                                hdr_count = 0;                                          
                                auto lat_50 = hdr_value_at_percentile(hist, 50);
                                auto lat_99 = hdr_value_at_percentile(hist, 99);
                                auto lat_999 = hdr_value_at_percentile(hist, 99.9);
                                auto mean = hdr_mean(hist);
                                logger.info("Client-get (", client_id_, ") statistics: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);
                            }
#endif
                            // return and erase the request
                            // TODO: enable value?
                            //req->value = std::move(it->value);
                            req->timestamp.store(it->gsn, std::memory_order_release);
                            zipkat_get_requests.erase(it);
                        }
    
                        // exit the loop
                        break;
                    }
                }
            },
            [&] (zip::api::client_insert_ack& ) {
                ZIP_ASSERT(false, "client_insert_ack in wrong queue (zipkat_get_recv_queue_)");
            }
        );
    }
}
#endif

} // namespace client
} // namespace zip
