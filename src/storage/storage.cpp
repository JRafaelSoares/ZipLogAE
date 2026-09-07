#include "storage/storage.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <hdr/hdr_histogram.h>
#include <iterator>
#include <list>
#include <shared_mutex>
#include <thread>

#include <immintrin.h>

#include "api/api.h"
#include "app/zipkat/zipkat.h"
#include "network/buffer.h"
#include "network/manager.h"
#include "network/recv_queue.h"
#include "util/log.h"
#include "util/switch.h"
#include "util/util.h"

namespace zip {
namespace storage {

#ifdef COLOCATED_ZIPKAT
static_assert(zip::api::zipkat_get_response::kKeyNotFound == app::zipkat::Zipkat::kKeyNotFound);
#endif

/** create a logger for this file */
static zip::util::logger logger("storage");

storage::storage(
        zip::network::manager& manager,
        std::set<unsigned int> client_cpus,
        std::set<unsigned int> subscriber_cpus,
#ifdef COLOCATED_ZIPKAT
        std::set<unsigned int> get_cpus,
#endif
        std::string order,
        zip::api::storage_intro& intro,
        uint64_t shard_id,
        uint64_t replica_id,
        std::unique_ptr<zip::app::Application> app
): manager_(manager), shard_id_(shard_id), replica_id_(replica_id),
   client_threads_(client_cpus.size()), subscriber_threads_(subscriber_cpus.size()),
#ifdef COLOCATED_ZIPKAT
   get_threads_(get_cpus.size()), finishing_client_(-1), app_(std::move(app)) {
#else
    {
#endif
    // verify arguments for correctness
    ZIP_ASSERT(replica_id < zip::consts::MAX_REPLICAS, "invalid replica ID");
    ZIP_ASSERT(shard_id < zip::consts::NUM_SHARDS, "invalid shard ID");
#ifndef SUBSCRIBER_THREAD_HANDLE_INSERT_AFTER
    ZIP_ASSERT(!client_cpus.empty(), "no client threads created");
#endif

    // create the network receive queues
#ifdef COLOCATED_ZIPKAT

#ifdef SUBSCRIBER_THREAD_HANDLE_GET
    auto recv_queues = manager.create_recv_queues(1 + client_threads_ + subscriber_threads_);
#elif defined(GET_THREAD_HANDLE_GET)
    auto recv_queues = manager.create_recv_queues(1 + client_threads_ + get_threads_);
#else
    auto recv_queues = manager.create_recv_queues(1 + client_threads_);
#endif

#else
    auto recv_queues = manager.create_recv_queues(1 + client_threads_ + subscriber_threads_);
#endif
    if (subscriber_threads_ == 0) {
        logger.warn("No subscriber threads created which can cause errors if subscribers try to connect");
    }

    // connect with the ordering service
    auto [ip, port] = zip::util::split_address(order);
    auto [send_queue, buffer, length] = manager.connect(ip, port, &intro, intro.length());
    order_ = std::move(send_queue);

    // verify the ordering server's intro message
    auto& req = *static_cast<zip::api::order_intro*>(buffer.get());
    ZIP_ASSERT(length == req.length(), "length of the received packet is invalid");
    ZIP_ASSERT(req.message_type == zip::api::ORDER_INTRO, "received an unknown type of message (", req.message_type, ")");
    logger.info("Established connection with the ordering service");

    // setup the finished message
    finished_.message_type = zip::api::STORAGE_FINISHED;
    finished_.shard_id = shard_id_;
    finished_.replica_id = replica_id_;

    // index of the receive queue
    unsigned long index = 0;

    // start the thread for processing the control loop
    auto& thread = threads_.emplace_back(&storage::control, this, std::move(recv_queues[index++]));
    // zip::util::pin_thread(thread, 63);

    // start threads for processing the client loop
    for (auto& cpu: client_cpus) {
        // create and pin this thread and add it to the thread vector
        logger.info("client queue, index=", index);
        auto& queue = recv_queues[index++];
        auto& thread = threads_.emplace_back(&storage::client, this, std::move(queue));
        zip::util::pin_thread(thread, cpu);
    }

    // create list for subscriber states and counters
    subscribers_ = std::make_unique<std::list<subscriber_state>[]>(subscriber_threads_);
    num_subscribers_ = std::make_unique<std::atomic<unsigned long>[]>(subscriber_threads_);
    unsigned long subscriber = 0;

    // start threads for processing the subscriber loop
    for (auto& cpu: subscriber_cpus) {
        // create and pin this thread and add it to the thread vector
#if defined(SUBSCRIBER_THREAD_HANDLE_GET) || !defined(COLOCATED_ZIPKAT)
        logger.info("subscriber queue, index=", index);
        auto& queue = recv_queues[index++];
        auto& thread = threads_.emplace_back(&storage::subscriber, this, std::move(queue), subscriber++);
#else
        auto& thread = threads_.emplace_back(&storage::subscriber, this, subscriber++);
#endif
        zip::util::pin_thread(thread, cpu);
    }

#ifdef GET_THREAD_HANDLE_GET
    // TODO: cmdline for cpus, and get_threads_
    // start threads for processing the zipkat get requests
    unsigned int get_thread = 0;
    for (auto& cpu : get_cpus) {
        // create and pin this thread and add it to the thread vector
        logger.info("get queue, index=", index);
        auto& queue = recv_queues[index++];
        auto& thread = threads_.emplace_back(&storage::get_thread, this, std::move(queue), get_thread++);
        zip::util::pin_thread(thread, cpu);
    }
#endif
}

storage::~storage() {
    // set the stop signal and wait for threads
    stop_.store(true, std::memory_order_relaxed);
    for (auto& t: threads_) {
        t.join();
    }

    // free any client state that still remains
    for (auto& state: client_state_) {
        auto ptr = state.load(std::memory_order_relaxed);
        if (!ptr) delete ptr;
    }
}

storage::client_state& storage::get_client_state(uint64_t client_id) {
    ZIP_ASSERT(client_id <= client_state_.size(), "client_id exceeds MAX_CLIENTS");
    // try to find the the client in
    // the state and return if it exists
    auto& location = client_state_.at(client_id);
    auto state = location.load(std::memory_order_acquire);
    if (state != nullptr) return *state;

    // otherwise create the state
    auto allocated = new client_state(client_id, std::ref(log_));

    // we want to swap the nullptr with the newly allocated pointer atomically
    // so perform a compare swap, however, if it fails, then we can say that
    // another thread stored something non-nullptr in there, in which case
    // we can free our allocated state and return that instead
    if (location.compare_exchange_weak(state, allocated, std::memory_order_acq_rel)) {
        return *allocated;
    } else {
        delete allocated;
        return *state;
    }
}

#if 0
void storage::add_client(uint64_t client_id, std::vector<zip::network::send_queue> send_queues) {
    // get or create the state for the client
    // and set the parameters for it
    auto& state = get_client_state(client_id);

    // make sure the client is unique
    ZIP_ASSERT(!state.connected.load(std::memory_order_relaxed), "client ID ", client_id, " already exists");
    logger.info("Established connection with client (", client_id, ")");

    state.send_queue = std::move(send_queues.front());
#ifdef COLOCATED_ZIPKAT
#if 0
    logger.info("expect 5 queues");
    ZIP_ASSERT_EQ(send_queues.size(), 5, "unmatching send_queue size");
    state.get_send_queue = std::move(send_queues.at(1));
    state.dumb_queue = std::move(send_queues.at(2));
    state.dumb_queue2 = std::move(send_queues.at(3));
    state.dumb_queue3 = std::move(send_queues.at(4));
#else
    ZIP_ASSERT_EQ(send_queues.size(), 2, "unmatching send_queue size");
    state.get_send_queue = std::move(send_queues.back());
#endif
#endif
    // mark the state of the client as connected
    state.connected.store(true, std::memory_order_release);

    // there is a colocated app, add a subscriber with the same id
    if (app_) {
        add_subscriber(client_id, /* unspecified */-1, &state.send_queue);
    }
}
#else
void storage::add_client(uint64_t client_id, zip::network::send_queue send_queue) {
    // get or create the state for the client
    // and set the parameters for it
    auto& state = get_client_state(client_id);

    // make sure the client is unique
    ZIP_ASSERT(!state.connected.load(std::memory_order_relaxed), "client ID ", client_id, " already exists");
    logger.info("Established connection with client (", client_id, ")");

    state.send_queue = std::move(send_queue);
    // mark the state of the client as connected
    state.connected.store(true, std::memory_order_release);

    // there is a colocated app, add a subscriber with the same id
    if (app_) {
        add_subscriber(client_id, /* unspecified */-1, &state.send_queue);
    }
}
#endif

inline uint64_t storage::subscriber_id_to_index(uint64_t id) {
    return id % subscriber_threads_;
}

void storage::add_subscriber(uint64_t subscriber_id, uint64_t start_gsn, zip::network::send_queue send_queue) {
    // get the reference to the relevant queue
    auto index = subscriber_id_to_index(subscriber_id);
    auto& subscribers = subscribers_[index];
    auto& num_subscribers = num_subscribers_[index];

    // make sure the subscriber being added is unique
    auto lock = std::unique_lock(subscriber_lock_);
    auto pred = [&] (auto& element) { return element.subscriber_id == subscriber_id; };
    auto it = std::find_if(subscribers.begin(), subscribers.end(), pred);
    ZIP_ASSERT(it == subscribers.end(), "subscriber ID already exists");
    logger.info("Established connection with subscriber (", subscriber_id, ")", ", index(", index, ")");

    // add to the end of the list
    subscribers.emplace_back(subscriber_id, start_gsn, std::move(send_queue));
    num_subscribers.fetch_add(1, std::memory_order_release);
}

#ifdef COLOCATED_ZIPKAT
void storage::add_subscriber(uint64_t subscriber_id, uint64_t start_gsn, zip::network::send_queue* send_queue) {
    // get the reference to the relevant queue
    auto index = subscriber_id_to_index(subscriber_id);
    auto& subscribers = subscribers_[index];
    auto& num_subscribers = num_subscribers_[index];

    // make sure the subscriber being added is unique
    auto lock = std::unique_lock(subscriber_lock_);
    auto pred = [&] (auto& element) { return element.subscriber_id == subscriber_id; };
    auto it = std::find_if(subscribers.begin(), subscribers.end(), pred);
    ZIP_ASSERT(it == subscribers.end(), "subscriber ID already exists");
    logger.info("Established connection with subscriber (", subscriber_id, ")", ", index(", index, ")");

    // add to the end of the list
    subscribers.emplace_back(subscriber_id, start_gsn, send_queue);
    logger.trace("add subscriber_stat [", index, ",", subscribers.size()-1, "]: ", &subscribers.back(), " subscriber_id=", subscriber_id);
    num_subscribers.fetch_add(1, std::memory_order_release);
}
#endif

void storage::control(zip::network::recv_queue recv_queue) {
#ifdef ZIP_MEASURE
    hdr_histogram* hist;                                                            
    hdr_init(1, 10000, 3, &hist);
    int hdr_count = 0;
#endif

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue,
            [&] (zip::api::storage_client_initialize& req) {
                // get this client's state and open the log for this client
                logger.trace("Initialising the log for client (", req.client_id, ")");
                auto& state = get_client_state(req.client_id);
                state.position.open();
            },
            [&] (zip::api::storage_order_slots& slots) {
#ifdef ZIP_MEASURE
                auto start = std::chrono::high_resolution_clock::now();
#endif
                // advance the epoch and add the slots
                log_.add_entries(slots);
#ifdef ZIP_MEASURE
                const auto end = std::chrono::high_resolution_clock::now();
                hdr_record_value(hist, zip::util::time_in_us(end - start));
                if (++hdr_count == 1000) {
                    hdr_count = 0;
                    auto lat_50 = hdr_value_at_percentile(hist, 50);                                
                    auto lat_99 = hdr_value_at_percentile(hist, 99);                                
                    auto lat_999 = hdr_value_at_percentile(hist, 99.9);                             
                    auto mean = hdr_mean(hist);
                    logger.info("Storage STORAGE_ORDER_SLOTS: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);
                }
#endif
                logger.trace("storage_order_slots done");
            },
            [&] (zip::api::storage_client_finalize& req) {
                // get this client's state and close the slots
                logger.info("Closing the log for client (", req.client_id, ")");
                auto& state = get_client_state(req.client_id);
                state.position.close();
                logger.info("Closing the log for client (", req.client_id, ") done");
#ifndef COLOCATED_ZIPKAT
            },
            [&] (zip::api::subscriber_finished& bye) {
                // verify that the subscriber exists
                auto lock = std::unique_lock(subscriber_lock_);
                ZIP_ASSERT(subscribers_.count(bye.subscriber_id) > 0, "subscriber ID does not exist");
                logger.info("Ended connection with subscriber (", bye.subscriber_id, ")");

                // remove the subscriber from the state
                subscribers_.erase(bye.subscriber_id);
                subscriber_changes_.emplace_back(change_t {std::in_place_type<uint64_t>, bye.subscriber_id});
                num_subscriber_changes_.fetch_add(1, std::memory_order_release);
            }
#else
            }
#endif
        );
    }
}

void storage::client(zip::network::recv_queue recv_queue) {
    // state for the current client
    zip::api::client_insert_ack ack;
    ack.message_type = zip::api::CLIENT_INSERT_ACK;
    ack.replica_id = replica_id_;

#ifdef COLOCATED_ZIPKAT
    // response of zipkat GET
    auto get_resp = std::make_unique<zip::api::zipkat_get_response>();
    get_resp->message_type = zip::api::ZIPKAT_GET_RESPONSE;
#endif

#ifdef ZIP_MEASURE
    hdr_histogram* hist;                                                            
    hdr_init(1, 10000, 3, &hist);
    int hdr_count = 0;
#endif

    // TODO: remove
    uint64_t num_noops = 0;
    uint64_t num_ia = 0;

#ifndef SUBSCRIBER_SEND_IA_ACK
    std::list<log::entry*> entries;
#endif
    
    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
#ifndef SUBSCRIBER_SEND_IA_ACK
        auto it = entries.begin();
        int iter = 0;
        //waiting_clients.clear();
        while (it != entries.end()) {
            auto entry = *it;
            auto status = entry->status.load(std::memory_order_relaxed);
            if (status != -1) {
                ack.gsn = entry->gsn;
                ack.closed = entry->closed;
                ack.app_return = status;
                ack.global_client_id = entry->global_client_id;

                auto& state = get_client_state(entry->client_id);
                state.send_queue.send(&ack, ack.length());
                it = entries.erase(it);
                logger.trace("Send InsertAfter Ack, client-", entry->client_id, ", gsn=", ack.gsn, ", global_client_id=", ack.global_client_id);
            }
            ++it;
        }
#endif

        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue,
            [&] (zip::api::storage_insert_after& req) {
#ifdef ZIP_MEASURE
                const auto start = std::chrono::high_resolution_clock::now();
#endif
                // handle a storage insert after request
                logger.trace("Receive STORAGE_INSERT_AFTER client (",req.client_id, ") data_length=", req.data_length, ", gsn_after=", req.gsn_after, ", num_slots=", req.num_slots);

                // get this client's state and make sure it's
                // connected before performing the request
                auto& state = get_client_state(req.client_id);
                while (!state.connected.load(std::memory_order_acquire));

#ifdef SUBSCRIBER_SEND_IA_ACK
                state.position.append_after(req, ack);
#else
                auto& entry = state.position.append_after(req, ack);
#endif

#ifdef COLOCATED_ZIPKAT
                // HACK: assumping all non-zero lengthed InsertAfter are zipkat commit request,
                // which will be handled and returned to the client in subscriber
                if (req.data_length == 0) {
                    // send the ACK back to the client
                    state.send_queue.send(&ack, ack.length());
                    //logger.info("Send InsertAfter Ack (noop), client-", req.client_id, ", gsn=", ack.gsn, ", global_client_id=", ack.global_client_id);
                    //state.get_send_queue.send(&ack2, ack2.length());
                } else {
#ifdef ZIP_MEASURE
                    const auto end = std::chrono::high_resolution_clock::now();
                    hdr_record_value(hist, zip::util::time_in_us(end - start));
                    if (++hdr_count == 100000) {
                        hdr_count = 0;
                        auto lat_50 = hdr_value_at_percentile(hist, 50);
                        auto lat_99 = hdr_value_at_percentile(hist, 99);
                        auto lat_999 = hdr_value_at_percentile(hist, 99.9);
                        auto mean = hdr_mean(hist);
                        logger.info("Storage STORAGE_INSERT_AFTER: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);
                    }
#endif

#ifndef SUBSCRIBER_SEND_IA_ACK
                    entries.push_back(&entry);
#endif
                }
#else
                // send the ACK back to the client
                logger.trace("Send InsertAfter Ack, gsn=", ack.gsn);
                state.send_queue.send(&ack, ack.length());
#endif
            },
            [&] (zip::api::client_finished& bye) {
                // get this client's state and make sure it's connected
                logger.info("Ended connection with client (", bye.client_id, ")");

#ifdef COLOCATED_ZIPKAT
                // FIXME: we remove a subscriber (client) only if this subscriber thread goes pass the last entry of that client.
                // HACK: finish the colocated subscriber first, shouldn't use req.client_id to find subscribers.
                // Assumption: client_id is same as subscriber_id.
                //while (finishing_client_ != -1);
                //finishing_client_.store(bye.client_id, std::memory_order_release);
#endif

                auto& state = get_client_state(bye.client_id);
                while (!state.connected.load(std::memory_order_acquire));

                // mark this client as disconnected and destroy it's send queue
                state.send_queue.send(&finished_, finished_.length(), true);
                auto discard = std::move(state.send_queue);
            },
#if defined(COLOCATED_ZIPKAT) && defined(CLIENT_THREAD_HANDLE_GET)
            [&] (zip::api::zipkat_get& req) {
                // handle a zipkat get request
                logger.trace("Client thread Receive ZIPKAT_GET client (",req.client_id, ")", ", key=", req.key, ", leng=", req.data_length);

                // get this client's state and make sure it's
                // connected before performing the request
                auto& state = get_client_state(req.client_id);
                while (!state.connected.load(std::memory_order_acquire));

                using namespace app::zipkat;
                // Return type is (gsn, value)
                std::pair<uint64_t, std::string> val;
                ZIP_ASSERT(app_, "null app_");
                app_->Execute(app::CommandType::GET, req.key, &val, req.data_length, sizeof(val));
                get_resp->client_id = req.client_id;
                get_resp->replica_id = replica_id_;
                get_resp->mid = req.mid;
                get_resp->gsn = val.first;
                get_resp->data_length = val.second.size();
                memcpy(&get_resp->value, val.second.data(), val.second.length());
                // send the ACK back to the client
                logger.trace("ClientThread Send ZIPKAT_GET_RESPONSE, gsn=", get_resp->gsn);
                state.send_queue.send(reinterpret_cast<void*>(get_resp.get()), get_resp->length(), /* destination */0, /* synchronized */false);
                //state.get_send_queue.send(reinterpret_cast<void*>(get_resp.get()), get_resp->length(), /* destination */1, /* synchronized */false);
            }
#else
            [&] (zip::api::zipkat_get& req) {
                ZIP_ASSERT(false, "wrong queue!");
            }
#endif
        );
    }
}

#ifndef COLOCATED_ZIPKAT
zip::api::subscriber_log_entry& storage::copy_entry(zip::network::buffer& buffer, log::entry* entry) {
    // set the parameters for this entry
    auto& req = buffer.as<zip::api::subscriber_log_entry>();
    req.message_type = zip::api::SUBSCRIBER_LOG_ENTRY;
    req.client_id = entry->client_id;
#if ZIP_CLIENT_SEQ
    req.client_seq = entry->client_seq;
#endif
#if ZIP_SHARD_SEQ
    req.shard_seq = entry->shard_seq;
#endif
    req.data_length = entry->data_length;
    req.replica_id = replica_id_;
    req.shard_id = shard_id_;
    req.gsn = entry->gsn;
#ifdef COLOCATED_ZIPKAT
    req.closed = entry->closed;
#endif
#ifdef ZIP_MEASURE
    req.start = entry->start;
#endif

    // copy the entry data into the buffer
    if (entry->data_length > 0)
        std::memcpy(req.data, entry->data, entry->data_length);

    // return the message
    return req;
}
#endif

void storage::send_entry(subscriber_state& subscriber, log::entry& entry,
                         bool synchronize, unsigned int destination) {
#ifdef COLOCATED_ZIPKAT
#if defined(ZIP_MEASURE) || defined(MEASURE_LOG_ITERATE)
    static hdr_histogram* hist;
    static bool init = false;
    static int hdr_count = 0;
    if (!init) {
        hdr_init(1, 10000, 3, &hist);
        init = true;
    }

    auto start = std::chrono::high_resolution_clock::now();
#endif
    // HACK: this assumpes non-zero lengthed buffer are all zipkat commit request,
    // so perform COMMIT and return to the colocated_queue.
    // Only process the requests belonged to this id.
    ZIP_ASSERT(subscriber.subscriber_id == entry.client_id, "unmatched subscriber_id, client_id");

    // And only non-zero lengthed entry will come to this method.
    ZIP_ASSERT(app_, "null app_");
#if 0
    int status = REPLY_OK;
    entry.locked = true;
#else
    int status = app_->Execute(app::CommandType::COMMIT, &entry, nullptr, entry.data_length, 0);
#endif

    zip::api::client_insert_ack ack;
    ack.message_type = zip::api::CLIENT_INSERT_ACK;
    ack.replica_id = replica_id_;
    ack.gsn = entry.gsn;
    ack.closed = entry.closed;
    ack.app_return = status;
    ZIP_ASSERT(subscriber.colocated_send_queue, "no colocated_send_queue");
    // send_entry processing: median latency: 3 us 99% latency: 5 us       99.9% latency: 5 us     mean: 2.63716.
#if defined(ZIP_MEASURE) || defined(MEASURE_LOG_ITERATE)
    const auto end = std::chrono::high_resolution_clock::now();
    hdr_record_value(hist, zip::util::time_in_us(end - start));
    if (++hdr_count == 100000) {
        hdr_count = 0;
        auto lat_50 = hdr_value_at_percentile(hist, 50);
        auto lat_99 = hdr_value_at_percentile(hist, 99);
        auto lat_999 = hdr_value_at_percentile(hist, 99.9);
        auto mean = hdr_mean(hist);
        logger.info("send_entry processing: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);
    }
#endif

#ifdef SUBSCRIBER_SEND_IA_ACK
    subscriber.colocated_send_queue->send(&ack, ack.length(), destination, synchronize);
#else
    entry.status.store(status, std::memory_order_relaxed);
#endif

#else
    subscriber.send_queue.send(buffer, length, destination, synchronize);
#endif
}

// return true if there is a conflict
static bool has_conflict(const Transaction& txn1, const Transaction& txn2) {
#if 0
    return false;
#else
    const auto& all_key2 = txn2.getKeyIndexes();
    auto k2_iter = all_key2.begin();
    for (const auto& k1 : txn1.getKeyIndexes()) {
        for (auto iter = k2_iter; iter != all_key2.end(); ++iter) {
            const auto& k2 = *iter;
            // logger.info(std::this_thread::get_id(), "] has_conflict ", k1, " vs ", k2);
            if (k1 < k2) break;
            if (k1 == k2) {
                // logger.info(std::this_thread::get_id(), "] has_conflict=true", k1, " vs ", k2);
                return true;
            }
            k2_iter++; // k1 > k2
        }
    }
    // logger.info(std::this_thread::get_id(), "] has_conflict=false");
    return false;
#endif
}

#if defined(SUBSCRIBER_THREAD_HANDLE_GET) || !defined(COLOCATED_ZIPKAT)
void storage::subscriber(zip::network::recv_queue recv_queue, unsigned long subscriber) {
#else
void storage::subscriber(unsigned long subscriber) {
#endif
    auto& num_subscribers = num_subscribers_[subscriber];
    auto& subscribers = subscribers_[subscriber];

    // create a buffers for sending log entries
    auto entries = manager_.get_buffers(zip::consts::BUFFER_SIZES.back(), zip::consts::rdma::MAX_OUTSTANDING_REQUESTS);
    auto temp = zip::util::wraparound_iterator(entries);

    // create a buffers for replaying log entries to new subscribers
    auto replays = manager_.get_buffers(zip::consts::BUFFER_SIZES.back(), zip::consts::rdma::MAX_OUTSTANDING_REQUESTS);

    // create the iterator for stepping over the log
    iterator log(log_);

    // log entry to sent to subscribers
    log::entry* entry = nullptr;

    // current number of subscribers
    unsigned long current_subscribers = 0;

#ifdef COLOCATED_ZIPKAT
    // last unlocked entry
    std::list<log::entry*> txn_conflict_window;
    
    // response of zipkat GET
    auto get_resp_buf = manager_.get_buffers(zip::consts::BUFFER_SIZES.front(), 1);
    auto& get_resp = get_resp_buf.front().as<zip::api::zipkat_get_response>();
    get_resp.message_type = zip::api::ZIPKAT_GET_RESPONSE;
#endif

#if defined(ZIP_MEASURE) || defined(MEASURE_LOG_ITERATE)
    // initialise the histogram                                                     
    hdr_histogram* hist;
    hdr_init(1, 10000, 3, &hist);
    int hdr_count = 0;
    std::chrono::high_resolution_clock::time_point entry_start = std::chrono::high_resolution_clock::now();

    hdr_histogram* hist_check_conflict;
    hdr_init(1, 10000, 3, &hist_check_conflict);
    int hdr_count_check_conflict = 0;

    hdr_histogram* hist_iter;
    hdr_init(1, 10000, 3, &hist_iter);
    std::chrono::high_resolution_clock::time_point iterate_end = std::chrono::high_resolution_clock::now();

    hdr_histogram* hist_dist;
    hdr_init(1, 10000, 3, &hist_dist);
    int prev_gsn = 0;
#endif

#ifdef SUBSCRIBER_THREAD_HANDLE_INSERT_AFTER
#ifdef ZIP_MEASURE
    hdr_histogram* hist_ia;
    hdr_init(1, 10000, 3, &hist_ia);
    int hdr_count_ia = 0;
#endif
    zip::api::client_insert_ack ack;
    ack.message_type = zip::api::CLIENT_INSERT_ACK;
    ack.replica_id = replica_id_;
#endif

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
#if defined(SUBSCRIBER_THREAD_HANDLE_GET) || !defined(COLOCATED_ZIPKAT)
        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue,
#ifdef SUBSCRIBER_THREAD_HANDLE_INSERT_AFTER
            [&] (zip::api::storage_insert_after& req) {
#ifdef ZIP_MEASURE
                const auto start = std::chrono::high_resolution_clock::now();
#endif
                // handle a storage insert after request
                logger.trace("Subscriber thread Receive STORAGE_INSERT_AFTER client (",req.client_id, ") data_length=", req.data_length, ", gsn_after=", req.gsn_after, ", num_slots=", req.num_slots);

                // get this client's state and make sure it's
                // connected before performing the request
                auto& state = get_client_state(req.client_id);
                while (!state.connected.load(std::memory_order_acquire));
                state.position.append_after(req, ack);
#ifdef COLOCATED_ZIPKAT
                // HACK: assumping all non-zero lengthed InsertAfter are zipkat commit request,
                // which will be handled and returned to the client in subscriber
                if (req.data_length == 0) {
                    // send the ACK back to the client
                    logger.trace("Send InsertAfter Ack (noop), client-", req.client_id, ", gsn=", ack.gsn);
                    state.send_queue.send(&ack, ack.length());
                } else {
#ifdef ZIP_MEASURE
                const auto end = std::chrono::high_resolution_clock::now();
                hdr_record_value(hist, zip::util::time_in_us(end - start));
                if (++hdr_count_ia == 100000) {
                    hdr_count_ia = 0;
                    auto lat_50 = hdr_value_at_percentile(hist_ia, 50);
                    auto lat_99 = hdr_value_at_percentile(hist_ia, 99);
                    auto lat_999 = hdr_value_at_percentile(hist_ia, 99.9);
                    auto mean = hdr_mean(hist_ia);
                    logger.info("Storage STORAGE_INSERT_AFTER: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);
                }
#endif
                }
#else
                // send the ACK back to the client
                logger.trace("Send InsertAfter Ack, gsn=", ack.gsn);
                state.send_queue.send(&ack, ack.length());
#endif
            },
            [&] (zip::api::client_finished& bye) {
                // get this client's state and make sure it's connected
                logger.info("Ended connection with client (", bye.client_id, ")");

#ifdef COLOCATED_ZIPKAT
                // FIXME: we remove a subscriber (client) only if this subscriber thread goes pass the last entry of that client.
                // HACK: finish the colocated subscriber first, shouldn't use req.client_id to find subscribers.
                // Assumption: client_id is same as subscriber_id.
                //while (finishing_client_ != -1);
                //finishing_client_.store(bye.client_id, std::memory_order_release);
#endif

                auto& state = get_client_state(bye.client_id);
                while (!state.connected.load(std::memory_order_acquire));

                // mark this client as disconnected and destroy it's send queue
                state.send_queue.send(&finished_, finished_.length(), true);
                auto discard = std::move(state.send_queue);
            },
#endif
            [&] (zip::api::zipkat_get& req) {
                // handle a zipkat get request
                logger.trace("subscriber Receive ZIPKAT_GET client (",req.client_id, ")", ", key=", req.key, ", leng=", req.data_length);
    
                // get this client's state and make sure it's
                // connected before performing the request
                auto& state = get_client_state(req.client_id);
                while (!state.connected.load(std::memory_order_acquire));
    
                using namespace app::zipkat;
                // Return type is (gsn, value)
                std::pair<uint64_t, std::string> val;
                ZIP_ASSERT(app_, "null app_");
                app_->Execute(app::CommandType::GET, req.key, &val, req.data_length, sizeof(val));
                memcpy(&get_resp.value, val.second.data(), val.second.length());
                get_resp.client_id = req.client_id;
                get_resp.replica_id = replica_id_;
                get_resp.mid = req.mid;
                get_resp.gsn = val.first;
                get_resp.data_length = val.second.size();
                // send the ACK back to the client
                logger.trace("subscriber Send ZIPKAT_GET_RESPONSE, gsn=", get_resp.gsn, ", data_length=", get_resp.data_length, ", buffer leng=", get_resp.length(), " to dest=1", ", mid=", get_resp.mid);
                //state.get_send_queue.send(get_resp_buf.front(), get_resp.length(), /* destination */1, /* synchronized */false);
                //state.send_queue.send(get_resp_buf.front(), get_resp.length(), /* destination */1, /* synchronized */false);
            },
            [&] (zip::api::storage_insert_after& req) {
                ZIP_ASSERT(false, "insert_after gets to get_queue!");
            }
        );
#endif


#if 0
        // FIXME: we remove a subscriber (client) only if this subscriber thread goes pass the last entry of that client.
//#ifdef COLOCATED_ZIPKAT
        const auto finishing_client = finishing_client_.load(std::memory_order_acquire);
        if (finishing_client != -1) {
            ZIP_ASSERT(finishing_client != -1, "Invalid finishing client");
            const auto index = subscriber_id_to_index(finishing_client);
            if (index == subscriber) {
                auto& subscribers = subscribers_[index];
                auto lock = std::unique_lock(subscriber_lock_);
                auto pred =  [&] (auto& element) { return element.subscriber_id == finishing_client; };
                auto it = std::find_if(subscribers.begin(), subscribers.end(), pred);
                ZIP_ASSERT(it != subscribers.end(), "could not find the subscriber ", finishing_client, ", index=", index);

                // remove the subscriber from the list
                auto& num_subscribers = num_subscribers_[index];
                num_subscribers.fetch_add(-1, std::memory_order_relaxed);
                logger.info("Remove subscriber-", it->subscriber_id);
                subscribers.erase(it);
                finishing_client_.store(-1, std::memory_order_relaxed);
            }
        }
#endif

        // check if the current number of subscribers is still valid
        auto read_value = num_subscribers.load(std::memory_order_acquire);
        if (current_subscribers != read_value) {
/*
            if (read_value > current_subscribers) {
                logger.trace("Adding ", read_value - current_subscribers, " new subscribers, read_value=", read_value, ", current_subscribers=", current_subscribers);
                // if we have advanced the log ahead then
                // send the earlier entries to the new subscribers
                if (entry != nullptr) {
                    auto it = subscribers.begin();
                    std::advance(it, current_subscribers);
                    for (unsigned long i = current_subscribers; i < read_value; i++, it++) {
                        if (it->start_gsn != -1 && it->start_gsn <= entry->gsn) {
                            // logger.trace("Sending entries to subscriber (", it->subscriber_id, ") starting from GSN ", it->start_gsn, " to ", entry->gsn);
                            // restart the log from an earlier position
                            auto temp = zip::util::wraparound_iterator(replays);
                            iterator log(log_, it->start_gsn);
                            log::entry* old = nullptr;

                            // send all entries to the subscriber until the current one
                            while (old != entry && log.next_entry(old)) {
#ifdef COLOCATED_ZIPKAT
                                if (old->data_length == 0) {
                                    // Skip noop
                                    continue;
                                }
#endif
                                // get a buffer, copy data into it, and send it
                                auto buffer = temp.get_and_increment();
                                auto& req = copy_entry(*buffer, old);
                                logger.trace("Send1 GSN ", req.gsn, " to subscriber data_length=", entry->data_length);
                                send_entry(*it, *buffer, req.length(), old == entry);
                            }
                        }
                    }
                }
            }
*/

            // set the number of current subscribers
            current_subscribers = read_value;
        }

        // try to get an entry from the log
        if (log.next_entry(entry)) {
#ifdef COLOCATED_ZIPKAT
            // Skip noop or the entry that's already locked (implying this thread doesn't have to handle it)
            if (entry->data_length == 0 || entry->locked.load(std::memory_order_relaxed))
                continue;

            const auto client_id = entry->client_id;
            if (subscriber_id_to_index(client_id) != subscriber) {
                txn_conflict_window.push_back(entry);
                continue;
            }
#else
            // get the buffer and initialise the pointer to the entry
            auto buffer = temp.get_and_increment();
#endif

            //logger.trace("subscriber_thread_id=", subscriber, ", entry->gsn=", entry->gsn, ", entry->client_id=", entry->client_id, ", data_leng=", entry->data_length);
            // whether the entry is handled
            bool handled = false;

            // send the entry to all subscribers
            for (auto& it : subscribers) {
                //if (it.start_gsn == -1 || entry->gsn >= it.start_gsn) {
#ifdef COLOCATED_ZIPKAT
                // TODO: remove for optimizing performance
                // if (client_id == it.subscriber_id && (it.start_gsn == -1 || entry->gsn >= it.start_gsn)) {
                if (client_id == it.subscriber_id) {
#if defined(ZIP_MEASURE) || defined(MEASURE_LOG_ITERATE)
                        const auto end = std::chrono::high_resolution_clock::now();
                        // measure between the time when entry get appended and the time when the entry gets processed
                        //hdr_record_value(hist, zip::util::time_in_us(end - entry->start));
                        // measure between two entries begins to be processed
                        hdr_record_value(hist, zip::util::time_in_us(end - entry_start));
                        entry_start = end;

                        // measure between the time this entry begins to be processed and the end time of previously processed entry
                        hdr_record_value(hist_iter, zip::util::time_in_us(end - iterate_end));

                        // record the distance between this entry and the previously handled one
                        hdr_record_value(hist_dist, entry->gsn - prev_gsn);
                        prev_gsn = entry->gsn;
                        
                        if (++hdr_count == 100000) {
                            hdr_count = 0;
                            auto lat_50 = hdr_value_at_percentile(hist, 50);                                
                            auto lat_99 = hdr_value_at_percentile(hist, 99);                                
                            auto lat_999 = hdr_value_at_percentile(hist, 99.9);                             
                            auto mean = hdr_mean(hist);
                            logger.info("Subscriber (", subscriber, ") statistics: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);

                            lat_50 = hdr_value_at_percentile(hist_iter, 50);                                
                            lat_99 = hdr_value_at_percentile(hist_iter, 99);                                
                            lat_999 = hdr_value_at_percentile(hist_iter, 99.9);                             
                            mean = hdr_mean(hist_iter);
                            logger.info("Subscriber (", subscriber, ") iterate: median: ", lat_50, " \t99%: ", lat_99, " \t99.9%: ", lat_999, " \tmean: ", mean);

                            lat_50 = hdr_value_at_percentile(hist_dist, 50);                                
                            lat_99 = hdr_value_at_percentile(hist_dist, 99);                                
                            lat_999 = hdr_value_at_percentile(hist_dist, 99.9);                             
                            mean = hdr_mean(hist_dist);
                            logger.info("Subscriber (", subscriber, ") distance: median: ", lat_50, " \t99%: ", lat_99, " \t99.9%: ", lat_999, " \tmean: ", mean);
                        }
#endif
                        
#if 1
#if defined(ZIP_MEASURE) || defined(MEASURE_LOG_ITERATE)
                        const auto conflict_start = std::chrono::high_resolution_clock::now();
#endif
                        // we can proceed only if
                        //  1) all the previous slot are locked
                        //  2) this entry doesn't have conflicted keys with a previous unlocked entry
                        auto c_it = txn_conflict_window.begin();
                        //logger.info("txn_conflict_window.size=", txn_conflict_window.size());


                        while (c_it != txn_conflict_window.end()) {
                            const auto& c_entry = *c_it;
                            if (c_entry->locked.load(std::memory_order_relaxed)) {
                                c_it = txn_conflict_window.erase(c_it);
                            } else {
                                // Check conflicts
                                if (has_conflict(*c_entry->txn, *entry->txn)) {
                                    while (!c_entry->locked.load(std::memory_order_relaxed)) { _mm_pause(); }
                                    c_it = txn_conflict_window.erase(c_it);
                                } else {
                                    ++c_it;
                                }
                            }
                        }

#if defined(ZIP_MEASURE) || defined(MEASURE_LOG_ITERATE)
                    const auto conflict_end = std::chrono::high_resolution_clock::now();
                    hdr_record_value(hist_check_conflict, zip::util::time_in_us(conflict_end - conflict_start));
                    if (++hdr_count_check_conflict == 100000) {
                        hdr_count_check_conflict = 0;
                        auto lat_50 = hdr_value_at_percentile(hist_check_conflict, 50);
                        auto lat_99 = hdr_value_at_percentile(hist_check_conflict, 99);
                        auto lat_999 = hdr_value_at_percentile(hist_check_conflict, 99.9);
                        auto mean = hdr_mean(hist_check_conflict);
                        logger.info("CheckConflict (", subscriber, ") statistics: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean);
                    }
#endif

#endif
                        send_entry(it, *entry);
                        handled = true;
#else
                        auto& req = copy_entry(*buffer, entry);
                        send_entry(it, *buffer, req.length());
#endif

#ifdef COLOCATED_ZIPKAT
                        // only one subscriber's id matches the entry's client's id
                        break;
                    }
#endif
                //}
            }
#ifdef COLOCATED_ZIPKAT
            if (!handled && !entry->locked.load(std::memory_order_relaxed)) {
                txn_conflict_window.push_back(entry);
            }
#if defined(ZIP_MEASURE) || defined(MEASURE_LOG_ITERATE)
            iterate_end = std::chrono::high_resolution_clock::now();
#endif
#endif
        }
    }
    logger.info("Subscriber (", subscriber, ") thread finishes");
}

#if GET_THREAD_HANDLE_GET
void storage::get_thread(zip::network::recv_queue recv_queue, unsigned int get_thread_id) {
    // response of zipkat GET
    zip::api::zipkat_get_response g;
    g.message_type = zip::api::ZIPKAT_GET_RESPONSE;

#ifdef ZIP_MEASURE
    // initialise the histogram
    hdr_histogram* hist;
    hdr_init(1, 10000, 3, &hist);
    int hdr_count = 0;

    // TODO: removed
    int handled = 0;
    hdr_histogram* hist_cs;
    hdr_histogram* hist_send;
    hdr_init(1, 10000, 3, &hist_cs);
    hdr_init(1, 10000, 3, &hist_send);
#endif

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue,
            [&] (zip::api::zipkat_get& req) {
                // handle a zipkat get request
#ifdef SUBSCRIBER_THREAD_HANDLE_GET
                ZIP_ASSERT(false, "Wrong thread");
#endif

#ifdef ZIP_MEASURE
                const auto start = std::chrono::high_resolution_clock::now();
#endif
    
                // get this client's state and make sure it's
                // connected before performing the request
                auto& state = get_client_state(req.client_id);
                while (!state.connected.load(std::memory_order_acquire));
#ifdef ZIP_MEASURE
                const auto get_cs_end = std::chrono::high_resolution_clock::now();
                hdr_record_value(hist_cs, zip::util::time_in_us(get_cs_end - start));
#endif
    
                using namespace app::zipkat;
                // Return type is (gsn, value)
                std::pair<uint64_t, std::string> val;
                ZIP_ASSERT(app_, "null app_");
                app_->Execute(app::CommandType::GET, req.key, &val, req.data_length, sizeof(val));
                memcpy(&g.value, val.second.data(), val.second.length());
                g.client_id = req.client_id;
                g.replica_id = replica_id_;
                g.mid = req.mid;
                g.gsn = val.first;
                g.data_length = val.second.size();

#ifdef ZIP_MEASURE
                const auto send_start = std::chrono::high_resolution_clock::now();
#endif
                // TODO: refine: 4 to be CONSTANT
                //const int get_dest = (req.client_id % 4) + 1;
                //state.get_send_queue.send(&g, g.length(), /* destination */get_dest, /* synchronized */false);
                state.get_send_queue.send(&g, g.length(), /* destination */1, /* synchronized */false);
                //state.send_queue.send(&g, g.length(), /* destination */0, /* synchronized */false);
#ifdef ZIP_MEASURE
                hdr_record_value(hist_send, zip::util::time_in_us(std::chrono::high_resolution_clock::now() - send_start));
#endif

                // send the ACK back to the client
                //logger.info("get_thread Send ZIPKAT_GET_RESPONSE, gsn=", g.gsn, ", data_length=", g.data_length, ", buffer leng=", g.length(), " to client-", g.client_id, ", mid=", g.mid, ", get_dest=", get_dest);
#ifdef ZIP_MEASURE
                const auto end = std::chrono::high_resolution_clock::now();
                hdr_record_value(hist, zip::util::time_in_us(end - start));
                ++handled;
                if (++hdr_count == 100000) {
                    hdr_count = 0;
                    auto lat_50 = hdr_value_at_percentile(hist, 50);
                    auto lat_99 = hdr_value_at_percentile(hist, 99);
                    auto lat_999 = hdr_value_at_percentile(hist, 99.9);
                    auto mean = hdr_mean(hist);
                    logger.info("get_thread ZIPKAT_GET (", get_thread_id, ") : median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean, ", handled=", handled);
                    lat_50 = hdr_value_at_percentile(hist_cs, 50);
                    lat_99 = hdr_value_at_percentile(hist_cs, 99);
                    lat_999 = hdr_value_at_percentile(hist_cs, 99.9);
                    mean = hdr_mean(hist_cs);
                    logger.info("get_thread ZIPKAT_GET2 (", get_thread_id, ") : median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean, ", handled=", handled);
                    lat_50 = hdr_value_at_percentile(hist_send, 50);
                    lat_99 = hdr_value_at_percentile(hist_send, 99);
                    lat_999 = hdr_value_at_percentile(hist_send, 99.9);
                    mean = hdr_mean(hist_send);
                    logger.info("get_thread ZIPKAT_GET3 (", get_thread_id, ") : median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us\tmean: ", mean, ", handled=", handled);
                }
#endif
            },
            [&] (zip::api::storage_insert_after& req) {
                ZIP_ASSERT(false, "insert_after gets to get_queue!");
            }
        );
    }
}
#endif

} // namespace storage
} // namespace zip
