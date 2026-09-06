#include "client/client.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <compare>
#include <cstdint>
#include <limits>
#include <list>
#include <optional>
#include <ratio>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <netinet/in.h>

#include "api/api.h"
#include "network/buffer.h"
#include "network/manager.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/switch.h"
#include "util/util.h"

namespace zip::client {

/** create a logger for this file */
static zip::util::logger logger("client");

client::client(
        zip::network::manager& manager,
        std::string order,
        uint64_t client_id,
        uint64_t shard_id,
        uint16_t cpu_id,
        unsigned long rate,
        unsigned long failures
):
client_id_(client_id), shard_id_(shard_id), failures_(failures), manager_(manager), estimate_(false) {
    // verify arguments for correctness
    ZIP_ASSERT(client_id_ < zip::consts::MAX_CLIENTS, "invalid client ID");
    ZIP_ASSERT(shard_id_ < zip::consts::NUM_SHARDS, "invalid shard ID");

    // calculate the number of slots based on rate
    auto num_slots = zip::util::align<zip::consts::MIN_SLOTS>(std::max(1UL, rate * zip::consts::EPOCH_DURATION / std::chrono::seconds(1)));

    // initialize the struct which is client's first message
    intro_.message_type = zip::api::CLIENT_INTRO;
    intro_.client_id = client_id;
    intro_.shard_id = shard_id;
    intro_.num_slots = num_slots;

    // initialize the client
    initialize_client(order, cpu_id);
}

client::client(
        zip::network::manager& manager,
        std::string order,
        uint64_t client_id,
        uint64_t shard_id,
        uint16_t cpu_id,
        pid pid,
        unsigned long failures
):
client_id_(client_id), shard_id_(shard_id), failures_(failures), manager_(manager), estimate_(true), pid_(pid) {
    // verify arguments for correctness
    ZIP_ASSERT(client_id_ < zip::consts::MAX_CLIENTS, "invalid client ID");
    ZIP_ASSERT(shard_id_ < zip::consts::NUM_SHARDS, "invalid shard ID");

    // initialize the struct which is client's first message
    intro_.message_type = zip::api::CLIENT_INTRO;
    intro_.client_id = client_id;
    intro_.shard_id = shard_id;
    intro_.num_slots = zip::consts::MIN_SLOTS;

    // initialize the client
    initialize_client(order, cpu_id);
}

void client::initialize_client(std::string order, uint16_t cpu_id) {
    // create the network receive queue
    recv_queue_ = std::move(manager_.create_recv_queues(1).front());

    // connect with the ordering service
    auto [ip, port] = zip::util::split_address(order);
    auto connection = manager_.connect(ip, port, &intro_, intro_.length());
    ZIP_ASSERT(connection, "failed to connect to the ordering service");
    auto& [send_queue, buffer, length] = *connection;
    order_ = std::move(send_queue);

    // verify the ordering server's intro message
    auto& intro = *static_cast<zip::api::order_intro*>(buffer);
    ZIP_ASSERT_EQ(intro.length(), length, "length of the received packet is invalid");
    ZIP_ASSERT_EQ(intro.message_type, zip::api::ORDER_INTRO, "received an unknown type of message");
    logger.info("Established connection with the ordering service");

    // start the thread for processing client requests
    thread_ = std::thread(&client::loop, this);
    zip::util::pin_thread(thread_, cpu_id);
}

void client::stop(bool trigger_failure) {
    if (trigger_failure) {
        force_stop_.store(true, std::memory_order_relaxed);
        thread_.join();
        // trigger RDMA failures
        manager_.deactivate_srq(recv_queue_);
    } else {
        // set the stop signal and wait for the thread
        stop_.store(true, std::memory_order_relaxed);
        thread_.join();
    }
}

void client::insert(zip::network::buffer& request, std::atomic<uint64_t>& response) {
    // prepare the request to send
    auto& req = request.as<zip::api::storage_insert>();
    ZIP_ASSERT(req.data_length <= zip::consts::MAX_PAYLOAD, "payload is too large");
    req.message_type = zip::api::STORAGE_INSERT;
    req.client_id = client_id_;

    // prepare the response buffer
    response.store(std::numeric_limits<uint64_t>::max(), std::memory_order_relaxed);

    // enqueue the request after taking the lock
    auto lock = std::unique_lock(lock_);
    requests_.enqueue(std::make_pair(&request, &response));
}

namespace detail {

/**
 * This struct stores the information about an epoch.
 */
struct epoch {

    /** timestamp of the beginning of the epoch */
    std::chrono::high_resolution_clock::time_point begin;

    /** index at the start of the epoch */
    unsigned long first;

    /** index of the end of the epoch */
    unsigned long last;

};

/** constant for first epoch */
static constexpr epoch FIRST_EPOCH {std::chrono::high_resolution_clock::time_point::min(), 0, 0};

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

};

} // namespace detail

enum LockAction {
    Lock,
    Unlock
};

struct LockRequest {
    LockAction action;
    uint64_t client;
    uint64_t lock_key;
    std::atomic<bool>* done;
};

/**
 * This method processes this client's requests in a loop.
 */
void client::loop() {
    // latest GSN that was delivered
    uint64_t latest_gsn = std::numeric_limits<uint64_t>::max();

    // number of client and subscriber queues for the storage servers
    unsigned long client_queues, destination;

    // map from replica IDs to storage server network send queue
    std::unordered_map<uint64_t, zip::network::send_queue> queues;

    // state of whether to continue the processing loop
    bool stopped = false, end = false;

    // number of slots consumed and assigned to this client
    unsigned long consumed = 0, assigned = 0;

    // tracking the epochs assigned to this client
    auto current = detail::FIRST_EPOCH;
    std::list<detail::epoch> epochs;

    // tracking the state for rate estimation using PID
    double current_rate = zip::consts::MIN_SLOTS, epoch_requests = 0, last_error = 0, total_error = 0;

    // create a message for sending no-ops
    zip::api::storage_insert nop;
    nop.message_type = zip::api::STORAGE_INSERT;
    nop.client_id = client_id_;
    nop.data_length = 0;

    // create a message for sending rate change requests
    zip::api::order_client_rate req;
    req.message_type = zip::api::ORDER_CLIENT_RATE;
    req.client_id = client_id_;
    req.num_slots = zip::consts::MIN_SLOTS;

    // create a message for sending an end request
    zip::api::client_finished finished;
    finished.message_type = zip::api::CLIENT_FINISHED;
    finished.client_id = client_id_;

    // state for the currently outstanding requests
    std::list<detail::outstanding> outstanding;

    // keep processing in a loop until shutdown
    while (!force_stop_.load(std::memory_order_relaxed) && !end) {
        // check if we need to stop processing
        if (!stopped && (stopped = stop_.load(std::memory_order_relaxed))) {
            // send the finish request to the storage servers
            for (auto& [_, queue]: queues) {
                queue.send(&finished, finished.length(), destination);
            }
        }

        // check if we need to switch to the next epoch
        auto now = std::chrono::high_resolution_clock::now();
        if (!epochs.empty() && epochs.front().begin <= now) {
            // check if we need to perform rate estimation
            if (estimate_ && current.begin != detail::FIRST_EPOCH.begin) {
                // calculate the error and update the current rate
                auto fraction_used = epoch_requests / (current.last - current.first);
                auto error = fraction_used - pid_.target;
                current_rate += pid_.proportional * error
                              + pid_.integral     * total_error
                              + pid_.detivative   * (last_error - error);
                current_rate = std::max<double>(zip::consts::MIN_SLOTS, current_rate);

                // update the PID state
                total_error += error;
                last_error = error;

                // if the number of slots changed, then send a request
                auto quantized = zip::consts::MIN_SLOTS * (std::round<unsigned long>(current_rate / zip::consts::MIN_SLOTS));
                if (quantized != req.num_slots) {
                    logger.trace("Requesting ", quantized, " slots from the ordering service");
                    req.num_slots = quantized;
                    order_.send(&req, req.length());
                }
            }

            // switch to the next epoch
            current = epochs.front();
            epochs.pop_front();
            epoch_requests = 0;
#if 0
            logger.debug("switch to new epoch, first=", current.first, ", last=", current.last,
                         ", begin=", zip::util::time_in_us(current.begin - std::chrono::milliseconds(0)),
                         ", now=", zip::util::time_in_us(now - std::chrono::milliseconds(0)),
                         ", now-begin=", zip::util::time_in_us(now - current.begin));
#endif
        }

        // based on where we are in the epoch we
        // need to calculate where we should be
        auto ideal = current.first + (current.last - current.first) * zip::util::time_in_us(now - current.begin)
            / zip::util::time_in_us(zip::consts::EPOCH_DURATION);
        //logger.debug("ideal=", ideal, ", consumed=", consumed, ", outstanding.size=", outstanding.size(), ", MAX_OUTSTANDING_REQUESTS=", zip::consts::rdma::MAX_OUTSTANDING_REQUESTS, ", now-begin=", zip::util::time_in_us(now - current.begin));

        // if we can send a request now and we are behind
        // in the log position then send a request
        if (   !stopped
            && consumed < ideal
            && queues.size() > failures_
            && outstanding.size() <= zip::consts::rdma::MAX_OUTSTANDING_REQUESTS
            && now - current.begin <= zip::consts::EPOCH_DURATION) {
            // calculate how behind we are on the schedule
            auto advanced = ideal - consumed;

#if 0
            ZIP_ASSERT(current.first + advanced <= current.last, "advanced=", advanced, ", first=", current.first, ", last=", current.last,
                       ", begin=", zip::util::time_in_us(current.begin - std::chrono::milliseconds(0)),
                       ", now=", zip::util::time_in_us(now - std::chrono::milliseconds(0)),
                       ", now-begin=", zip::util::time_in_us(now - current.begin));
#endif

            // there is a client request available then send that, otherwise
            // if there there are no outstanding requests then send a no-op
            if (auto request = requests_.dequeue()) {
                // prepare the request to send
                auto& req = request->first->as<zip::api::storage_insert>();
                req.num_slots = advanced;

                auto& lock_req = *reinterpret_cast<LockRequest*>(req.data);
                // create an entry for the outgoing request
                logger.trace("Sending a client request advancing by ", advanced, " slots");
                //logger.info("Sending a client request advancing by ", advanced, " slots. action=", lock_req.action, ", client=", lock_req.client, ", key=", lock_req.lock_key);
                //logger.info("Sending a client request advancing by ", advanced, " slots");
                outstanding.emplace_back(detail::outstanding {request->second, std::numeric_limits<uint64_t>::max(), 0});
                consumed += advanced;
                epoch_requests++;

                // send the request
                for (auto& [_, queue]: queues) queue.send(*request->first, req.length(), destination);
            } else if (outstanding.empty()) {
                // create an entry for the outgoing no-op
                logger.trace("Sending a no-op request advancing by ", advanced, " slots");
                //logger.info("Sending a no-op request advancing by ", advanced, " slots");
                outstanding.emplace_back(detail::outstanding {nullptr, std::numeric_limits<uint64_t>::max(), 0});
                nop.num_slots = advanced;
                consumed += advanced;

                // send the no-op
                for (auto& [_, queue]: queues) queue.send(&nop, nop.length(), destination);
            }
        }

        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue_,
            [&] (zip::api::client_insert_ack& ack) {
                // if this ACK is for an old request then skip
                //logger.info("Received ACK from server (", shard_id_, ":", ack.replica_id, ") with GSN ", ack.gsn);
                logger.trace("Received ACK from server (", shard_id_, ":", ack.replica_id, ") with GSN ", ack.gsn);
                if (latest_gsn != std::numeric_limits<uint64_t>::max() && ack.gsn <= latest_gsn) return;

                // find the request for which this ACK is received
                auto pred = [&] (auto& elem) { return elem.gsn == std::numeric_limits<uint64_t>::max() || ack.gsn == elem.gsn; };
                auto it = std::find_if(outstanding.begin(), outstanding.end(), pred);
                if (it != outstanding.end()) {
                    // save the GSN for this request
                    if (it->gsn == std::numeric_limits<uint64_t>::max()) it->gsn = ack.gsn;

                    // if all the ACKs for this request are received,
                    // then set the response, and erase this outstanding request
                    if (++it->num_acks > failures_) {
                        if (it->response != nullptr) it->response->store(it->gsn, std::memory_order_relaxed);
                        latest_gsn = it->gsn;
                        outstanding.erase(it);
                    }
                }
            },
            [&] (zip::api::client_new_epoch& epoch) {
                // update the epoch state
                epochs.emplace_back(detail::epoch {epoch.begin, assigned, assigned += epoch.num_slots});
            },
            [&] (zip::api::storage_finished& bye) {
                // verify that the storage server exists
                if (bye.shard_id != shard_id_ || !queues.contains(bye.replica_id)) {
                    logger.warn("Failed to disconnect with storage server (", bye.shard_id, ":", bye.replica_id, ") as it does not exist");
                    return;
                }

                // remove the storage server from the state
                logger.info("Ended connection with storage server (", bye.shard_id, ":", bye.replica_id, ")");
                queues.erase(bye.replica_id);

                // check if all storage servers have been finalized,
                // and send the final message to the ordering server
                if (queues.empty()) order_.send(&finished, finished.length());
            },
            [&] ([[maybe_unused]] zip::api::order_finished& bye) {
                // mark the order connection as finalized
                logger.info("Ended connection with ordering service");
                end = true;
            },
            [&] (zip::api::client_storage_info& info) {
                // connect to all the servers in the list
                for (unsigned long i = 0; i < info.num_servers; i++) {
                    // try to connect to the server
                    auto connection = manager_.connect(info.addresses[i], &intro_, intro_.length());
                    if (!connection) {
                        logger.warn("Failed to connect to storage server");
                        continue;
                    }

                    // verify the server's intro message
                    auto& [send_queue, buffer, length] = *connection;
                    auto& intro = *static_cast<zip::api::storage_intro*>(buffer);
                    if (   intro.length() != length
                        || intro.message_type != zip::api::STORAGE_INTRO
                        || intro.shard_id != shard_id_
                        || queues.contains(intro.replica_id)
                        || !(queues.empty() || client_queues == intro.client_queues)) {
                        logger.warn("Failed to validate storage server information");
                        continue;
                    }

                    // get the number of destination queues
                    if (queues.empty()) {
                        client_queues = intro.client_queues;
                        destination = 1 + (client_id_ / zip::consts::NUM_SHARDS) % client_queues;
                    }

                    // add the new storage server to the state
                    logger.info("Established connection with storage server (", shard_id_, ":", intro.replica_id, ")");
                    queues[intro.replica_id] = std::move(send_queue);
                }
            },
            [&] ([[maybe_unused]] zip::api::client_heartbeat &heartbeat) {
            }
        );
    }
}

} // namespace zip::client
