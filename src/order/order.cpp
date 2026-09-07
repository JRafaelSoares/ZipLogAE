#include "order/order.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "api/api.h"
#include "network/manager.h"
#include "util/log.h"
#include "util/switch.h"
#include "util/util.h"

namespace zip {
namespace order {

/** create a logger for this file */
static zip::util::logger logger("order");

order::order(zip::network::manager& manager, unsigned int cpu_id): manager_(manager) {
    // create the network receive queue
    recv_queue_ = std::move(manager_.create_recv_queues(1).front());

    // create the storage info buffer
    info_buffer_ = manager_.get_buffers(zip::consts::BUFFER_SIZES.back(), 1);

    // start the thread for processing ordering requests
    thread_ = std::thread(&order::loop, this);
    zip::util::pin_thread(thread_, cpu_id);
}

order::~order() {
    // set the stop signal and wait for the thread
    stop_.store(true, std::memory_order_relaxed);
    thread_.join();
}

void order::add_storage(
    uint64_t shard_id,
    uint64_t replica_id,
    unsigned int client_queues,
    unsigned int subscriber_queues,
    sockaddr_in address,
    zip::network::send_queue send_queue
) {
    // make sure the storage server being added is unique
    auto lock = std::unique_lock(lock_);
    ZIP_ASSERT(storage_replicas_[shard_id].count(replica_id) == 0, "storage server replica ID already exists");
    logger.info("Established connection with storage server (", shard_id, ":", replica_id, ")");

    // get the number of client and subscriber queues
    auto& subscriber_queues_ = storage_subscriber_queues_[shard_id];
    auto& client_queues_ = storage_client_queues_[shard_id];
    if (storage_replicas_[shard_id].empty()) {
        subscriber_queues_ = subscriber_queues;
        client_queues_ = client_queues;
    } else {
        ZIP_ASSERT(subscriber_queues_ == subscriber_queues, "invalid number of subscriber queues");
        ZIP_ASSERT(client_queues_ == client_queues, "invalid number of client queues");
    }

    // add the storage server replica to the state
    ZIP_ASSERT(client_queue_.empty() && subscriber_queue_.empty(), "cannot add storage servers");
    storage_replicas_[shard_id][replica_id] = address;
    storage_queues_[shard_id][replica_id] = std::move(send_queue);
}

void order::add_client(uint64_t client_id, uint64_t shard_id, uint64_t num_slots, zip::network::send_queue send_queue) {
    // make sure the client being added is unique
    auto lock = std::unique_lock(lock_);
    ZIP_ASSERT_ZERO(client_request_.count(client_id), "client ID already exists");
    ZIP_ASSERT(client_request_.size() < zip::consts::MAX_CLIENTS, "too many clients");
    logger.info("Established connection with client (", client_id, ")");

    // prepare a message to tell the client about all it's shard's replicas
    auto& info = info_buffer_.front().as<zip::api::client_storage_info>();

    // fill in the details in the message
    info.message_type = zip::api::CLIENT_STORAGE_INFO;
    info.num_servers = 0;
    for (auto& [rid, address]: storage_replicas_[shard_id]) {
        info.addresses[info.num_servers++] = address;
    }

    // send the message to the client
    send_queue.send(info_buffer_.front(), info.length(), true);

    // prepare a message to send to all shard replicas
    // to add the given client to their state
    zip::api::storage_client_initialize init;
    init.message_type = zip::api::STORAGE_CLIENT_INITIALISE;
    init.client_id = client_id;

    // send the message to all replicas
    for (auto& [_, queue]: storage_queues_[shard_id]) {
        queue.send(&init, init.length());
    }

    // add the client to the state
#if ZIP_CLIENT_SEQ
    client_seq_[client_id] = 0;
#endif
    client_request_[client_id] = num_slots;
    client_shard_[client_id] = shard_id;
    storage_clients_[shard_id].insert(client_id);
    client_queue_[client_id] = std::move(send_queue);
}

void order::add_subscriber(uint64_t subscriber_id, zip::network::send_queue send_queue) {
    // make sure the subscriber being added is unique
    auto lock = std::unique_lock(lock_);
    ZIP_ASSERT(subscriber_queue_.count(subscriber_id) == 0, "subscriber ID already exists");
    logger.info("Established connection with subscriber (", subscriber_id, ")");

    // prepare a message for sending this info to the subscriber
    auto& info = info_buffer_.front().as<zip::api::subscriber_storage_info>();

    // fill in the details in the message
    info.message_type = zip::api::SUBSCRIBER_STORAGE_INFO;
    info.num_servers = 0;
    for (uint64_t sid = 0; sid < zip::consts::NUM_SHARDS; sid++) {
        for (auto& [rid, address]: storage_replicas_[sid]) {
            info.addresses[info.num_servers++] = address;
        }
    }

    // send the message to the subscriber and add it to the state
    send_queue.send(info_buffer_.front(), info.length(), true);
    subscriber_queue_[subscriber_id] = std::move(send_queue);
}

void order::loop() {
    // timestamp for tracking epoch timer
    auto epoch = std::chrono::high_resolution_clock::time_point::min();

    // get a buffer for control messages for each shard
    auto controls = manager_.get_buffers(zip::consts::BUFFER_SIZES.back(), zip::consts::NUM_SHARDS);

    // create a message to update clients about the next epoch
    zip::api::client_new_epoch req;
    req.message_type = zip::api::CLIENT_NEW_EPOCH;

    // create a message to send order finished
    zip::api::order_finished finished;
    finished.message_type = zip::api::ORDER_FINISHED;

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // poll receive queue for a packet
        zip::util::recv_apply(logger, recv_queue_,
            [&] (zip::api::client_finished& bye) {
                // verify the client exists
                auto lock = std::unique_lock(lock_);
                ZIP_ASSERT(client_queue_.count(bye.client_id) > 0, "unknown client ID");
                logger.info("Ended connection with client (", bye.client_id, ")");

                // reply to the client and erase it from the state
                client_queue_[bye.client_id].send(&finished, finished.length(), true);
                auto sid = client_shard_[bye.client_id];
#if ZIP_CLIENT_SEQ
                client_seq_.erase(bye.client_id);
#endif
                client_shard_.erase(bye.client_id);
                client_queue_.erase(bye.client_id);
                client_request_.erase(bye.client_id);
                storage_clients_[sid].erase(bye.client_id);

                // prepare a message to send to all shard replicas
                // to remove the given client from their state
                zip::api::storage_client_finalize req;
                req.message_type = zip::api::STORAGE_CLIENT_FINALISE;
                req.client_id = bye.client_id;

                // send a message to all replicas
                for (auto& [_, queue]: storage_queues_[sid]) {
                    queue.send(&req, req.length());
                }
            },
            [&] (zip::api::subscriber_finished& bye) {
                // handle a subscriber finished req
                logger.info("Ended connection with subscriber (", bye.subscriber_id, ")");

                // check whether this is an existing client
                // and erase this client's state
                auto lock = std::unique_lock(lock_);
                subscriber_queue_.erase(bye.subscriber_id);
            }
        );

        // execute the next epoch when timer expires
        auto now = std::chrono::high_resolution_clock::now();
        if (now > epoch) {
            // only execute the next epoch if we
            // have some clients
            auto lock = std::unique_lock(lock_);
            if (!client_request_.empty()) {
                // find the number of slots that need to go to clients
                unsigned long total_slots = 0;
                std::vector<std::pair<uint64_t, unsigned long>> slots;

                // go over each client state and calculate that client's slots
                for (auto& [cid, request]: client_request_) {
                    // add the slots to the global assignment
                    slots.emplace_back(cid, request);
                    total_slots += request;
                }

                // sort the vector according to the number of slots assigned to each client
                auto cmp = [] (auto& left, auto& right) { return left.second < right.second; };
                std::sort(slots.begin(), slots.end(), cmp);

                // for each shard, send it the details
                // of the clients that are connected to it
                auto buffer = controls.begin();
                for (unsigned long i = 0; i < zip::consts::NUM_SHARDS; i++, buffer++) {
                    // create a message for this shard's replicas
                    auto& req = buffer->as<zip::api::storage_order_slots>();
                    req.message_type = zip::api::STORAGE_ORDER_SLOTS;
                    req.start_gsn = gsn_base_;

                    // fill in the details for this shard
#if ZIP_SHARD_SEQ
                    req.shard_seq = shard_seq_[i];
#endif
                    req.num_clients = 0;

                    // fill in the details for all clients, setting
                    // the client ID to -1 if that client is not
                    // in the particular shard
                    for (auto& [cid, num]: slots) {
                        auto& assignment = req.assignments[req.num_clients++];
                        assignment.num_slots = num;
                        if (client_shard_[cid] != i) {
                            assignment.client_id = -1;
                        } else {
#if ZIP_CLIENT_SEQ
                            assignment.client_seq = client_seq_[cid];
#endif
                            assignment.client_id = cid;
#if ZIP_CLIENT_SEQ
                            client_seq_[cid] += num;
#endif
#if ZIP_SHARD_SEQ
                            shard_seq_[i] += num;
#endif
                        }
                    }

                    // send the message to all replicas at the right destination queue
                    for (auto& [_, queue]: storage_queues_[i]) {
                        queue.send(*buffer, req.length());
                    }
                }

                // update the message to be sent to the clients
                req.begin = now + zip::consts::EPOCH_DURATION;

                // send the notification to clients
                for (auto& [cid, queue]: client_queue_) {
                    req.num_slots = client_request_[cid];
                    queue.send(&req, req.length());
                }

                // update the state for the next epoch
                epoch = now + zip::consts::EPOCH_DURATION;
                gsn_base_ += total_slots;
            }
        }
    }
}

} // namespace order
} // namespace zip
