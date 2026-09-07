#include <zip/order/order.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <limits>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include <zip/api/api.h>
#include <zip/network/buffer.h>
#include <zip/network/manager.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/switch.h>
#include <zip/util/util.h>

#include "zip/network/erpc_constants.h"

namespace zip::order {

/** create a logger for this file */
static zip::util::logger logger("order");

order::order(zip::network::manager& manager, network::erpc_transport_factory& transport, uint16_t cpu_id): manager_(manager) {
    // start the thread for processing ordering requests
    transport_ = &transport;

    auto& thread = threads_.emplace_back(&order::loop, this);
    zip::util::pin_thread(thread, cpu_id);

    // setup the intro message for the ordering server
    intro_.message_type = intro_.tag;

    // start the thread for accepting connections
    threads_.emplace_back([this, &thread] () {
        network::erpc_transport_factory::set_local_rpc_id_(0);
        auto recv_endpoint = transport_->create_recv_endpoint();

        while (!stop_.load(std::memory_order_relaxed)) {

            // add the client or the storage server
            zip::util::recv_apply(logger, *recv_endpoint,
                [&] (zip::api::storage_intro& intro) {
                    // make sure the storage server being added is unique
                    auto lock = std::unique_lock(lock_);
                    if (!client_state_.empty() || server_queues_[intro.shard_id].contains(intro.replica_id)) {
                       logger.warn("Could not add storage server (", intro.shard_id, ":", intro.replica_id, ")");
                       return;
                    }

                    // add the storage server to the state
                    auto send_queue = transport_->create_send_endpoint(intro.address, network::STORAGE_SERVER_OFFSET);
                    send_queue->send(&intro_, intro_.length());
                    auto epoch_send_queue = transport_->create_send_endpoint(intro.address, network::STORAGE_SERVER_OFFSET+2);

                    server_queues_[intro.shard_id][intro.replica_id] = std::move(epoch_send_queue);
                    logger.info("Established connection with storage server (", intro.shard_id, ":", intro.replica_id, ")");

                },
                [&] (zip::api::client_intro& intro) {
                    // make sure the client being added is unique
                    auto lock = std::unique_lock(lock_);
                    if (   intro.client_id >= zip::consts::MAX_CLIENTS
                        || client_state_.contains(intro.client_id)) {
                        logger.warn("Could not add client (", intro.client_id, ")");
                        return;
                    }

                    // add the client to the state
                    logger.info("Established connection with client (", intro.client_id, ")");
                    auto send_queue = transport_->create_send_endpoint(intro.address, network::CLIENT_SERVER_OFFSET);
                    send_queue->send(&intro_, intro_.length());
                    client_state_.try_emplace(intro.client_id, intro.shard_id, std::move(send_queue));
                }
            );
        }
    });

    // start the execution of processing threads
    start_.store(true, std::memory_order_release);
    start_.notify_all();
}

order::~order() {
    // set the stop signal and wait for the thread
    stop_.store(true, std::memory_order_relaxed);
    for (auto& thread: threads_) thread.join();
}

void order::loop() {
    // wait until we're allowed to start executing
    start_.wait(false, std::memory_order_acquire);

    network::erpc_transport_factory::set_local_rpc_id_(1);
    auto recv_endpoint = transport_->create_recv_endpoint();
    // timestamp for tracking epoch timer
    auto epoch = std::chrono::high_resolution_clock::now();

    // create buffers for control messages for the storage servers
    std::unordered_map<shard_t, std::vector<std::unique_ptr<zip::network::buffer>>> buffers;
    std::unordered_map<shard_t, zip::util::wraparound_iterator<decltype(buffers)::mapped_type>> iterators;

    // create a message to update clients about the next epoch
    zip::api::client_new_epoch next;
    next.message_type = next.tag;

    // create a message to send order finished
    zip::api::order_finished finished;
    finished.message_type = finished.tag;

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // spin loop optimization
        zip::util::relax();

        // poll receive queue for a packet
        zip::util::recv_apply(logger, *recv_endpoint,
            [&] (zip::api::order_client_rate& rate) {
                // verify the client exists
                auto lock = std::unique_lock(lock_);
                auto it = client_state_.find(rate.client_id);
                if (it == client_state_.end()) {
                    logger.warn("Received rate changing request from unknown client (", rate.client_id, ")");
                    return;
                }

                // change the client's requested rate
                auto& state = it->second;
                state.num_slots = zip::util::align<zip::consts::MIN_SLOTS>(std::max(1U, rate.num_slots));
            },
            [&] (zip::api::storage_finished& bye) {
                // verify the storage server exists
                auto lock = std::unique_lock(lock_);
                if (!server_queues_.contains(bye.shard_id) || !server_queues_[bye.shard_id].contains(bye.replica_id)) {
                    logger.warn("Failed to disconnect with storage server (", bye.shard_id, ":", bye.replica_id, ") as it does not exist");
                    return;
                }

                // reply to the storage server
                auto it = server_queues_[bye.shard_id].find(bye.replica_id);
                it->second->send(&finished, finished.length());

                // erase the storage server from the state
                logger.info("Ended connection with storage server (", bye.shard_id, ":", bye.replica_id, ")");
                server_queues_[bye.shard_id].erase(it);
            },
            [&] (zip::api::client_finished& bye) {
                // verify the client exists
                auto lock = std::unique_lock(lock_);
                auto it = client_state_.find(bye.client_id);
                if (it == client_state_.end()) {
                    logger.warn("Failed to disconnect with client (", bye.client_id, ") as it does not exist");
                    return;
                }

                // reply to the client
                auto& state = it->second;
                state.endpoint->send(&finished, finished.length());

                // prepare a message to send to all storage servers
                // to finalize the client
                zip::api::storage_finalize_client req;
                req.message_type = req.tag;
                req.client_id = bye.client_id;

                // send a message to all storage servers for this client's shard
                for (auto& [_, queue]: server_queues_[state.shard_id]) {
                    queue->send(&req, req.length());
                }

                // erase the client from the state
                logger.info("Ended connection with client (", bye.client_id, ")");
                client_state_.erase(it);
            }
        );

        // execute the next epoch when timer expires
        auto now = std::chrono::high_resolution_clock::now();
        if (now > epoch && !client_state_.empty()) {
            // find the number of slots that need to go to clients
            auto lock = std::unique_lock(lock_);
            std::vector<std::tuple<uint32_t, shard_t, client_t>> slots;
            uint32_t total_slots = 0;

            // update the message to be sent to the clients
            next.begin = zip::util::time_in_ns(now + zip::consts::EPOCH_DURATION);

            // go over each client state and calculate that client's slots
            for (auto& [client_id, state]: client_state_) {
                // add the assignment to the vector
                total_slots += state.num_slots;
                slots.emplace_back(state.num_slots, state.shard_id, client_id);

                // send the epoch detail to the client
                next.num_slots = state.num_slots;
                state.endpoint->send(&next, next.length());
            }

            // sort the vector according to the number of slots assigned
            // to each client in decreasing order
            std::ranges::sort(slots, std::greater<decltype(slots)::value_type>());

            // create a message for the shards' storage servers
            for (auto& [shard_id, queues]: server_queues_) {
                // check if we need to allocate buffers for this shard
                if (!buffers.contains(shard_id)) {
                    buffers[shard_id] = manager_.get_buffers(zip::consts::MAX_OUTSTANDING);
                    iterators[shard_id] = zip::util::wraparound_iterator(buffers[shard_id]);
                }

                auto& buffer = iterators[shard_id].get_and_increment();
                auto& req = buffer->as<zip::api::storage_new_epoch>();
                req.message_type = req.tag;
                req.begin = zip::util::time_in_ns(now + zip::consts::EPOCH_DURATION);
                req.start_gsn = gsn_base_;
                req.num_clients = 0;

                // fill in the details for all clients
                for (auto& [num, sid, client_id]: slots) {
                    auto& assignment = req.assignments[req.num_clients++];
                    assignment.num_slots = num;
                    assignment.client_id = sid == shard_id ? client_id : std::numeric_limits<client_t>::max();
                }

                // send the message to all storage servers
                for (auto& [_, queue]: queues) {
                    queue->send(*buffer, req.length());
                }
            }

            // update the state for the next epoch
            epoch = now + zip::consts::EPOCH_DURATION;
            gsn_base_ += total_slots;
        }
    }
}

} // namespace zip::order
