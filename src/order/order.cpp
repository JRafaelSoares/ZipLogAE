#include "order/order.h"

#include <algorithm>
#include <chrono>
#include <compare>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <tuple>
#include <utility>
#include <vector>

#include <netinet/in.h>

#include "api/api.h"
#include "network/manager.h"
#include "util/log.h"
#include "util/switch.h"
#include "util/util.h"

namespace zip::order {

/** create a logger for this file */
static zip::util::logger logger("order");

order::order(zip::network::manager& manager, uint16_t cpu_id): manager_(manager) {
    // create the network receive queue
    recv_queue_ = std::move(manager_.create_recv_queues(1).front());

    // create the storage info buffer
    buffers_ = manager_.get_buffers(zip::consts::rdma::MAX_OUTSTANDING_REQUESTS + 1);
    iterator_ = zip::util::wraparound_iterator(buffers_);

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
    sockaddr_in address,
    zip::network::send_queue send_queue
) {
    // make sure the storage server being added is unique
    auto lock = std::unique_lock(lock_);
    if (   shard_id >= zip::consts::NUM_SHARDS
        || replica_id >= zip::consts::MAX_REPLICAS
        || storage_addresses_[shard_id].contains(replica_id)) {
       logger.warn("Could not add storage server (", shard_id, ":", replica_id, ")");
       return;
    }

    // add the storage server replica to the state
    ZIP_ASSERT(client_state_.empty() && subscriber_queue_.empty(), "cannot add storage servers after client and subscribers");
    logger.info("Established connection with storage server (", shard_id, ":", replica_id, ")");
    storage_addresses_[shard_id][replica_id] = address;
    storage_queues_[shard_id][replica_id] = std::move(send_queue);
}

void order::add_client(uint64_t client_id, uint64_t shard_id, unsigned long num_slots, zip::network::send_queue send_queue) {
    // make sure the client being added is unique
    auto lock = std::unique_lock(lock_);
    if (   shard_id >= zip::consts::NUM_SHARDS
        || client_id >= zip::consts::MAX_CLIENTS
        || client_state_.contains(client_id)
        || client_state_.size() >= zip::consts::MAX_CLIENTS) {
        logger.warn("Could not add client (", client_id, ")");
        return;
    }

    // prepare a message to tell the client about all it's shard's replicas
    auto buffer = iterator_.get_and_increment();
    auto& info = buffer->as<zip::api::client_storage_info>();

    // fill in the details in the message
    info.message_type = zip::api::CLIENT_STORAGE_INFO;
    info.num_servers = 0;
    for (auto& [_, address]: storage_addresses_[shard_id]) {
        info.addresses[info.num_servers++] = address;
    }

    // send the message to the client
    send_queue.set_assert_on_failure(false);
    send_queue.send(*buffer, info.length());

    // add the client to the state
    logger.info("Established connection with client (", client_id, ")");
    client_state_.try_emplace(client_id, shard_id, num_slots, std::move(send_queue));
}

void order::add_subscriber(uint64_t subscriber_id, zip::network::send_queue send_queue) {
    // make sure the subscriber being added is unique
    auto lock = std::unique_lock(lock_);
    if (subscriber_queue_.contains(subscriber_id)) {
        logger.warn("Could not add subscriber (", subscriber_id, ") ");
        return;
    }

    // prepare a message for sending this info to the subscriber
    auto buffer = iterator_.get_and_increment();
    auto& info = buffer->as<zip::api::subscriber_storage_info>();

    // fill in the details in the message
    info.message_type = zip::api::SUBSCRIBER_STORAGE_INFO;
    info.num_servers = 0;
    for (uint64_t sid = 0; sid < zip::consts::NUM_SHARDS; sid++) {
        for (auto& [_, address]: storage_addresses_[sid]) {
            info.addresses[info.num_servers++] = address;
        }
    }

    // send the message to the subscriber
    send_queue.send(*buffer, info.length());

    // send the message to the subscriber and add it to the state
    logger.info("Established connection with subscriber (", subscriber_id, ")");
    subscriber_queue_[subscriber_id] = std::move(send_queue);
}

void order::loop() {
    // timestamp for tracking epoch timer
    auto epoch = std::chrono::high_resolution_clock::time_point::min(), heartbeat = epoch;

    // create buffers for control messages for each shard
    std::vector<std::vector<zip::network::buffer>> controls;
    std::vector<zip::util::wraparound_iterator<decltype(controls)::value_type>> iterators;
    for (uint64_t sid = 0; sid < zip::consts::NUM_SHARDS; sid++) {
        auto& buffers = controls.emplace_back(manager_.get_buffers(zip::consts::rdma::MAX_OUTSTANDING_REQUESTS + 1));
        iterators.emplace_back(buffers);
    }

    // create a message to update clients about the next epoch
    zip::api::client_new_epoch next;
    next.message_type = zip::api::CLIENT_NEW_EPOCH;

    // create a message to send order finished
    zip::api::order_finished finished;
    finished.message_type = zip::api::ORDER_FINISHED;

    // heartbeat to clients for crash handling
    zip::api::client_heartbeat heartbeat_msg;
    heartbeat_msg.message_type = zip::api::CLIENT_HEARTBEAT;
    uint64_t heartbeat_id = 0;

    std::unordered_set<uint64_t> suspicious_clients(10);

    // change the client state to failed
    auto failed = [&] (uint64_t client_id, client_state& state) {
        // inform the storage servers that the client has failed
        zip::api::storage_client_freeze req;
        req.message_type = zip::api::STORAGE_CLIENT_FREEZE;
        req.client_id = client_id;

        // mark the client state as failed
        state.failed = true;
        logger.warn("Marking client (", client_id, ") as suspected to have failed");
        for (auto& [rid, queue]: storage_queues_[state.shard_id]) {
            queue.send(&req, req.length());
        }

        suspicious_clients.insert(client_id);
    };

    // change the client state to requested
    auto requested = [&] (uint64_t client_id, client_state& state, uint64_t min_cut) {
        // create the request for the client data
        zip::api::storage_client_cut req;
        req.message_type = zip::api::STORAGE_CLIENT_CUT;
        req.client_id = client_id;
        req.begin_gsn = min_cut;

        // request the data from all storage servers
        state.requested = true;
        logger.info("Requesting data for client (", client_id, ") from storage servers starting from GSN ", min_cut);
        for (auto& [_, queue]: storage_queues_[state.shard_id]) {
            queue.send(&req, req.length());
        }
    };

    // change the client state to finalized
    auto finalized = [&] (uint64_t client_id, client_state& state) {
        // send the roll-forward data to the storage servers
        if (state.requested) {
            for (auto& [_, buffer]: state.data) {
                auto& data = buffer.as<zip::api::storage_client_patch>();
                for (auto& [rid, queue]: storage_queues_[state.shard_id]) {
                    queue.send(buffer, data.length());
                }
            }
        }

        // prepare a finalize message
        zip::api::storage_client_finalize req;
        req.message_type = zip::api::STORAGE_CLIENT_FINALIZE;
        req.client_id = client_id;

        // send the message to all storage servers
        logger.info("Ended connection with client (", client_id, ")");
        for (auto& [_, queue]: storage_queues_[state.shard_id]) {
            queue.send(&req, req.length(), state.requested);
        }

        // erase the client from the state
        client_state_.erase(client_id);
        suspicious_clients.erase(client_id);
    };

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // process any failures that might occur
        zip::util::failure_apply(logger, manager_,
            [&] (zip::api::client_intro& intro) {
                auto lock = std::unique_lock(lock_);
                auto it = client_state_.find(intro.client_id);
                if (it != client_state_.end() && !it->second.failed) {
                    // mark the client as failed
                    failed(intro.client_id, it->second);
                }
            }
        );

        // poll receive queue for a packet
        zip::util::recv_apply(logger, recv_queue_,
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
                state.num_slots = zip::util::align<zip::consts::MIN_SLOTS>(std::max(1UL, rate.num_slots));
            },
            [&] (zip::api::client_finished& bye) {
                // verify the client exists
                auto lock = std::unique_lock(lock_);
                auto it = client_state_.find(bye.client_id);
                if (it == client_state_.end()) {
                    logger.warn("Failed to disconnect with client (", bye.client_id, ") as it does not exist");
                    return;
                }

                // reply to the client and finalize it
                auto& state = it->second;
                state.send_queue.send(&finished, finished.length(), true);
                finalized(bye.client_id, state);
            },
            [&] (zip::api::subscriber_finished& bye) {
                // verify the subscriber exists
                auto lock = std::unique_lock(lock_);
                if (!subscriber_queue_.contains(bye.subscriber_id)) {
                    logger.warn("Failed to disconnect with subscriber (", bye.subscriber_id, ") as it does not exist");
                    return;
                }

                // reply to the subscriber and erase the subscriber state
                logger.info("Ended connection with subscriber (", bye.subscriber_id, ")");
                subscriber_queue_[bye.subscriber_id].send(&finished, finished.length(), true);
                subscriber_queue_.erase(bye.subscriber_id);
            },
            [&] (zip::api::order_client_freeze& freeze) {
                // verify the client exists
                auto lock = std::unique_lock(lock_);
                auto it = client_state_.find(freeze.client_id);
                if (it == client_state_.end()) {
                    logger.warn("Failed to mark client (", freeze.client_id, ") as failed as it does not exist");
                    return;
                }

                // if the client failure is already detected then continue
                auto& state = it->second;
                if (!state.failed) failed(freeze.client_id, state);
            },
            [&] (zip::api::order_client_cut& cut) {
                // verify the client exsists and is marked as failed
                auto lock = std::unique_lock(lock_);
                auto it = client_state_.find(cut.client_id);
                if (it == client_state_.end() || !it->second.failed) {
                    logger.warn("Received cut for unknown client (", cut.client_id, ")");
                    return;
                }

                // verify the message came from a correct storage server
                auto& state = it->second;
                if (state.shard_id != cut.shard_id || state.cuts.contains(cut.replica_id)) {
                    logger.warn("Received cut from invalid storage server (", cut.shard_id, ":", cut.replica_id, ") for client (", cut.client_id, ")");
                    return;
                }

                // save the client cut for this storage server
                state.cuts[cut.replica_id] = cut.latest_gsn;

                // if we have received cuts from enough storage servers
                // then we can request client data from the storage servers
                if (!state.requested && state.cuts.size() == storage_queues_[state.shard_id].size()) {
                    // find the minimum and maximum cuts
                    auto min = state.cuts.begin()->second, max = state.cuts.rbegin()->second;

                    // if one of the servers sent -1, disambiguate the responses
                    if (min != max && max == std::numeric_limits<uint64_t>::max()) {
                        for (auto it = ++state.cuts.rbegin(); it != state.cuts.rend(); ++it) {
                            if (it->second != std::numeric_limits<uint64_t>::max()) {
                                max = it->second;
                                break;
                            }
                        }
                    }

                    // if all servers are consistent then we can finalize
                    // otherwise, we need to request client data from storage servers
                    if (min == max) {
                        finalized(cut.client_id, state);
                    } else {
                        requested(cut.client_id, state, min);
                    }
                }
            },
            [&] (zip::api::order_client_data& data) {
                // verify the client exists, and data has been requested
                auto lock = std::unique_lock(lock_);
                auto it = client_state_.find(data.client_id);
                if (it == client_state_.end() || !it->second.requested) {
                    logger.warn("Received data for unknown client (", data.client_id, ")");
                    return;
                }

                // verify the message came from a correct storage server
                auto& state = it->second;
                if (state.shard_id != data.shard_id || state.flushed.contains(data.replica_id)) {
                    logger.warn("Received data from invalid storage server (", data.shard_id, ":", data.replica_id, ") for client (", data.client_id, ")");
                    return;
                }

                // check if this is a flush message
                if (data.gsn == std::numeric_limits<uint64_t>::max()) {
                    // mark the storage server as flushed
                    state.flushed.insert(data.replica_id);

                    // if enough storage servers have flushed,
                    // then we can finalize
                    if (state.flushed.size() == storage_queues_[state.shard_id].size()) {
                        finalized(data.client_id, state);
                    }
                } else if (!state.data.contains(data.gsn)) {
                    // we don't have data for this GSN so
                    // allocate a buffer and initialise the data
                    auto buffer = std::move(manager_.get_buffers(1).front());
                    auto& req = buffer.as<zip::api::storage_client_patch>();
                    req.message_type = zip::api::STORAGE_CLIENT_PATCH;
                    req.client_id = data.client_id;
                    req.data_length = data.data_length;
                    req.gsn = data.gsn;
                    std::memcpy(req.data, data.data, data.data_length);

                    // add the data to the map
                    state.data.try_emplace(data.gsn, std::move(buffer));
                }
            }
        );

        // execute the next epoch when timer expires
        auto now = std::chrono::high_resolution_clock::now();
        if (now > heartbeat) {
            auto lock = std::unique_lock(lock_);
            heartbeat_msg.id = heartbeat_id++;
            for (auto& [cid, state]: client_state_){
                // if a client is suspicious, don't send heartbeat as its recv_queue may already be cleaned
                if (suspicious_clients.find(cid) != suspicious_clients.end()) continue;
                state.send_queue.send(&heartbeat_msg, heartbeat_msg.length());
            }
            heartbeat = now + zip::consts::HEARTBEAT_INTERVAL;
        } else if (now > epoch) {
            // find the number of slots that need to go to clients
            auto lock = std::unique_lock(lock_);
            std::vector<std::tuple<unsigned long, uint64_t, uint64_t>> slots;
            unsigned long total_slots = 0;

            // update the message to be sent to the clients
            next.begin = now + zip::consts::EPOCH_DURATION;

            // go over each client state and calculate that client's slots
            for (auto& [cid, state]: client_state_) {
                // check if the client is currently active
                if (!state.failed) {
                    // add the assignment to the vector
                    total_slots += state.num_slots;
                    slots.emplace_back(state.num_slots, state.shard_id, cid);

                    // send the epoch detail to the client
                    next.num_slots = state.num_slots;
                    state.send_queue.send(&next, next.length());
                }
            }

            // sort the vector according to the number of slots assigned
            // to each client, the shard ID, and the client ID
            std::sort(slots.begin(), slots.end(), std::greater<decltype(slots)::value_type>());

            // for each shard, send it the details
            // of the clients that are connected to it
            for (uint64_t sid = 0; sid < zip::consts::NUM_SHARDS; sid++) {
                // create a message for this shard's replicas
                auto buffer = iterators[sid].get_and_increment();
                auto& req = buffer->as<zip::api::storage_slots>();
                req.message_type = zip::api::STORAGE_SLOTS;
                req.start_gsn = gsn_base_;
                req.num_clients = 0;

                // fill in the details for all clients, setting
                // the client ID to -1 if that client is not
                // in the particular shard
                for (auto& [num, csid, cid]: slots) {
                    auto& assignment = req.assignments[req.num_clients++];
                    assignment.num_slots = num;
                    if (csid != sid) {
                        assignment.client_id = std::numeric_limits<uint64_t>::max();
                    } else {
                        assignment.client_id = cid;
                    }
                }

                // send the message to all replicas
                for (auto& [_, queue]: storage_queues_[sid]) {
                    queue.send(*buffer, req.length());
                }
            }

            // update the state for the next epoch
            epoch = now + zip::consts::EPOCH_DURATION;
            gsn_base_ += total_slots;
        }
    }
}

} // namespace zip::order
