#include <algorithm>
#include <zip/subscriber/subscriber.h>

#include <cstring>
#include <tuple>
#include <utility>
#include <limits>
#include <unordered_set>

#include <zip/api/api.h>
#include <zip/network/buffer.h>
#include <zip/network/manager.h>
#include <zip/subscriber/log.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/switch.h>
#include <zip/util/util.h>
#include "zip/network/erpc_constants.h"
#include "../../apps/pc/include/zip/pc/api/api.h"

namespace zip::subscriber {

/** create a logger for this file */
static zip::util::logger logger("subscriber");

subscriber::subscriber(
        zip::network::manager& manager,
        zip::network::erpc_transport_factory& transport,

        subscriber_t subscriber_id,
        uint32_t failures,
        std::set<uint16_t> cpus,
        uint32_t num_iterators,
        std::set<std::string> servers,
        std::string addr,
        uint16_t port
):
manager_(manager), subscriber_id_(subscriber_id), failures_(failures),
log_(num_iterators), iterators_(num_iterators, log_), num_threads_(cpus.size()) {
    // verify arguments for correctness
    ZIP_ASSERT(num_threads_ > 0, "no subscriber threads created");

    transport_ = &transport;

    // setup the intro message for the subscriber
    intro_.message_type = intro_.tag;
    intro_.subscriber_id = subscriber_id_;
    intro_.address = api::net_info_to_msg({
        .host = addr,
        .port = port
    });
    // setup the finished message
    finished_.message_type = finished_.tag;
    finished_.subscriber_id = subscriber_id_;

    // start threads for processing the subscriber loop
    uint32_t index = 0;
    for (auto cpu: cpus) {
        // create and this thread and it's corresponding receive queue
        auto& thread = threads_.emplace_back(&subscriber::loop, this, index, servers);
        zip::util::pin_thread(thread, cpu);
        index++;
    }

    // connect to all the storage servers
    /*
    for (auto& address: servers) add_server(address);
    for (auto& [_, queues]: server_queues_) {
        ZIP_ASSERT(queues.size() > failures_, "not enough storage servers for failure resilience");
    }
    */

    // start the execution of processing threads
    start_.store(true, std::memory_order_release);
    start_.notify_all();
}



subscriber::~subscriber() {
    // send a finished message to the storage servers
    for (auto& [_, replicas]: server_queues_) {
        for (auto& [_, queues]: replicas) {
            queues.front()->send(&finished_, finished_.length());
            queues.clear();
        }
    }

    // set the stop signal and wait for threads
    for (auto& thread: threads_) thread.join();
}

void copy_intro(zip::api::subscriber_intro& dest, const zip::api::subscriber_intro& src) {
    dest.message_type = src.message_type;
    dest.subscriber_id = src.subscriber_id;
    dest.address = src.address;
}

void subscriber::loop(uint32_t index, std::set<std::string> servers) {
    // wait until we're allowed to start executing
    start_.wait(false, std::memory_order_acquire);
    network::erpc_transport_factory::set_local_rpc_id_(index+1);
    auto recv_endpoint = transport_->create_recv_endpoint();
    zip::api::subscriber_intro local_intro;
    copy_intro(local_intro, intro_);
    local_intro.erpc_index = index+1;
    for (auto& address: servers)
    {
        // try to connect to the storage server

        auto send_endpoint = transport_->create_send_endpoint(address, network::STORAGE_SERVER_OFFSET +1);
        auto buf = transport_->get_buffer();

        auto len = send_endpoint->request_reply(&local_intro, local_intro.length(), *buf.get(), *recv_endpoint);
        auto& intro = *static_cast<zip::api::storage_intro*>(buf.get()->buffer_);
        ZIP_ASSERT_EQ(intro.length(), len, "length of the received packet is invalid");
        ZIP_ASSERT_EQ(intro.message_type, zip::api::STORAGE_INTRO, "received an unknown type of message");
        logger.info("Established connection with storage server (", intro.shard_id, ":", intro.replica_id, ")");

    }

    // state for log entries that are being received
    struct entry_t { client_t client_id; uint32_t data_length; uintptr_t data; uint32_t num_acks; };
    std::unordered_map<gsn_t, entry_t> ongoing;
    std::unordered_map<shard_t, gsn_t> shard_gsn;

    // position to insert log entries in each shard
    std::unordered_map<shard_t, position> positions;

    // number of finished messages received
    std::unordered_map<shard_t, std::unordered_set<replica_t>> finished;

    // keep processing in a loop until shutdown
    bool end = false;
    while (!end) {
        // spin loop optimization
        zip::util::relax();

        // poll the receive queue for a message
        zip::util::recv_apply(logger, *recv_endpoint,
            [&] (zip::api::subscriber_log_entries& entries) {
                // obtain the metadata for this shard's position
                auto& latest = shard_gsn.try_emplace(entries.shard_id, std::numeric_limits<gsn_t>::max()).first->second;
                auto& position = positions.try_emplace(entries.shard_id, log_).first->second;

                // process the entries in a loop
                uint32_t index = 0, data_length = 0;
                while (index < entries.num_entries) {
                    // obtain the current entry and update the state
                    auto& entry = *reinterpret_cast<zip::api::subscriber_log_entries::entry_t*>(entries.entries + data_length);
                    data_length += entry.length();
                    index++;

                    // if this entry has already been delivered, then continue
                    if (latest != std::numeric_limits<gsn_t>::max() && entry.gsn <= latest) continue;

                    // get the entry from the ongoing entries and check if this is
                    // the first copy of the entry received
                    auto [it, first] = ongoing.try_emplace(entry.gsn);
                    auto& current = it->second;
                    if (first) {
                        // add the entry to the state
                        if (entry.data_length > sizeof(uintptr_t)) {
                            auto temp = std::malloc(entry.data_length);
                            std::memcpy(temp, entry.data, entry.data_length);
                            current.data = reinterpret_cast<uintptr_t>(temp);
                        } else if (entry.data_length > 0) {
                            current.data = *reinterpret_cast<uintptr_t*>(entry.data);                       
                        }
                        current.client_id = entry.client_id;
                        current.data_length = entry.data_length;
                    } else {
                        ZIP_ASSERT_EQ(current.client_id, entry.client_id, "failed to validate entry with GSN ", entry.gsn);
                        ZIP_ASSERT_EQ(current.data_length, entry.data_length, "failed to validate entry with GSN ", entry.gsn);
                    }

                    // if the entry can be delivered, then add it to the log
                    if (++current.num_acks > failures_) {
                        position.insert(entry.gsn, current.client_id, current.data, current.data_length);
                        ongoing.erase(it);
                        latest = entry.gsn;
                    }
                }
            },
            [&] (zip::api::storage_finished& bye) {
                    logger.info("Ended connection with storage server (", bye.shard_id, ":", bye.replica_id, ")");
                /*
                // verify that the storage server exists
                if (   !server_queues_.contains(bye.shard_id)
                    || !server_queues_[bye.shard_id].contains(bye.replica_id)
                    || finished[bye.shard_id].contains(bye.replica_id)) {
                    logger.warn("Failed to disconnect with storage server (", bye.shard_id, ":", bye.replica_id, ") as it does not exist");
                    return;
                }

                // remove the storage server from the state
                finished[bye.shard_id].emplace(bye.replica_id);

                // if all the storage servers have been finalized then we can exit the loop
                if (std::ranges::all_of(finished, [&] (auto& elem) { return elem.second.size() == server_queues_[elem.first].size(); })) {
                    logger.info("Ended connection with storage server (", bye.shard_id, ":", bye.replica_id, ")");
                    end = true;
                }
                */
            }
        );
    }
}

} // namespace zip::subscriber
