#include "subscriber/subscriber.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <set>
#include <tuple>
#include <unordered_map>
#include <utility>

#include <netinet/in.h>

#include "network/manager.h"
#include "network/recv_queue.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/switch.h"
#include "util/util.h"

namespace zip::subscriber {

/** create a logger for this file */
static zip::util::logger logger("subscriber");

subscriber::subscriber(
        zip::network::manager& manager,
        std::set<uint16_t> polling_cpus,
        std::set<uint16_t> application_cpus,
        std::string order,
        uint64_t subscriber_id,
        bool sequential,
        unsigned long failures,
        const std::function<void(unsigned long, zip::api::subscriber_log_entry&)>& callback
):
subscriber_id_(subscriber_id), log_(application_cpus.size()), manager_(manager), polling_threads_(polling_cpus.size()),
application_threads_(application_cpus.size()), failures_(failures), callback_(callback) {
    // verify arguments for correctness
    ZIP_ASSERT(polling_threads_ > 0, "invalid value of polling threads");
    ZIP_ASSERT(application_threads_ > 0, "invalid value of application threads");

    // create the network receive queues
    auto recv_queues = manager_.create_recv_queues(1 + polling_threads_);

    // initialize the struct which is subscriber's first message
    intro_.message_type = zip::api::SUBSCRIBER_INTRO;
    intro_.subscriber_id = subscriber_id;
    intro_.num_queues = polling_threads_;

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

    // start the thread for processing the control loop
    threads_.emplace_back(&subscriber::control, this, std::move(recv_queues[0]));

    // start the threads for polling the receive queues
    unsigned long index = 0;
    for (auto& cpu: polling_cpus) {
        auto& thread = threads_.emplace_back(&subscriber::poll, this, std::move(recv_queues[++index]));
        zip::util::pin_thread(thread, cpu);
    }

    // start the threads for processing the log entries
    index = 0;
    for (auto& cpu: application_cpus) {
        auto& thread = threads_.emplace_back(&subscriber::process, this, index++, sequential);
        zip::util::pin_thread(thread, cpu);
    }
}

void subscriber::stop() {
    // set the stop signal and wait for the threads
    stop_.store(true, std::memory_order_relaxed);
    for (auto& thread: threads_) {
        thread.join();
    }
}

/**
 * This method communicated with the ordering service.
 */
void subscriber::control(zip::network::recv_queue recv_queue) {
    // map from shard IDs to a map from replica IDs to send queues to storage servers
    std::array<std::unordered_map<uint64_t, std::vector<zip::network::send_queue>>, zip::consts::NUM_SHARDS> storage_queues;

    // map from shard IDs to a map from replica IDs to number of finished messages received
    std::array<std::unordered_map<uint64_t, unsigned long>, zip::consts::NUM_SHARDS> storage_finished;

    // state whether to continue the processing loop
    bool order_finished = false, stopped = false, end = false;

    // keep processing in a loop
    while (!end) {
        // recycle blocks in the log
        log_.recycle_blocks();

        // check if shutdown has been requested
        if (!stopped && (stopped = stop_.load(std::memory_order_relaxed))) {
            // prepare an end request
            zip::api::subscriber_finished req;
            req.message_type = zip::api::SUBSCRIBER_FINISHED;
            req.subscriber_id = subscriber_id_;

            // send the finish request to the servers
            order_.send(&req, req.length(), true);
            for (uint64_t sid = 0; sid < zip::consts::NUM_SHARDS; sid++) {
                for (auto& [_, queues]: storage_queues[sid]) {
                    queues.front().send(&req, req.length(), true);
                }
            }
        }

        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue,
            [&] (zip::api::subscriber_storage_info& info) {
                // connect to all the servers in the list
                for (unsigned long i = 0; i < info.num_servers; i++) {
                    // try to connect to the server
                    auto connection = manager_.connect(info.addresses[i], &intro_, intro_.length(), 0);
                    if (!connection) {
                        logger.warn("Failed to connect to storage server");
                        continue;
                    }

                    // verify the server's intro message
                    auto& [send_queues, buffer, length] = *connection;
                    auto& intro = *static_cast<zip::api::storage_intro*>(buffer);
                    if (   intro.length() != length
                        || intro.message_type != zip::api::STORAGE_INTRO
                        || intro.shard_id >= zip::consts::NUM_SHARDS
                        || storage_queues[intro.shard_id].contains(intro.replica_id)) {
                        logger.warn("Failed to validate storage server information");
                        continue;
                    }

                    // add the new storage server to the state
                    logger.info("Established connection with storage server (", intro.shard_id, ":", intro.replica_id, ")");
                    storage_queues[intro.shard_id][intro.replica_id] = std::move(send_queues);
                }
            },
            [&] ([[maybe_unused]] zip::api::order_finished& bye) {
                // mark the ordering server connection as finalized
                logger.info("Ended connection with ordering service");
                ZIP_ASSERT(!order_finished, "finalize message already received");
                order_finished = true;

                // check whether all the storage connections are finalized
                auto pred = [] (auto& queues) { return queues.empty(); };
                if (std::all_of(storage_queues.begin(), storage_queues.end(), pred)) {
                    process_.store(false, std::memory_order_relaxed);
                    end = true;
                }
            },
            [&] (zip::api::storage_finished& bye) {
                // verify that the storage server exists
                if (bye.shard_id >= zip::consts::NUM_SHARDS || !storage_queues[bye.shard_id].contains(bye.replica_id)) {
                    logger.warn("Failed to disconnect with storage server (", bye.shard_id, ":", bye.replica_id, ") as it does not exist");
                    return;
                }

                // get the state for this shard
                auto& finished = storage_finished[bye.shard_id];
                auto& queues = storage_queues[bye.shard_id];

                // increased the finished count for this replica
                finished[bye.replica_id]++;

                // check whether this replica is finalized
                if (finished[bye.replica_id] == queues[bye.replica_id].size()) {
                    logger.info("Ended connection with storage server (", bye.shard_id, ":", bye.replica_id, ")");
                    finished.erase(bye.replica_id);
                    queues.erase(bye.replica_id);
                }

                // check whether all the storage connections are finalized
                auto pred = [] (auto& queues) { return queues.empty(); };
                if (order_finished && std::all_of(storage_queues.begin(), storage_queues.end(), pred)) {
                    process_.store(false, std::memory_order_relaxed);
                    end = true;
                }
            }
        );
    }
}

/**
 * This method polls the given receive queue and inserts the entries
 * into the log.
 */
void subscriber::poll(zip::network::recv_queue recv_queue) {
    // sorted map of log entries currently being processed
    std::unordered_map<uint64_t, zip::api::subscriber_log_entry*> current_requests;

    // map from shard IDs and thread IDs to the latest GSN and position
    std::array<std::unordered_map<uint64_t, std::pair<uint64_t, position>>, zip::consts::NUM_SHARDS> shard_info;

    // keep processing in a loop until shutdown
    while (process_.load(std::memory_order_relaxed)) {
        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue,
            [&] (zip::api::subscriber_log_entry& current) {
                // get the details for this shard
                logger.trace("Received entry with GSN ", current.gsn, " from client (", current.client_id, ") from storage server (", current.shard_id, ":", current.replica_id, ") with paylod of size ", current.data_length);
                auto& [gsn, position] = shard_info[current.shard_id].try_emplace(current.thread_id, std::numeric_limits<uint64_t>::max(), log_).first->second;

                // stop processing if this entry has already been delivered
                if (gsn != std::numeric_limits<uint64_t>::max() && current.gsn <= gsn) return;

                // search through current requests to see if this entry is in-flight
                auto& entry = current_requests[current.gsn];

                // if this is a new entry then it must be initialized
                // otherwise, we need to verify the received parameters
                if (entry == nullptr) {
                    // copy the data and set the number of ACKs that must be received
                    entry = static_cast<zip::api::subscriber_log_entry*>(std::malloc(current.length()));
                    std::memcpy(entry, &current, current.length());
                    entry->num_acks = 0;
                } else {
                    // verify the entry parameters and free the buffer for reuse
                    if (   entry->shard_id    != current.shard_id
                        || entry->client_id   != current.client_id
                        || entry->thread_id   != current.thread_id
                        || entry->data_length != current.data_length) {
                        logger.warn("Failed to validate entry with GSN ", current.gsn, " from client (", current.client_id, ")");
                        return;
                    }
                }

                // increment the ACKs for this entry, and
                // if delivered, update the latest GSN
                // and insert this into the log
                if (++entry->num_acks > failures_) {
                    gsn = current.gsn;
                    position.insert(entry);
                    current_requests.erase(gsn);
                }
            }
        );
    }
}

/**
 * This method iterates over the log and processes the log entries.
 */
void subscriber::process(unsigned long index, bool sequential) {
    // create the iterator for the log
    auto log = sequential ? sequential_iterator(log_, index, application_threads_) : iterator(log_, index, application_threads_);

    // iterate over the log and process the entries
    while (process_.load(std::memory_order_relaxed)) {
        if (auto entry = log.next_entry(); entry != nullptr) {
            if (entry->data_length > 0) callback_(index, *entry);
            std::free(entry);
        }
    }

    // process and free the remaining entries
    while (auto entry = log.next_entry()) {
        if (entry->data_length > 0) callback_(index, *entry);
        std::free(entry);
    }
}

} // namespace zip::subscriber
