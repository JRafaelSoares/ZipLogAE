#include "storage/storage.h"

#include <cstring>
#include <functional>
#include <numeric>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "api/api.h"
#include "network/buffer.h"
#include "network/manager.h"
#include "network/recv_queue.h"
#include "util/log.h"
#include "util/switch.h"
#include "util/util.h"

namespace zip::storage {

/** create a logger for this file */
static zip::util::logger logger("storage");

storage::storage(
        zip::network::manager& manager,
        std::set<uint16_t> client_cpus,
        std::set<uint16_t> subscriber_cpus,
        unsigned long subscriber_depth,
        std::string order,
        zip::api::storage_intro& intro,
        uint64_t shard_id,
        uint64_t replica_id
):
manager_(manager), shard_id_(shard_id), replica_id_(replica_id),
client_threads_(client_cpus.size()), subscriber_threads_(subscriber_cpus.size()),
subscriber_depth_(subscriber_depth) {
    // verify arguments for correctness
    ZIP_ASSERT(shard_id_ < zip::consts::NUM_SHARDS, "invalid shard ID");
    ZIP_ASSERT(replica_id_ < zip::consts::MAX_REPLICAS, "invalid replica ID");
    ZIP_ASSERT(client_threads_ > 0, "no client threads created");
    ZIP_ASSERT(subscriber_depth_ > 0, "invalid subscriber depth");

    // create the network receive queues
    auto recv_queues = manager_.create_recv_queues(1 + client_threads_);

    // connect with the ordering service
    auto [ip, port] = zip::util::split_address(order);
    auto connection = manager_.connect(ip, port, &intro, intro.length());
    ZIP_ASSERT(connection, "failed to connect to ordering service");
    auto& [send_queue, buffer, length] = *connection;
    order_ = std::move(send_queue);

    // verify the ordering server's intro message
    auto& req = *static_cast<zip::api::order_intro*>(buffer);
    ZIP_ASSERT_EQ(req.length(), length, "length of the received packet is invalid");
    ZIP_ASSERT_EQ(req.message_type, zip::api::ORDER_INTRO, "received an unknown type of message");
    logger.info("Established connection with the ordering service");

    // setup the finished message
    finished_.message_type = zip::api::STORAGE_FINISHED;
    finished_.shard_id = shard_id_;
    finished_.replica_id = replica_id_;

    // index of the receive queue
    unsigned long queue = 0;

    // start the thread for processing the control loop
    threads_.emplace_back(&storage::control, this, std::move(recv_queues[queue++]));

    // start threads for processing the client loop
    for (auto& cpu: client_cpus) {
        // create and pin this thread and add it to the thread vector
        auto& thread = threads_.emplace_back(&storage::client, this, std::move(recv_queues[queue++]));
        zip::util::pin_thread(thread, cpu);
    }

    // setup state for subscriber threads
    if (subscriber_threads_ == 0) {
        logger.warn("No subscriber threads created which can cause errors if subscribers try to connect");
    }

    // start threads for processing the subscriber loop
    unsigned long subscriber = 0;
    for (auto& cpu: subscriber_cpus) {
        // create and pin this thread and add it to the thread vector
        auto& thread = threads_.emplace_back(&storage::subscriber, this, subscriber++);
        zip::util::pin_thread(thread, cpu);
    }
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
        if (ptr != nullptr) delete ptr;
    }
}

storage::client_state& storage::get_client_state(uint64_t client_id) {
    // try to find the client in
    // the state and return if it exists
    auto& location = client_state_[client_id];
    auto state = location.load(std::memory_order_acquire);
    if (state) return *state;

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

void storage::add_client(uint64_t client_id, zip::network::send_queue send_queue) {
    // verify arguments for correctness
    if (client_id >= zip::consts::MAX_CLIENTS) {
        logger.warn("Could not add client (", client_id, ")");
        return;
    }

    // get or create the state for the client
    // and set the parameters for it
    auto& state = get_client_state(client_id);
    auto lock = std::unique_lock(state.lock);

    // make sure the client is not already connected
    if (state.connected) {
        logger.warn("Could not add client (", client_id, ")");
        return;
    }

    // mark the state of the client as connected
    logger.info("Established connection with client (", client_id, ")");
    send_queue.set_assert_on_failure(false);
    state.send_queue = std::move(send_queue);
    state.connected = true;

    // notify any waiting thread
    state.cv.notify_one();
}

void storage::add_subscriber(uint64_t subscriber_id, unsigned long num_queues, std::vector<zip::network::send_queue> send_queues) {
    // verify arguments for correctness
    auto lock = std::unique_lock(subscriber_lock_);
    if (subscribers_.contains(subscriber_id)) {
        logger.warn("Could not add subscriber (", subscriber_id, ")");
        return;
    }

    // check whether we can overflow the receive
    // buffers for this subscriber
    auto max_queues = zip::consts::rdma::MAX_OUTSTANDING_REQUESTS / zip::consts::rdma::MAX_RECEIVE_REQUESTS;
    if (std::lcm(subscriber_threads_, num_queues) / subscriber_threads_ > max_queues) {
        logger.warn("Chance of data corruption due to receive buffer overflow for subscriber (", subscriber_id, ")");
    }

    // set the subscriber queue depths
    for (auto& queue: send_queues) queue.set_max_outstanding(subscriber_depth_);

    // add the subscriber to the state
    logger.info("Established connection with subscriber (", subscriber_id, ")");
    subscriber_changes_.add(change_t {std::in_place_type<subscriber_state>, subscriber_id, num_queues, std::move(send_queues)});
    subscribers_.emplace(subscriber_id);
}

void storage::control(zip::network::recv_queue recv_queue) {
    // buffers to send log entries to the ordering service
    // for failure recovery
    auto buffers = manager_.get_buffers(zip::consts::rdma::MAX_OUTSTANDING_REQUESTS + 1);
    auto iterator = zip::util::wraparound_iterator(buffers);

    // initialise the metadata for the entries
    for (auto& buffer: buffers) {
        auto& data = buffer.as<zip::api::order_client_data>();
        data.message_type = zip::api::ORDER_CLIENT_DATA;
        data.shard_id = shard_id_;
        data.replica_id = replica_id_;
    }

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // process any failures that might occur
        zip::util::failure_apply(logger, manager_,
            [&] (zip::api::client_intro& intro) {
                // get the client's state and take the lock
                auto& state = get_client_state(intro.client_id);
                auto lock = std::unique_lock(state.lock);

                // wait until the client is connected
                state.cv.wait(lock, [&] () { return state.connected; });

                // mark the client as disconnected, if it's not already
                if (!state.disconnected) {
                    logger.info("Marking client (", intro.client_id, ") as suspected as failed");
                    auto discard = std::move(state.send_queue);
                    state.disconnected = true;
                }

                // create a message to notify the ordering service
                zip::api::order_client_freeze freeze;
                freeze.message_type = zip::api::ORDER_CLIENT_FREEZE;
                freeze.client_id = intro.client_id;

                // send the notification to the ordering service
                order_.send(&freeze, freeze.length());
            }
        );

        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue,
            [&] (zip::api::storage_slots& slots) {
                // advance the epoch and add the slots
                if (slots.num_clients > 0) log_.add_entries(slots);
            },
            [&] (zip::api::storage_client_freeze& freeze) {
                // get this client's state and take the lock
                auto& state = get_client_state(freeze.client_id);
                auto lock = std::unique_lock(state.lock);

                // wait until the client is connected
                state.cv.wait(lock, [&] () { return state.connected; });

                // mark the client as disconnected, if it's not already
                if (!state.disconnected) {
                    logger.info("Marking client (", freeze.client_id, ") as suspected as failed");
                    auto discard = std::move(state.send_queue);
                    state.disconnected = true;
                }

                // create a message to send the latest GSN
                zip::api::order_client_cut cut;
                cut.message_type = zip::api::ORDER_CLIENT_CUT;
                cut.client_id = freeze.client_id;
                cut.shard_id = shard_id_;
                cut.replica_id = replica_id_;
                cut.latest_gsn = state.latest_gsn;

                // send the latest GSN to the ordering service
                order_.send(&cut, cut.length());
            },
            [&] (zip::api::storage_client_cut& req) {
                // get this client's state and take the lock
                auto& state = get_client_state(req.client_id);
                auto lock = std::unique_lock(state.lock);

                // create a message to signal flush data
                zip::api::order_client_data flush;
                flush.message_type = zip::api::ORDER_CLIENT_DATA;
                flush.client_id = req.client_id;
                flush.shard_id = shard_id_;
                flush.replica_id = replica_id_;
                flush.gsn = std::numeric_limits<uint64_t>::max();
                flush.data_length = 0;

                // iterate over all the requested entries and send
                // them to the ordering service
                if (state.latest_gsn != std::numeric_limits<uint64_t>::max()) {
                    for (auto entry: state.position.suffix(req.begin_gsn)) {
                        if (entry->data_length > 0) {
                            // copy the entry data into the buffer
                            auto buffer = iterator.get_and_increment();
                            auto& data = buffer->as<zip::api::order_client_data>();
                            data.gsn = entry->gsn;
                            data.client_id = entry->client_id;
                            data.data_length = entry->data_length;
                            if (entry->data_length > sizeof(uint64_t)) {
                                std::memcpy(data.data, reinterpret_cast<void*>(entry->data), entry->data_length);
                            } else {
                                *reinterpret_cast<uint64_t*>(data.data) = entry->data;
                            }

                            // send the buffer to the ordering service
                            order_.send(*buffer, data.length());
                        }
                    }
                }

                // send the flush message to the ordering service
                order_.send(&flush, flush.length());
            },
            [&] (zip::api::storage_client_patch& data) {
                // get this client's state and take the lock
                auto& state = get_client_state(data.client_id);
                auto lock = std::unique_lock(state.lock);

                // patch the data into the client's log
                // if we don't already have this data
                if (data.gsn > state.latest_gsn) {
                    state.position.insert(data.data, data.data_length, data.gsn);
                }
            },
            [&] (zip::api::storage_client_finalize& req) {
                // get this client's state and take the lock
                auto& state = get_client_state(req.client_id);
                auto lock = std::unique_lock(state.lock);

                // finalize the client and close all it's remaining slots
                logger.info("Finalizing the log for client (", req.client_id, ")");
                state.position.close();
            },
            [&] (zip::api::subscriber_finished& bye) {
                // verify that the subscriber exists
                auto lock = std::unique_lock(subscriber_lock_);
                if (!subscribers_.contains(bye.subscriber_id)) {
                    logger.warn("Failed to disconnect with subscriber (", bye.subscriber_id, ") as it does not exist");
                    return;
                }

                // remove the subscriber from the state
                logger.info("Ended connection with subscriber (", bye.subscriber_id, ")");
                subscriber_changes_.add(change_t {std::in_place_type<uint64_t>, bye.subscriber_id});
                subscribers_.erase(bye.subscriber_id);
            }
        );
    }
}

void storage::client(zip::network::recv_queue recv_queue) {
    // ACK message for the clients
    zip::api::client_insert_ack ack;
    ack.message_type = zip::api::CLIENT_INSERT_ACK;
    ack.replica_id = replica_id_;

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // poll the receive queue for a packet
        zip::util::recv_apply(logger, recv_queue,
            [&] (zip::api::storage_insert& req) {
                // get this client's state and take the lock
                auto& state = get_client_state(req.client_id);
                auto lock = std::unique_lock(state.lock);

                // wait until the client is connected
                state.cv.wait(lock, [&] () { return state.connected; });

                // if the client has not been disconnected
                // then insert the entry into the log and send the ACK
                if (!state.disconnected) {
                    state.latest_gsn = ack.gsn = state.position.insert(req.data, req.data_length, req.num_slots);
                    state.send_queue.send(&ack, ack.length());
                }
            },
            [&] (zip::api::client_finished& bye) {
                // get this client's state and take the lock
                auto& state = get_client_state(bye.client_id);
                auto lock = std::unique_lock(state.lock);

                // wait until the client is connected
                state.cv.wait(lock, [&] () { return state.connected; });

                // mark the client as disconnected, if it's not already
                if (!state.disconnected) {
                    logger.info("Ended connection with client (", bye.client_id, ")");
                    state.send_queue.send(&finished_, finished_.length(), true);
                    auto discard = std::move(state.send_queue);
                    state.disconnected = true;
                }
            }
        );
    }
}

void storage::subscriber(unsigned long index) {
    // create state for current subscribers
    std::unordered_map<uint64_t, std::pair<unsigned long, zip::network::send_queue>> subscribers;
    auto changes = zip::util::bag_reader(subscriber_changes_);
    unsigned long num_sent = index;

    // create a buffers and iterators for sending log entries
    auto buffers = manager_.get_buffers(subscriber_depth_ + 1);
    auto iterator = zip::util::wraparound_iterator(buffers);

    // initialize the metadata for the entries
    for (auto& buffer: buffers) {
        auto& req = buffer.as<zip::api::subscriber_log_entry>();
        req.message_type = zip::api::SUBSCRIBER_LOG_ENTRY;
        req.replica_id = replica_id_;
        req.shard_id = shard_id_;
        req.thread_id = index;
    }

    // create the iterator for stepping over the log
    zip::storage::iterator log(log_, index, subscriber_threads_);

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // check if the list of subscribers has changed
        // and materialize it in the local state
        if (auto change = changes.get()) {
            // if this is a new subscriber then add it to the state
            if (std::holds_alternative<subscriber_state>(*change)) {
                // add the subscriber to the state
                auto& state = std::get<subscriber_state>(*change);
                subscribers.try_emplace(state.subscriber_id, state.num_queues, std::move(state.send_queues[index]));
            }
            else if (std::holds_alternative<uint64_t>(*change)) {
                // remove the subscriber from the state
                auto sid = std::get<uint64_t>(*change);
                subscribers[sid].second.send(&finished_, finished_.length(), true);
                subscribers.erase(sid);
            }
        }

        // try to get an entry from the log
        if (auto entry = log.next_entry()) {
            // get the buffer and initialize the entry
            auto buffer = iterator.get_and_increment();
            auto& req = buffer->as<zip::api::subscriber_log_entry>();
            req.client_id = entry->client_id;
            req.data_length = entry->data_length;
            req.gsn = entry->gsn;

            // copy the payload into the message to be sent
            if (entry->data_length > sizeof(uint64_t)) {
                std::memcpy(req.data, reinterpret_cast<void*>(entry->data), entry->data_length);
            } else if (entry->data_length > 0) {
                *reinterpret_cast<uint64_t*>(req.data) = entry->data;
            }

            // send the entry to all subscribers
            for (auto& [sid, state]: subscribers) {
                auto& [num_queues, queue] = state;
                queue.send(*buffer, req.length(), 1 + num_sent % num_queues);
            }

            // update the number of messages sent
            num_sent += subscriber_threads_;
        }
    }
}

} // namespace zip::storage
