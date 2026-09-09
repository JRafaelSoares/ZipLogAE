#include <algorithm>
#include <atomic>
#include <zip/storage/storage.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_set>
#include <utility>

#include <zip/api/api.h>
#include <zip/network/buffer.h>
#include <zip/network/manager.h>
#include <zip/storage/log.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/switch.h>
#include <zip/util/util.h>

#include "zip/network/erpc_constants.h"

namespace zip::storage {

/** create a logger for this file */
static zip::util::logger logger("storage");


namespace {

/** Helper functions to initialize client states. */
template <size_t... Is>
static inline auto init_clients(log& log, std::index_sequence<Is...>) {
    return std::array<client_state, sizeof...(Is)>{client_state {log, Is}...};
}
static inline auto init_clients(log& log) {
    return init_clients(log, std::make_index_sequence<zip::consts::MAX_CLIENTS>());
}

} // namespace detail

storage::storage(
        zip::network::manager& manager,
        network::erpc_transport_factory& transport,
        std::string order,
        shard_t shard_id,
        replica_t replica_id,
        std::set<uint16_t> client_cpus,
        std::set<uint16_t> subscriber_cpus,
        std::chrono::nanoseconds timeout,
        std::string addr,
        uint16_t port
):
manager_(manager), shard_id_(shard_id), replica_id_(replica_id),
client_state_(init_clients(log_)), client_threads_(client_cpus.size()),
subscriber_threads_(subscriber_cpus.size()), timeout_(timeout) {

    transport_ = &transport;
    // verify arguments for correctness
    ZIP_ASSERT(client_threads_ > 0, "no client threads created");
    if (subscriber_threads_ == 0) {
        logger.warn("No subscriber threads created which can cause errors if subscribers try to connect");
    }

    // setup the intro message for the storage server
    intro_.message_type = intro_.tag;
    intro_.shard_id = shard_id_;
    intro_.replica_id = replica_id_;
    intro_.client_threads = client_threads_;
    intro_.address = api::net_info_to_msg({
        .host = addr,
        .port = port
    });

    // setup the finished message
    finished_.message_type = finished_.tag;
    finished_.shard_id = shard_id_;
    finished_.replica_id = replica_id_;

    // start the thread for processing the control loop
    auto& thread = threads_.emplace_back(&storage::control, this);

    // connect with the ordering server
    auto [ip, order_port] = zip::util::split_address(order);
    control_recv_endpoint_ = transport.create_recv_endpoint();

    order_ = transport.create_send_endpoint(ip, order_port, network::ORDER_SERVER_OFFSET);
    ZIP_ASSERT_NOT_NULL(order_.get(), "failed to create ordering control endpoint");
    auto buf = transport.get_buffer();
    auto len = order_->request_reply(&intro_, intro_.length(), *buf.get(), *control_recv_endpoint_);
    ZIP_ASSERT(len != 0, "failed to connect to ordering service");

    // verify the ordering server's intro message
    auto& req = *static_cast<zip::api::order_intro*>(buf.get()->buffer_);
    ZIP_ASSERT_EQ(req.length(), len, "length of the received packet is invalid");
    ZIP_ASSERT_EQ(req.message_type, zip::api::ORDER_INTRO, "received an unknown type of message");
    logger.info("Established connection with the ordering service");

    auto filename = std::format("/tmp/logdir/entries_{}.dat", replica_id_);

    // start threads for processing the client loop
    uint32_t erpc_index = 0;
    uint32_t index = 0;
    for (auto cpu: client_cpus) {
        // create the thread and it's corresponding receive queue
        auto& thread = threads_.emplace_back(&storage::client, this, index);
        zip::util::pin_thread(thread, cpu);
        index++; erpc_index++;
    }

    // initialize the state for the subscriber threads
    subscriber_locks_ = std::make_unique<std::mutex[]>(subscriber_threads_);
    subscriber_queues_ = std::make_unique<std::vector<std::pair<subscriber_t, std::unique_ptr<zip::network::send_endpoint>>>[]>(subscriber_threads_);

    // start threads for processing the subscriber loop
    index = 0;
    for (auto cpu: subscriber_cpus) {
        auto& thread = threads_.emplace_back(&storage::subscriber, this, index, erpc_index);
        zip::util::pin_thread(thread, cpu);
        index++; erpc_index++;
    }

    // start the thread for accepting connections
    threads_.emplace_back([this] () {
        // keep track of number of clients connected
        uint32_t num_clients = 0;

        // keep track of subscribers trying to connect
        std::unordered_set<subscriber_t> subscribers;
        std::unordered_map<subscriber_t, std::vector<std::unique_ptr<zip::network::send_endpoint>>> connecting;

        network::erpc_transport_factory::set_local_rpc_id_(1);
        auto recv_endpoint = transport_->create_recv_endpoint();

        // process this in a loop
        while (!stop_.load(std::memory_order_relaxed)) {

            // check if this is a client or a subscriber
            zip::util::recv_apply(logger, *recv_endpoint,
                [&] (zip::api::client_intro& intro) {
                    // make sure the client being added is valid
                    if (intro.client_id >= zip::consts::MAX_CLIENTS || intro.shard_id != shard_id_) {
                        logger.warn("Could not add client (", intro.client_id, ")");
                        return;
                    }

                    // get the state for the client
                    auto& state = client_state_[intro.client_id];

                    // make sure the client is not already connected
                    if (state.connected.load(std::memory_order_relaxed)) {
                        logger.warn("Could not add client (", intro.client_id, ")");
                        return;
                    }

                    // mark the state of the client as connected
                    logger.info("Established connection with client (", intro.client_id, ")");
                    state.send_endpoint = transport_->create_send_endpoint(intro.address, network::CLIENT_SERVER_OFFSET);

                    state.send_endpoint->send(&intro_, intro_.length());
                    state.connected.store(true, std::memory_order_release);
                    state.connected.notify_one();
                    num_clients++;
                },
                [&] (zip::api::subscriber_intro& intro) {
                    // make sure the subscriber being added is valid
                    if (num_clients > 0 || subscribers.contains(intro.subscriber_id)) {
                        logger.warn("Could not add subscriber (", intro.subscriber_id, ")");
                        return;
                    }

                    // add the subscriber send queue to the map
                    auto it = connecting.try_emplace(intro.subscriber_id).first;
                    auto& queues = it->second;
                    queues.emplace_back(transport_->create_send_endpoint(intro.address, network::SUBSCRIBER_SERVER_OFFSET + intro.erpc_index));
                    logger.info("Issuing message to subscriber");
                    queues.back()->send(&intro_, intro_.length());
                    // if the number of queues is correct, add the subscriber
                    if (queues.size() == subscriber_threads_) {
                        // add the subscriber to the state for the corresponding thread
                        logger.info("Established connection with subscriber (", intro.subscriber_id, ")");
                        for (uint32_t i = 0; i < subscriber_threads_; i++) {
                            auto lock = std::unique_lock(subscriber_locks_[i]);
                            subscriber_queues_[i].emplace_back(intro.subscriber_id, std::move(queues[i]));
                        }

                        // add the subscriber to the set
                        subscribers.emplace(intro.subscriber_id);
                        connecting.erase(it);

                        // add the subscriber to the state
                        auto lock = std::unique_lock(subscriber_lock_);
                        subscribers_.emplace(intro.subscriber_id);
                    }
                }
            );
        }
    });

    // start the execution of processing threads
    start_.store(true, std::memory_order_release);
    start_.notify_all();
}

storage::~storage() {
    // set the stop signal and wait for threads
    stop_.store(true, std::memory_order_relaxed);
    for (auto& thread: threads_) thread.join();
}

void storage::control() {
    // wait until we're allowed to start executing
    start_.wait(false, std::memory_order_acquire);

    network::erpc_transport_factory::set_local_rpc_id_(2);
    auto recv_endpoint = transport_->create_recv_endpoint();

    // state to keep track of when to exit the loop
    bool stopped = false, end = false;

    // keep processing in a loop until shutdown
    while (!end) {
        // spin loop optimization
        zip::util::relax();

        // try to check if we have been asked to exit, and
        // send the finished message to the ordering server
        if (!stopped && (stopped = stop_.load(std::memory_order_relaxed))) {
            order_->send(&finished_, finished_.length());

            // don't block shutdown on an order_finished reply: the ordering
            // server may already be shutting down itself and never send one,
            // which would otherwise hang this thread (and the process) forever
            end = true;
            break;
        }

        // poll the receive queue for a packet
        zip::util::recv_apply(logger, *recv_endpoint,
            [&] (zip::api::storage_new_epoch& epoch) {
                // advance the epoch and add the slots
                log_.add_entries(epoch);
            },
            [&] (zip::api::storage_finalize_client& req) {
                // get this client's state
                auto& state = client_state_[req.client_id];

                // mark the client as finalized
                auto lock = std::unique_lock(state.lock);
                state.finalized = true;
                auto close = state.disconnected;
                lock.unlock();

                // if the client has already disconnected, then
                // close all the remaining slots of this client
                if (close) {
                    logger.info("Finalizing the log for the client (", req.client_id, ")");
                    state.pos.close();
                }
            },
            [&] (zip::api::subscriber_finished& bye) {
                // verify that this subscriber exists
                auto lock = std::unique_lock(subscriber_lock_);
                if (!subscribers_.contains(bye.subscriber_id)) {
                    logger.warn("Failed to disconnect with subscriber (", bye.subscriber_id, ") as it does not exist");
                    return;
                }
                subscribers_.erase(bye.subscriber_id);
                lock.unlock();

                // disconnect from the subscriber and send the final message
                logger.info("Ended connection with subscriber (", bye.subscriber_id, ")");
                for (uint32_t index = 0; index < subscriber_threads_; index++) {
                    // find the send queue for the subscriber
                    auto lock = std::unique_lock(subscriber_locks_[index]);
                    auto it = std::ranges::find_if(subscriber_queues_[index], [&] (auto& elem) { return elem.first == bye.subscriber_id; });

                    // send the final message and erase the subscriber
                    it->second->send(&finished_, finished_.length());
                    subscriber_queues_[index].erase(it);
                }
            }
        );
    }
}

void storage::client(uint32_t index) {
    // wait until we're allowed to start executing
    start_.wait(false, std::memory_order_acquire);
    network::erpc_transport_factory::local_rpc_id_ = index+3; // +3 because [0, 2] is used for control messages
    auto recv_endpoint = transport_->create_recv_endpoint();

    // Thread-local variables for file management
    int current_file_ = 0;
    int appended_entries_batch_ = 0;
    auto filename =  std::format("/tmp/logdir/entries_{}_{}_{}.dat", replica_id_, index, current_file_);
    int fd_ = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);

    if (fd_ == -1) {
        std::cerr << "open failed: "
                  << strerror(errno) << std::endl;
    }
    // create a message to send an ACK for a append request
    zip::api::client_append_ack ack;
    ack.message_type = ack.tag;
    ack.replica_id = replica_id_;

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // spin loop optimization
        zip::util::relax();

        // poll the receive queue for a message
        zip::util::recv_apply(logger, *recv_endpoint,
            [&] (zip::api::storage_append& req) {
                // get this client's state and wait until connected
                auto& state = client_state_[req.client_id];
                state.connected.wait(false, std::memory_order_acquire);
                // insert the entry into the log and send an ACK to the client
                ack.gsn = state.pos.insert(req.data, req.data_length, req.num_slots, fd_);
                state.send_endpoint->send(&ack, ack.length());
                /*
                if (++appended_entries_batch_ == 100000)
                {
                    //fsync(fd_);
                    close(fd_);
                    filename =  std::format("/tmp/logdir/entries_{}_{}_{}.dat", replica_id_, index, ++current_file_);
                    fd_ = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
                    appended_entries_batch_ = 0;
                }
                */
            },
            [&] (zip::api::client_finished& bye) {
                // get this client's state and wait until connected
                auto& state = client_state_[bye.client_id];
                state.connected.wait(false, std::memory_order_acquire);

                // send the reply to the client and close the connection
                logger.info("Ended connection with client (", bye.client_id, ")");
                state.send_endpoint->send(&finished_, finished_.length());
                state.send_endpoint.reset();

                // mark the client as disconnected
                auto lock = std::unique_lock(state.lock);
                state.disconnected = true;
                auto close = state.finalized;
                lock.unlock();

                // if the client has been finalized, then
                // close all the remaining slots of this client
                if (close) {
                    logger.info("Finalizing the log for the client (", bye.client_id, ")");
                    state.pos.close();
                }
            }
        );
    }
    close(fd_);
}

void storage::subscriber(uint32_t index, uint32_t erpc_index) {
    // wait until we're allowed to start executing
    start_.wait(false, std::memory_order_acquire);
    network::erpc_transport_factory::local_rpc_id_ = erpc_index+3; // +3 because [0, 2] is used for control messages

    // get the state for this thread
    auto& mutex = subscriber_locks_[index];
    auto& subscribers = subscriber_queues_[index];

    // get buffers for sending log entries to subscribers
    auto buffers = manager_.get_buffers(zip::consts::MAX_OUTSTANDING);

    // initialze the buffers with the basic metadata
    for (auto& buffer: buffers) {
        auto& entries = buffer->as<zip::api::subscriber_log_entries>();
        entries.message_type = entries.tag;
        entries.shard_id = shard_id_;
        entries.replica_id = replica_id_;
    }

    // create the iterator for the log
    auto iterator = zip::storage::iterator(log_);

    // create state for batching and timer for flushing
    auto batch = buffers.begin();
    uint32_t num_entries = 0, data_length = 0;
    auto now = std::chrono::high_resolution_clock::now(), next_flush = now + timeout_;

    // flush the log entries out to the subscribers
    auto flush = [&] () {
        // prepare the message to send
        auto& entries = (*batch)->as<zip::api::subscriber_log_entries>();
        entries.data_length = data_length;
        entries.num_entries = num_entries;

        // send the message to all subscribers
        auto lock = std::unique_lock(mutex);
        for (auto& [_, queue]: subscribers) {
            queue->send(**batch, entries.length());
        }
        lock.unlock();

        // update the state for the next batch
        num_entries = data_length = 0;
        batch = std::next(batch) == buffers.end() ? buffers.begin() : std::next(batch);
    };

    // keep processing in a loop until shutdown
    while (!stop_.load(std::memory_order_relaxed)) {
        // spin loop optimization
        zip::util::relax();


        // try to get the next entry from the log
        if (auto next = iterator.next_entry()) {
            if (next->gsn % subscriber_threads_ == index) {
                // if the entry cannot fit into the current batch, then flush it
                if (data_length + sizeof(zip::api::subscriber_log_entries::entry_t) + next->data_length > zip::consts::MAX_PAYLOAD) {
                    flush();
                    next_flush = std::chrono::high_resolution_clock::now() + timeout_;
                }

                // serialize the entry into the current batch
                auto& entries = (*batch)->as<zip::api::subscriber_log_entries>();
                auto& entry = *reinterpret_cast<zip::api::subscriber_log_entries::entry_t*>(entries.entries + data_length);
                entry.client_id = next->client_id;
                entry.data_length = next->data_length;
                entry.gsn = next->gsn;
                if (next->data_length > sizeof(uintptr_t)) {
                    std::memcpy(entry.data, reinterpret_cast<void*>(next->data), next->data_length);
                } else if (next->data_length > 0) {
                    std::memcpy(entry.data, &(next->data), next->data_length);
                }

                // update the number of entries
                data_length += entry.length();
                num_entries++;
            }
        }

        // flush the batch if the timer is expired
        if (num_entries > 0 && (now = std::chrono::high_resolution_clock::now()) > next_flush) {
            next_flush = now + timeout_;
            flush();
        }
    }
}

} // namespace zip::storage
