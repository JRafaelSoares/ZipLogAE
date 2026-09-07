#include <atomic>
#include <zip/client/client.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <list>
#include <optional>
#include <set>
#include <utility>

#include <zip/api/api.h>
#include <zip/network/buffer.h>
#include <zip/network/manager.h>
#include <zip/util/concurrent.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/switch.h>
#include <zip/util/util.h>

#include "zip/network/erpc_constants.h"

namespace zip::client {

/** create a logger for this file */
static zip::util::logger logger("client");

client::client(
        zip::network::manager& manager,
        zip::network::erpc_transport_factory& transport,
        std::string order,
        client_t client_id,
        shard_t shard_id,
        uint32_t failures,
        double min_fraction,
        double max_fraction,
        uint16_t cpu_id,
        std::set<std::string> servers,
        std::string local_ip,
        uint16_t local_port
):
client_id_(client_id), shard_id_(shard_id), failures_(failures), manager_(manager), min_fraction_(min_fraction), max_fraction_(max_fraction) {
    // verify arguments for correctness
    ZIP_ASSERT(min_fraction_ > 0 && min_fraction_ < max_fraction_ && max_fraction_ < 1, "invalid value of minimum and maximum fraction");
    ZIP_ASSERT(client_id_ < zip::consts::MAX_CLIENTS, "invalid client ID");

    transport_ = &transport;

    // create the clients's intro message
    intro_.message_type = intro_.tag;
    intro_.client_id = client_id_;
    intro_.shard_id = shard_id_;
    intro_.address = api::net_info_to_msg({
        .host = local_ip,
        .port = local_port
    });

    // start the thread for processing client requests
    thread_ = std::thread(&client::loop, this, order, servers);
    zip::util::pin_thread(thread_, cpu_id);

}

void client::stop() {
    // set the stop signal and wait for the thread
    stop_.store(true, std::memory_order_relaxed);
    thread_.join();
}

void client::add_server(std::string address) {
    // try to connect to the storage server
    auto send_endpoint = transport_->create_send_endpoint(address, network::STORAGE_SERVER_OFFSET+1);
    auto buf = transport_->get_buffer();
    auto len = send_endpoint->request_reply(&intro_, intro_.length(), *buf.get(), *recv_endpoint_);
    auto& intro = *static_cast<zip::api::storage_intro*>(buf.get()->buffer_);
    ZIP_ASSERT_EQ(intro.length(), len, "length of the received packet is invalid");
    ZIP_ASSERT_EQ(intro.message_type, zip::api::STORAGE_INTRO, "received an unknown type of message");

    if ( intro.shard_id != shard_id_
        || std::ranges::any_of(server_queue_, [&] (auto& elem) { return elem.first == intro.replica_id; })) {
        logger.warn("Failed to validate storage server information");
        return;
    }
    int index = (client_id_ % intro.client_threads);
    auto send_endpoint_thread = transport_->create_send_endpoint(address, network::STORAGE_SERVER_OFFSET + 3 + index);
    // add the storage server to the state
    logger.info("Established connection with storage server (", intro.shard_id, ":", intro.replica_id, ")");
    server_queue_.emplace_back(intro.replica_id, std::move(send_endpoint_thread));
}

void client::append(zip::network::buffer& buffer,  std::atomic<gsn_t>& response) {
    // prepare the message to send
    auto& message = buffer.as<zip::api::storage_append>();
    ZIP_ASSERT(message.data_length <= zip::consts::MAX_PAYLOAD, "invalid size of the payload");
    message.message_type = message.tag;
    message.client_id = client_id_;

    // prepare the response and enqueue the request
    response.store(std::numeric_limits<gsn_t>::max(), std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_acq_rel);
    requests_.enqueue({&buffer, &response});
}

gsn_t client::append(zip::network::buffer& buffer) {
    // prepare the message to send
    auto& message = buffer.as<zip::api::storage_append>();
    ZIP_ASSERT(message.data_length <= zip::consts::MAX_PAYLOAD, "invalid size of the payload");
    message.message_type = message.tag;
    message.client_id = client_id_;

    // prepare the response and enqueue the request
    std::atomic<gsn_t> response = std::numeric_limits<gsn_t>::max();
    std::atomic_thread_fence(std::memory_order_acq_rel);
    requests_.enqueue({&buffer, &response});

    // wait until we receive a response and return
    gsn_t result;
    while ((result = response.load(std::memory_order_relaxed)) == std::numeric_limits<gsn_t>::max()) zip::util::relax();
    return result;
}

namespace detail {

/**
 * This struct stores the information about an epoch.
 */
struct epoch {

    /** timestamp of the beginning of the epoch */
    std::chrono::high_resolution_clock::time_point begin;

    /** index at the start of the epoch */
    uint64_t first;

    /** index of the end of the epoch */
    uint64_t last;

};

/** constant for first epoch */
static constexpr epoch FIRST_EPOCH {std::chrono::high_resolution_clock::time_point::min(), 0, 0};

/**
 * This struct stores the data of an outstanding request.
 */
struct outstanding {

    /** location to store the response */
    std::atomic<gsn_t>* response = nullptr;

    /** the temporary GSN received in an ACK */
    gsn_t gsn = std::numeric_limits<gsn_t>::max();

    /** number of ACKs received */
    uint32_t num_acks = 0;

};

} // namespace detail

/**
 * This method processes this client's requests in a loop.
 */
void client::loop(std::string order, std::set<std::string> servers) {
    transport_->set_global_rpc_id_(zip::network::CLIENT_SERVER_OFFSET);

    // create the network receive queue
    recv_endpoint_ = transport_->create_recv_endpoint();

    // connect to all the storage servers
    for (auto& address: servers) add_server(address);
    ZIP_ASSERT(server_queue_.size() > failures_, "not enough storage servers for failure resilience");

    // connect with the ordering server
    auto [ip, port] = zip::util::split_address(order);
    auto order_init = transport_->create_send_endpoint(ip, port, network::ORDER_SERVER_OFFSET);
    ZIP_ASSERT_NOT_NULL(order_init.get(), "failed to create ordering init endpoint");
    auto buf = transport_->get_buffer();

    auto len = order_init->request_reply(&intro_, intro_.length(), *buf.get(), *recv_endpoint_);

    // verify the ordering server's intro message
    auto& intro = *static_cast<zip::api::order_intro*>(buf.get()->buffer_);
    ZIP_ASSERT_EQ(intro.length(), len, "length of the received packet is invalid");
    ZIP_ASSERT_EQ(intro.message_type, zip::api::ORDER_INTRO, "received an unknown type of message");
    logger.info("Established connection with the ordering service");

    order_ = transport_->create_send_endpoint(ip, port, network::ORDER_SERVER_OFFSET+1);
    ZIP_ASSERT_NOT_NULL(order_.get(), "failed to create ordering control endpoint");

    // start the execution of processing threads
    start_.store(true, std::memory_order_release);
    start_.notify_all();

    // wait until we're allowed to start executing
    start_.wait(false, std::memory_order_acquire);

    // state of whether to continue the processing loop
    bool stopped = false, end = false;

    // number of slots consumed and assigned to this client
    uint64_t consumed = 0, assigned = 0;

    // tracking the epochs assigned to this client
    auto current = detail::FIRST_EPOCH;
    std::list<detail::epoch> epochs;

    // state to keep track of how many slots were consumed
    uint32_t filled_slots = 0, total_slots = zip::consts::MIN_SLOTS, current_slots = zip::consts::MIN_SLOTS;

    // create a message for sending rate changing requests
    zip::api::order_client_rate rate;
    rate.message_type = rate.tag;
    rate.client_id = client_id_;
    rate.num_slots = current_slots;

    // create a message for sending no-op entries
    zip::api::storage_append empty;
    empty.message_type = empty.tag;
    empty.client_id = client_id_;
    empty.data_length = 0;

    // create a message for sending an end request
    zip::api::client_finished finished;
    finished.message_type = finished.tag;
    finished.client_id = client_id_;

    // state for the currently outstanding requests
    std::list<detail::outstanding> outstanding;
    gsn_t last_gsn = std::numeric_limits<gsn_t>::max();

    // keep processing in a loop until shutdown
    while (!end) {
        // spin loop optimization
        zip::util::relax();

        // check if we need to stop processing
        if (!stopped && (stopped = stop_.load(std::memory_order_relaxed))) {
            // send the finish request to the ordering server and storage servers
            order_->send(&finished, finished.length());
            for (auto& [_, queue]: server_queue_) {
                queue->send(&finished, finished.length());
            }
        }

        // check if we need to switch to the next epoch
        auto now = std::chrono::high_resolution_clock::now();
        if (!stopped && !epochs.empty() && epochs.front().begin <= now) {
            // switch to the next epoch
            current = epochs.front();
            epochs.pop_front();

            // update rate estimation state and increase the requested rate if required
            auto fraction = static_cast<double>(filled_slots) / static_cast<double>(total_slots);
            filled_slots = 0; total_slots = current.last - current.first;

            
            if (fraction > max_fraction_) {
                current_slots += zip::consts::MIN_SLOTS;
            }
            else if (fraction < min_fraction_ && current_slots > zip::consts::MIN_SLOTS) {
                current_slots -= zip::consts::MIN_SLOTS;
            }

            // send a request to update rate if needed
            if (rate.num_slots != current_slots) {
                logger.trace("Updating requested number of slots to ", current_slots);
                rate.num_slots = current_slots;
                order_->send(&rate, rate.length());
            }
        }

        // based on where we are in the epoch we need to calculate where we should be
        auto ideal = std::min(current.last, current.first + (current.last - current.first) * (now - current.begin) / zip::consts::EPOCH_DURATION);

        // if we can send a request now and we are behind in the log position then send a request
        if (!stopped && consumed < ideal && outstanding.size() < zip::consts::MAX_OUTSTANDING) {
            // calculate how behind we are on the schedule
            auto advanced = ideal - consumed;

            // there is a request available then send that, otherwise
            // if there there are no outstanding requests then send a no-op
            if (auto request = requests_.dequeue()) {
                // create an entry for the outgoing request
                logger.trace("Sending a append request advancing by ", advanced, " slots");
                outstanding.emplace_back(detail::outstanding {request->second});
                consumed += advanced;
                filled_slots++;

                // send the append request
                auto& message = request->first->as<zip::api::storage_append>();
                message.num_slots = advanced;
                for (auto& [_, queue]: server_queue_) queue->send(*(request->first), message.length());
            } else if (outstanding.empty()) {
                // update the state indicating that we have sent the no-op
                logger.trace("Sending an empty request advancing by ", advanced, " slots");
                outstanding.emplace_back();
                empty.num_slots = advanced;
                consumed += advanced;

                // send the empty request
                for (auto& [_, queue]: server_queue_) queue->send(&empty, empty.length());
            }
        }

        // poll the receive queue for a packet
        zip::util::recv_apply(logger, *recv_endpoint_,
            [&] (zip::api::client_append_ack& ack) {
                // check whether an ack for this request has been delivered
                logger.trace("Received ACK from storage server (", shard_id_, ":", ack.replica_id, ") with GSN ", ack.gsn);
                if (last_gsn != std::numeric_limits<gsn_t>::max() && ack.gsn <= last_gsn) return;

                // find the request for which this ACK is received
                for (auto it = outstanding.begin(); it != outstanding.end(); ++it) {
                    if (it->gsn == std::numeric_limits<gsn_t>::max() || it->gsn == ack.gsn) {
                        // save the GSN for this request
                        if (it->gsn == std::numeric_limits<gsn_t>::max()) it->gsn = ack.gsn;

                        // if all the ACKs for this request are received,
                        // then set the response and remove from outstanding queue
                        if (++it->num_acks > failures_) {
                            if (it->response != nullptr) it->response->store(ack.gsn, std::memory_order_relaxed);
                            outstanding.erase(it);
                            last_gsn = ack.gsn;
                        }
                        break;
                    }
                }
            },
            [&] (zip::api::client_new_epoch& epoch) {
                // update the epoch state and ACK this request
                logger.trace("Received new epoch beginning at ", epoch.begin, " with ", epoch.num_slots, " slots");
                epochs.emplace_back(detail::epoch {zip::util::time_from_ns(epoch.begin), assigned, assigned += epoch.num_slots});
            },
            [&] (zip::api::storage_finished& bye) {
                // verify that the storage server exists
                if (   bye.shard_id != shard_id_
                    || std::ranges::none_of(server_queue_, [&] (auto& elem) { return elem.first == bye.replica_id; })) {
                    logger.warn("Failed to disconnect with storage server (", bye.shard_id, ":", bye.replica_id, ") as it does not exist");
                    return;
                }

                // remove the storage server from the state
                logger.info("Ended connection with storage server (", shard_id_, ":", bye.replica_id, ")");
                std::erase_if(server_queue_, [&] (auto& elem) { return elem.first == bye.replica_id; });

                // if all the storage servers and ordering server have
                // been finalized then we can exit the loop
                if (server_queue_.empty() && !order_) end = true;
            },
            [&] ([[maybe_unused]] zip::api::order_finished& bye) {
                // verify that the order connection still exists
                if (!order_) {
                    logger.warn("Failed to disconnect with ordering server as it does not exist");
                    return;
                }

                // mark the order connection as finalized
                logger.info("Ended connection with ordering server");
                order_.reset();

                // if all the storage servers have been finalized
                // then we can exit the loop
                if (server_queue_.empty()) end = true;
            }
        );
    }
}

} // namespace zip::client
