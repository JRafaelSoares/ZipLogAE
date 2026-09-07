#include "subscriber/subscriber.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include <netinet/in.h>

#include "network/manager.h"
#include "util/log.h"
#include "util/util.h"

namespace zip {
namespace subscriber {

/** create a logger for this file */
static zip::util::logger logger("subscriber");

subscriber::subscriber(
        zip::network::manager& manager,
        std::string order,
        uint64_t subscriber_id,
        uint64_t start_gsn,
        bool ordered
): subscriber_id_(subscriber_id), expected_gsn_(start_gsn), ordered_(ordered), manager_(manager) {
    // create the network receive queue
    recv_queue_ = std::move(manager_.create_recv_queues(1).front());

    // initialise the struct which is subscriber's first message
    intro_.message_type = zip::api::SUBSCRIBER_INTRO;
    intro_.subscriber_id = subscriber_id;
    intro_.start_gsn = start_gsn;

    // connect with the ordering service
    auto [ip, port] = zip::util::split_address(order);
    auto [send_queue, buffer, length] = manager.connect(ip, port, &intro_, intro_.length());
    order_ = std::move(send_queue);

    // verify the ordering server's intro message
    auto& intro = *static_cast<zip::api::order_intro*>(buffer);
    ZIP_ASSERT(length == intro.length(), "length of the received packet is invalid");
    ZIP_ASSERT(intro.message_type == zip::api::ORDER_INTRO, "received an unknown type of message (", intro.message_type, ")");
    logger.info("Established connection with the ordering service");

    // create and post buffers on this receive queue
    for (unsigned long i = 0; i < zip::consts::NUM_BUFFER_SIZES; i++) {
        used_buffers_[i] = manager.get_buffers(zip::consts::BUFFER_SIZES[i], zip::consts::rdma::NUM_RECEIVE_BUFFERS);
        recv_queue_.arm(used_buffers_[i]);
    }

    // initialise the arrays
    latest_gsn_.fill(-1);
    for (auto& iterators: storage_iterators_) {
        iterators.fill(current_requests_.end());
    }
}

subscriber::~subscriber() {
    // prepare an end request
    zip::api::subscriber_finished req;
    req.message_type = zip::api::SUBSCRIBER_FINISHED;
    req.subscriber_id = subscriber_id_;

    // send the finish request to the servers
    order_.send(&req, req.length(), true);
    for (uint64_t i = 0; i < zip::consts::NUM_SHARDS; i++) {
        for (auto& queue: storage_queues_[i]) {
            auto destination = 1 + storage_client_queues_[i] + (subscriber_id_ % storage_subscriber_queues_[i]);
            queue.send(&req, req.length(), true, destination);
        }
    }
}

/**
 * This method polls the subscribers receive queues
 * and requests and returns log records.
 */
unsigned long subscriber::poll(const std::function<void(zip::api::subscriber_log_entry&)>& callback) {
    // state for received packets
    zip::network::buffer* buffer;
    unsigned long length;

    // how many times callback was executed
    unsigned long executed = 0;

    // look in the current entries to see if
    // some in-order entries can be delivered
    if (ordered_) {
        while (true) {
            // if there are no requests available,
            // the exit the loop
            if (current_requests_.empty()) break;

            // check if the smallest GSN is the expected
            // and has been delivered
            auto& first = current_requests_.front();
            if (first->replica_id != 0 || first->gsn != expected_gsn_) break;

            // execute the callback
            if (first->data_length > 0) {
                callback(*first);
                executed++;
            }
            expected_gsn_++;

            // free the entry
            free_requests_.emplace_back(std::move(first));
            current_requests_.pop_front();
        }
    }

    // poll the receive queue for a packet
    if (recv_queue_.recv(buffer, length)) {
        // the first 64 bytes if the message
        // is the message type
        auto req_type = buffer->as<uint64_t>();
        switch (req_type) {
            case zip::api::SUBSCRIBER_STORAGE_INFO:
            {
                // handle a subscriber shard into
                auto& info = buffer->as<zip::api::subscriber_storage_info>();
                ZIP_ASSERT(length == info.length(), "length of the received packet is invalid");

                // connect to all the servers in the list
                for (unsigned long i = 0; i < info.num_servers; i++) {
                    // connect to the server
                    auto [send_queue, buffer, length] = manager_.connect(info.addresses[i], &intro_, intro_.length());

                    // verify the server's intro message
                    auto& intro = *static_cast<zip::api::storage_intro*>(buffer);
                    ZIP_ASSERT(length == intro.length(), "length of the received packet is invalid");
                    ZIP_ASSERT(intro.message_type == zip::api::STORAGE_INTRO, "received an unknown type of message (", intro.message_type, ")");
                    logger.info("Established connection with storage server (", intro.shard_id, ":", intro.replica_id, ")");

                    // get the number of client and subscriber queues
                    auto& subscriber_queues = storage_subscriber_queues_[intro.shard_id];
                    auto& client_queues = storage_client_queues_[intro.shard_id];
                    auto& replicas = storage_replicas_[intro.shard_id];
                    if (replicas.empty()) {
                        subscriber_queues = intro.subscriber_queues;
                        client_queues = intro.client_queues;
                    } else {
                        ZIP_ASSERT(subscriber_queues == intro.subscriber_queues, "invalid number of subscriber queues");
                        ZIP_ASSERT(client_queues == intro.client_queues, "invalid number of client queues");
                    }

                    // add the new storage server to the state
                    auto it = std::find(replicas.begin(), replicas.end(), intro.replica_id);
                    ZIP_ASSERT(it == replicas.end(), "replica ID already exists");
                    replicas.emplace_back(intro.replica_id);

                    // add the send queue to the state
                    storage_queues_[intro.shard_id].emplace_back(std::move(send_queue));
                }
            }

            break;
            case zip::api::SUBSCRIBER_LOG_ENTRY:
            {
                // handle an insert after ack
                auto& entry = buffer->as<zip::api::subscriber_log_entry>();
                ZIP_ASSERT(length == entry.length(), "length of the received packet is invalid");
                logger.trace("Received entry with GSN ", entry.gsn, " from client (", entry.client_id, ") from storage server (", entry.shard_id, ":", entry.replica_id, ")");

                // stop processing if this entry has already been delivered
                auto& latest_gsn = latest_gsn_[entry.shard_id];
                if (latest_gsn != -1 && entry.gsn <= latest_gsn) {
                    recv_queue_.arm(*buffer);
                    break;
                }

                // search through current requests to see if this entry is in-flight
                auto it = current_requests_.begin();
                for (; it != current_requests_.end(); it++) {
                    if ((*it)->gsn >= entry.gsn) {
                        break;
                    }
                }

                // if this is a new entry then it must be initialised
                // otherwise, we need to verify the received parameters
                if (it == current_requests_.end() || (*it)->gsn != entry.gsn) {
                    // check to see if we have a free entry otherwise
                    // allocate new memory
                    if (free_requests_.empty()) {
                        auto allocated = zip::util::malloc_unique<zip::api::subscriber_log_entry>(zip::consts::BUFFER_SIZES.back());
                        it = current_requests_.insert(it, std::move(allocated));
                    } else {
                        it = current_requests_.insert(it, std::move(free_requests_.front()));
                        free_requests_.pop_front();
                    }

                    // copy the data and set the number of acks that must be received
                    std::memcpy((*it).get(), &entry, entry.length());
                    (*it)->num_acks = zip::util::more_than_half(storage_replicas_[entry.shard_id].size());
                } else {
                    // verify the entry parameters and free the buffer for reuse
                    ZIP_ASSERT(entry.client_id   == (*it)->client_id
                            && entry.shard_id    == (*it)->shard_id
#if ZIP_CLIENT_SEQ
                            && entry.client_seq  == (*it)->client_seq
#endif
#if ZIP_SHARD_SEQ
                            && entry.shard_seq   == (*it)->shard_seq
#endif
                            && entry.data_length == (*it)->data_length, "failed to verify entry");
                }

                // decrement the counter and check if this is the last
                // entry that needed to be delivered
                if (--((*it)->num_acks) == 0) {
                    // update the lastest GSN from this shard
                    latest_gsn = (*it)->gsn;

                    // if we are not sequential then we deliver
                    // this entry right away
                    if (!ordered_) {
                        // execute the callback
                        if ((*it)->data_length > 0) {
                            callback(**it);
                            executed++;
                        }

                        // move the entry to the free list
                        free_requests_.emplace_back(std::move(*it));
                        current_requests_.erase(it);
                    }
                }
            }

            break;
            default:
                logger.warn("Received an unknown type of message (", req_type, ")");
        }

        // request can be freed
        recv_queue_.arm(*buffer);
    }

    // return if found
    return executed;
}

} // namespace subscriber
} // namespace zip
