#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <netinet/in.h>

#include "network/buffer.h"
#include "network/recv_queue.h"
#include "network/send_queue.h"
#include "util/consts.h"

namespace zip {

/** Forward declaration for manager. */
namespace network { class manager; }

namespace order {

/**
 * This class represents the ordering
 * service.
 */
class order {

public:

    /**
     * Initialise a order server.
     *
     * @param manager the network manager
     * @param cpu_id  the CPU to run the process on
     */
    order(
        zip::network::manager& manager,
        unsigned int cpu_id
    );

    /**
     * Deinit an order server.
     */
    ~order();

    /**
     * Add the given storage server.
     *
     * @param shard_id          shard ID of the server
     * @param replica_id        replica ID of the server
     * @param client_queues     the number of client queues for this server
     * @param subscriber_queues the number of subsciber queues for this server
     * @param address           IP address of the server
     * @param send_queue        network send queue of the server
     */
    void add_storage(
        uint64_t shard_id,
        uint64_t replica_id,
        unsigned int client_queues,
        unsigned int subscriber_queues,
        sockaddr_in address,
        zip::network::send_queue send_queue
    );

    /**
     * Add the given client.
     *
     * @param client_id  ID of the client
     * @param shard_id   ID of the shard for this client
     * @param num_slots  number of slots per epoch
     * @param send_queue network send queue of the client
     */
    void add_client(uint64_t client_id, uint64_t shard_id, uint64_t num_slots, zip::network::send_queue send_queue);

    /**
     * Add the given subscriber.
     *
     * @param subscriber_id ID of the subscriber
     * @param send_queue    network send queue of the subscriber
     */
    void add_subscriber(uint64_t subscriber_id, zip::network::send_queue send_queue);

private:

    /**
     * This method processes ordering requests in a loop.
     */
    void loop();

    /** lock for accessing the state */
    std::mutex lock_;

    /** signal to stop the processing thread */
    std::atomic<bool> stop_ = false;

    /** thread to run the processing loop */
    std::thread thread_;

    /** start GSN of the next epoch */
    uint64_t gsn_base_ = 0;

    /** map from shard IDs to replicas' IP address */
    std::array<std::unordered_map<uint64_t, sockaddr_in>, zip::consts::NUM_SHARDS> storage_replicas_;

    /** map from shard IDs to number of client queues */
    std::array<unsigned int, zip::consts::NUM_SHARDS> storage_client_queues_{};

    /** map from shard IDs to number of subscriber queues */
    std::array<unsigned int, zip::consts::NUM_SHARDS> storage_subscriber_queues_{};

    /** map from shard IDs to replicas's send queue */
    std::array<std::unordered_map<uint64_t, zip::network::send_queue>, zip::consts::NUM_SHARDS> storage_queues_;

    /** map from shard IDs to the clients connected to them */
    std::array<std::unordered_set<uint64_t>, zip::consts::NUM_SHARDS> storage_clients_;

#if ZIP_SHARD_SEQ
    /** map from shard IDs to the current shard sequence number */
    std::array<uint64_t, zip::consts::NUM_SHARDS> shard_seq_{};
#endif

#if ZIP_CLIENT_SEQ
    /** map from client IDs to the current client sequence number */
    std::unordered_map<uint64_t, uint64_t> client_seq_;
#endif

    /** map from client IDs to their shard */
    std::unordered_map<uint64_t, uint64_t> client_shard_;

    /** map from client IDs to their network send queue */
    std::unordered_map<uint64_t, zip::network::send_queue> client_queue_;

    /** map from client IDs to their requested slots */
    std::unordered_map<uint64_t, uint64_t> client_request_;

    /** map from subscriber IDs to their network send queue */
    std::unordered_map<uint64_t, zip::network::send_queue> subscriber_queue_;

    /** network manager */
    zip::network::manager& manager_;

    /** ordering server's receive queue */
    zip::network::recv_queue recv_queue_;

    /** buffer to use to send clients and subscribers storage info */
    std::list<zip::network::buffer> info_buffer_;
};

} // namespace order
} // namespace zip
