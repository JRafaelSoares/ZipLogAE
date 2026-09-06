#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "network/buffer.h"
#include "network/recv_queue.h"
#include "network/send_queue.h"
#include "util/consts.h"
#include "util/util.h"

/** Forward declaration for socket structs. */
struct sockaddr_in;

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
     * Initialize an order server.
     *
     * @param manager the network manager
     * @param cpu_id  the CPU to run the process on
     */
    order(zip::network::manager& manager, uint16_t cpu_id);

    /**
     * Deinitialize an order server.
     */
    ~order();

    /**
     * Add the given storage server.
     *
     * @param shard_id          shard ID of the server
     * @param replica_id        replica ID of the server
     * @param address           IP address of the server
     * @param send_queue        network send queue of the server
     */
    void add_storage(
        uint64_t shard_id,
        uint64_t replica_id,
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
    void add_client(uint64_t client_id, uint64_t shard_id, unsigned long num_slots, zip::network::send_queue send_queue);

    /**
     * Add the given subscriber.
     *
     * @param subscriber_id ID of the subscriber
     * @param send_queue    network send queue of the subscriber
     */
    void add_subscriber(uint64_t subscriber_id, zip::network::send_queue send_queue);

private:

    /**
     * This struct stores the state for client.
     */
    struct client_state {

        /** Initialise client state. */
        client_state(uint64_t shard_id, unsigned long num_slots, zip::network::send_queue send_queue): shard_id(shard_id), num_slots(num_slots), send_queue(std::move(send_queue)) {}

        /** ID of the shard for the client */
        uint64_t shard_id;

        /** number of slots to assign per epoch */
        unsigned long num_slots;

        /** the network send queue of the client */
        zip::network::send_queue send_queue;

        /** whether the client is marked as failed */
        bool failed = false;

        /** whether client data is requested */
        bool requested = false;

        /** client cuts sent by the storage servers */
        std::map<uint64_t, uint64_t> cuts;

        /** set of storage servers that have sent their data */
        std::unordered_set<uint64_t> flushed;

        /** data received from the storage servers */
        std::map<uint64_t, zip::network::buffer> data;

    };

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
    std::array<std::unordered_map<uint64_t, sockaddr_in>, zip::consts::NUM_SHARDS> storage_addresses_;

    /** map from shard IDs to replicas' send queue */
    std::array<std::unordered_map<uint64_t, zip::network::send_queue>, zip::consts::NUM_SHARDS> storage_queues_;

    /** map from client IDs to their state */
    std::unordered_map<uint64_t, client_state> client_state_;

    /** map from subscriber IDs to their network send queue */
    std::unordered_map<uint64_t, zip::network::send_queue> subscriber_queue_;

    /** network manager */
    zip::network::manager& manager_;

    /** ordering server's receive queue */
    zip::network::recv_queue recv_queue_;

    /** buffer to use to send clients and subscribers storage info */
    std::vector<zip::network::buffer> buffers_;

    /** iterator to the network buffers */
    zip::util::wraparound_iterator<decltype(buffers_)> iterator_;

};

} // namespace order

} // namespace zip
