#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <zip/api/api.h>
#include <zip/util/consts.h>
#include <zip/network/transport.h>
#include <zip/network/erpc_transport.h>
/** Forward declaration for manager and server. */
namespace zip::network { class manager; class server; }

namespace zip::order {

/**
 * This class represents the ordering
 * server.
 */
class order {

public:

    /**
     * Initialize an ordering server.
     *
     * @param manager network manager
     * @param transport  network server
     * @param cpu_id  CPU to run the processing thread
     */
    order(zip::network::manager& manager, network::erpc_transport_factory& transport, uint16_t cpu_id);

    /**
     * Deinitialize an ordering server.
     */
    ~order();

private:

    /**
     * This struct stores the state for the client.
     */
    struct client_state {

        /** Initialise client state. */
        client_state(shard_t shard_id, std::unique_ptr<zip::network::send_endpoint> endpoint):
        shard_id(shard_id), endpoint(std::move(endpoint)) {}

        /** ID of the shard for this client */
        shard_t shard_id;

        /** number of slots to assign per epoch */
        uint32_t num_slots = zip::consts::MIN_SLOTS;

        /** network send queue of the client */
        std::unique_ptr<zip::network::send_endpoint> endpoint;


    };

    /**
     * This method processes ordering requests in a loop.
     */
    void loop();

    /** lock for accessing the state */
    std::mutex lock_;

    /** signal to stop the processing thread */
    std::atomic<bool> stop_ = false;

    /** signal to start the processing thread */
    std::atomic<bool> start_ = false;

    /** vector of all processing threads */
    std::vector<std::thread> threads_;

    /** start GSN of the next epoch */
    gsn_t gsn_base_ = 0;

    /** map from shard ID and replica IDs to their send queue */
    std::unordered_map<shard_t, std::unordered_map<replica_t, std::unique_ptr<zip::network::send_endpoint>>> server_queues_;

    /** map from client IDs to their state */
    std::unordered_map<client_t, client_state> client_state_;

    /** network manager */
    zip::network::manager& manager_;

    /** this ordering server's receive queue */
    std::unique_ptr<zip::network::recv_endpoint> recv_queue_;

    /** intro message for the ordering server */
    zip::api::order_intro intro_;

    /** active transport backend used to create send endpoints */
    zip::network::erpc_transport_factory* transport_ = nullptr;
};

} // namespace zip::order
