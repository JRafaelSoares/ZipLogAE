#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <zip/api/api.h>
#include <zip/util/concurrent.h>

#include "zip/network/erpc_transport.h"
#include "zip/network/transport.h"

/** Forward declaration for manager and buffer. */
namespace zip::network { class manager; class buffer; }

namespace zip::client {

/**
 * This class represents a Ziplog client.
 */
class client {

public:

    /**
     * Initialize a Ziplog client.
     *
     * @param manager      network manager
     * @param order        address of the ordering server
     * @param client_id    ID of the client
     * @param shard_id     ID of the shard for this client
     * @param failures     number of failures to tolerate
     * @param min_fraction minimum fraction of used slots
     * @param max_fraction maximum fraction of used slots
     * @param cpu_id       CPU to run the process on
     * @param servers      storage servers to connect to
     */
    client(
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
    );

    /**
     * Stop the client.
     */
    void stop();

    /**
     * Perform an append request asynchronously.
     *
     * @param buffer   buffer with the request
     * @param response location to store the response
     */
    void append(zip::network::buffer& buffer, std::atomic<gsn_t>& response);

    /**
     * Perform an append request synchronously.
     *
     * @param buffer   buffer with the request
     *
     * @return GS assigned to the request
     */
    gsn_t append(zip::network::buffer& buffer);

private:

    /**
     * Add the given storage server to the client.
     *
     * @param address the address of the server
     */
    void add_server(std::string address);

    /**
     * This method processes this client's requests in a loop.
     */
    void loop(std::string order, std::set<std::string> servers);

    /** this client's network receive queue */
    std::unique_ptr<zip::network::recv_endpoint> recv_endpoint_;

    /**
     * ID of the client
     *
     * NOTE: client ID can be a unique integer
     *       in the range `[0, zip::consts::MAX_CLIENTS - 1]`
     */
    client_t client_id_;

    /**
     * ID of the shard for this client
     *
     * NOTE: shard ID can be a unique integer
     *       in the range `[0, zip::consts::MAX_SHARDS - 1]`
     */
    shard_t shard_id_;

    /** number of failures to tolerate */
    uint32_t failures_;

    /** network manager */
    zip::network::manager& manager_;

    /** network send queue to the ordering server */
    std::unique_ptr<zip::network::send_endpoint> order_;

    /** thread for processing requests */
    std::thread thread_;

    /** signal to stop the processing thread */
    std::atomic<bool> stop_ = false;

    /** signal to start the processing thread */
    std::atomic<bool> start_ = false;

    /** map from replica ID to the storage server's send queues */
    std::vector<std::pair<replica_t, std::unique_ptr<zip::network::send_endpoint>>> server_queue_;

    /** queue for the ongoing requests */
    zip::util::spsc_queue<std::pair<zip::network::buffer*, std::atomic<gsn_t>*>> requests_;

    /** fraction for which to trigger rate estimation */
    double min_fraction_, max_fraction_;

    /** intro message for the client */
    zip::api::client_intro intro_;

    /** active transport backend used to create send endpoints */
    zip::network::erpc_transport_factory* transport_ = nullptr;

    /** control-plane receive endpoint for this storage replica */
    std::unique_ptr<zip::network::recv_endpoint> control_recv_endpoint_;
};

} // namespace zip::client
