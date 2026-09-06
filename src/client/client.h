#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "api/api.h"
#include "network/recv_queue.h"
#include "network/send_queue.h"
#include "util/concurrent.h"

namespace zip {

/** Forward declaration for manager and buffer. */
namespace network { class manager; class buffer; }

namespace client {

/**
 * Contains the constants used for PID rate estimation.
 */
struct pid {

    /** target fraction of used slots. */
    double target;

    /** proportional constant */
    double proportional;

    /** integral constant */
    double integral;

    /** derivative constant */
    double detivative;

};

/**
 * This class represents a Ziplog client.
 */
class client {

public:

    /**
     * Initialize a client with PID rate estimation.
     *
     * @param manager    the network manager
     * @param order      the address of the ordering server
     * @param client_id  the ID of the client
     * @param shard_id   the ID of the shard
     * @param cpu_id     the CPU to run the process on
     * @param pid        the parameters for PID rate estimation
     * @param failures   the number of failures to tolerate
     */
    client(
        zip::network::manager& manager,
        std::string order,
        uint64_t client_id,
        uint64_t shard_id,
        uint16_t cpu_id,
        pid pid,
        unsigned long failures
    );

    /**
     * Initialize a client with fixed rate.
     *
     * @param manager    the network manager
     * @param order      the address of the ordering server
     * @param client_id  the ID of the client
     * @param shard_id   the ID of the shard
     * @param cpu_id     the CPU to run the process on
     * @param rate       the rate to request from the ordering service
     * @param failures   the number of failures to tolerate
     */
    client(
        zip::network::manager& manager,
        std::string order,
        uint64_t client_id,
        uint64_t shard_id,
        uint16_t cpu_id,
        unsigned long rate,
        unsigned long failures
    );

    /**
     * Stop the client.
     */
    void stop(bool trigger_failure = false);

    /**
     * Perform an insert request on the given client.
     *
     * @param request  the buffer with the request
     * @param response the location of the response
     */
    void insert(zip::network::buffer& request, std::atomic<uint64_t>& response);

private:

    /** Initialise the order connection and client processing thread. */
    void initialize_client(std::string order, uint16_t cpu_id);

    /**
     * This method processes this client's requests in a loop.
     */
    void loop();

    /** current client's network receive queue */
    zip::network::recv_queue recv_queue_;

    /**
     * ID of the client
     *
     * NOTE: client ID can be a unique ID
     *       in the range `[0, zip::consts::NUM_CLIENTS - 1]`
     */
    uint64_t client_id_;

    /** ID of the shard this client is connected to */
    uint64_t shard_id_;

    /** number of failures to tolerate */
    unsigned long failures_;

    /** intro message for this client */
    zip::api::client_intro intro_;

    /** network manager */
    zip::network::manager& manager_;

    /** network send queue to the ordering service */
    zip::network::send_queue order_;

    /** client thread for processing requests */
    std::thread thread_;

    /** signal to stop the processing thread */
    std::atomic<bool> stop_ = false;
    /** signal to trigger a failure */
    std::atomic<bool> force_stop_ = false;

    /** whether to perform PID rate estimation */
    bool estimate_;

    /** parameters for PID rate estimation */
    pid pid_;

    /** ensure that only one thread is executing `client::insert` */
    std::mutex lock_;

    /** queue for the ongoing requests */
    zip::util::queue<std::pair<zip::network::buffer*, std::atomic<uint64_t>*>> requests_;

};

} // namespace client

} // namespace zip
