#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <zip/api/api.h>
#include <zip/network/buffer.h>
#include <zip/subscriber/log.h>
#include <zip/util/consts.h>
#include <zip/util/util.h>

#include "zip/network/erpc_transport.h"

/** Forward declaration for network manager and server. */
namespace zip::network { class manager; class server; }

namespace zip::subscriber {

/**
 * Represents a single subscriber.
 */
class subscriber {

public:

    /**
     * Initialize a subscriber.
     *
     * @param manager       network manager
     * @param subscriber_id ID of the subscriber
     * @param failures      number of failures to tolerate
     * @param cpus          CPUs to run the subscriber threads on
     * @param num_iterators number of iterators to create
     * @param servers       servers to connect to
     */
    subscriber(
        zip::network::manager& manager,
        zip::network::erpc_transport_factory& transport,

        subscriber_t subscriber_id,
        uint32_t failures,
        std::set<uint16_t> cpus,
        uint32_t num_iterators,
        std::set<std::string> servers,
        std::string addr,
        uint16_t port
    );

    /**
     * Deinitialize a subscriber.
     */
    ~subscriber();

    /**
     * Return the iterators.
     *
     * @return the iterators for the log
     */
    std::vector<iterator>& iterators() { return iterators_; }

private:

    /**
     * Add the given storage server to the subscriber.
     *
     * @param address the address of the server
     */
    void add_server(std::string address);

    /**
     * This method is for performing subscriber-related requests.
     *
     * @param index the index of this subscriber thread
     */
    void loop(uint32_t index, std::set<std::string> servers);

    /** network manager */
    zip::network::manager& manager_;

    /** ID of the subscriber */
    subscriber_t subscriber_id_;

    /** number of failures to tolerate */
    uint32_t failures_;

    /** signal to start the processing thread */
    std::atomic<bool> start_ = false;

    /** log for this subscriber */
    log log_;

    /** vector of iterators for the log */
    std::vector<iterator> iterators_;

    /** map from shard and replica IDs to their send queues for each thread */
    std::unordered_map<shard_t, std::unordered_map<replica_t, std::vector<std::unique_ptr<zip::network::send_endpoint>>>>
    server_queues_;

    /** number of subscriber threads */
    uint32_t num_threads_;

    /** receive queues for the threads */
    std::vector<std::unique_ptr<zip::network::recv_endpoint>> recv_queues_;

    /** vector of all threads */
    std::vector<std::thread> threads_;

    /** finished message for the subscriber */
    zip::api::subscriber_finished finished_;

    /** intro message for the subscriber */
    zip::api::subscriber_intro intro_;

    /** active transport backend used to create send endpoints */
    zip::network::erpc_transport_factory* transport_ = nullptr;

    /** control-plane receive endpoint for this storage replica */
    std::unique_ptr<zip::network::recv_endpoint> control_recv_endpoint_;

};

} // namespace zip::subscriber
