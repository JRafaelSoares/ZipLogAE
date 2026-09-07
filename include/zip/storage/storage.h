#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <zip/api/api.h>
#include <zip/storage/log.h>
#include <zip/util/consts.h>
#include <zip/network/erpc_transport.h>

/** Forward declaration for network manager and server. */
namespace zip::network { class manager; class server; }

namespace zip::storage {

/**
 * This struct stores the state of a client.
 */
struct client_state {

    /** Initialize the client state. */
    client_state(log& log, client_t client_id): pos(log, client_id) {}

    /** network send queue */
    std::unique_ptr<zip::network::send_endpoint> send_endpoint;

    /** position in the log */
    position pos;

    /** whether the client has connected */
    std::atomic<bool> connected = false;

    /** lock to protect the disconnection/finalization state */
    std::mutex lock;

    /** whether the client has disconnected */
    bool disconnected = false;

    /** whether the client has finalized */
    bool finalized = false;

};

/**
 * Represents a single storage server.
 */
class storage {

public:

    /**
     * Initialize a storage server.
     *
     * @param manager         network manager
     * @param transport          network server
     * @param order           address of the ordering server
     * @param shard_id        ID of the shard for the storage server
     * @param replica_id      ID of the replica in the shard
     * @param client_cpus     CPUs to run the client threads on
     * @param subscriber_cpus CPUs to run the subscriber threads on
     * @param timeout         timeout for flushing the batch to subscribers
     */
    storage(
        zip::network::manager& manager,
        network::erpc_transport_factory& transport,
        std::string order,
        shard_t shard_id,
        replica_t replica_id,
        std::set<uint16_t> clients_cpus,
        std::set<uint16_t> subscriber_cpus,
        std::chrono::nanoseconds timeout,
        std::string addr,
        uint16_t port
    );

    /**
     * Deinitialize a storage server.
     */
    ~storage();

private:

    /**
     * This method is for performing control and ordering
     * server related requests.
     */
    void control();

    /**
     * This method is for performing client-related requests.
     *
     * @param index the index of this client thread
     */
    void client(uint32_t index);

    /**
     * This method is for performing subscriber-related requests.
     *
     * @param index the index of this subscriber thread
     */
    void subscriber(uint32_t index, uint32_t erpc_index);

    /** network manager */
    zip::network::manager& manager_;

    /**
     * ID of the shard for this storage server
     *
     * NOTE: shard ID can be a unique integer
     *       in the range `[0, zip::consts::MAX_SHARDS - 1]`
     */
    shard_t shard_id_;

    /**
     * ID of the replica in the shard for this storage server
     *
     * NOTE: replica ID can be a unique integer
     *       in the range `[0, zip::consts::MAX_REPLICAS - 1]`
     */
    replica_t replica_id_;

    /** signal to stop the processing thread */
    std::atomic<bool> stop_ = false;

    /** signal to start the processing thread */
    std::atomic<bool> start_ = false;

    /** log for this storage server */
    log log_;

    /** map from client IDs to their state */
    std::array<client_state, zip::consts::MAX_CLIENTS> client_state_;

    /** network send queue to the ordering server */
    std::unique_ptr<zip::network::send_endpoint> order_;

    /** number of client threads */
    uint32_t client_threads_;

    /** number of subscriber threads */
    uint32_t subscriber_threads_;

    /** timeout for flushing the batch of subscriber entries */
    std::chrono::nanoseconds timeout_;

    /** lock to protect the subscribers set */
    std::mutex subscriber_lock_;

    /** set of the connected subscribers */
    std::unordered_set<subscriber_t> subscribers_;

    /** vector of locks to protect subscriber state for the threads */
    std::unique_ptr<std::mutex[]> subscriber_locks_;

    /** vector of map from subscriber ID to their send queue */
    std::unique_ptr<std::vector<std::pair<subscriber_t, std::unique_ptr<zip::network::send_endpoint>>>[]> subscriber_queues_;

    /** receive queues for the threads */
    std::vector<std::unique_ptr<zip::network::recv_endpoint>> recv_endpoints_;

    /** vector of all threads */
    std::vector<std::thread> threads_;

    /** finished message for the storage server */
    zip::api::storage_finished finished_;

    /** intro message for the storage server */
    zip::api::storage_intro intro_;

    /** active transport backend used to create send endpoints */
    zip::network::erpc_transport_factory* transport_ = nullptr;

    /** control-plane receive endpoint for this storage replica */
    std::unique_ptr<zip::network::recv_endpoint> control_recv_endpoint_;

    /*
    static thread_local int fd_;
    static thread_local int current_file_;
    static thread_local int appended_entries_batch_;
    */
};

} // namespace zip::storage
