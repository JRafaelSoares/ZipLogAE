#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "api/api.h"
#include "network/send_queue.h"
#include "storage/log.h"
#include "util/concurrent.h"
#include "util/consts.h"

namespace zip {

/** Forward declaration for network manager, and receive queue. */
namespace network { class manager; class recv_queue; }

namespace storage {

/**
 * Represents a single storage server.
 */
class storage {

public:

    /**
     * Initialize a storage server.
     *
     * @param manager           network manager
     * @param memory            memory manager
     * @param client_cpus       CPUs to use for processing client requests
     * @param subscriber_cpus   CPUs to use for processing subscriber requests
     * @param subscriber_depth  number of outstanding entries being sent to subscribers
     * @param order             address of the ordering server
     * @param intro             intro message for this storage server
     * @param shard_id          ID of the shard
     * @param replica_id        ID of the replica within a shard
     */
    storage(
        zip::network::manager& manager,
        std::set<uint16_t> client_cpus,
        std::set<uint16_t> subscriber_cpus,
        unsigned long subscriber_depth,
        std::string order,
        zip::api::storage_intro& intro,
        uint64_t shard_id,
        uint64_t replica_id
    );

    /**
     * Deinitialize a server.
     */
    ~storage();

    /**
     * Add the given client.
     *
     * @param client_id  ID of the client
     * @param send_queue network send queue of the client
     */
    void add_client(uint64_t client_id, zip::network::send_queue send_queue);

    /**
     * Add the given subscriber.
     *
     * @param subscriber_id ID of the subscriber
     * @param send_queues   network send queues to the subscriber
     * @param num_queues    number of receive queues at this subscriber
     */
    void add_subscriber(uint64_t subscriber_id, unsigned long num_queues, std::vector<zip::network::send_queue> send_queues);

private:

    /**
     * This struct stores the state of a client.
     */
    struct client_state {

        /** Initialize the client state. */
        client_state(uint64_t client_id, log& log): position(client_id, log) {}

        /** lock to protect the client state */
        std::mutex lock;

        /** condition variable to wait on initialization */
        std::condition_variable cv;

        /** whether the client has connected */
        bool connected = false;

        /** whether the client has been disconnected */
        bool disconnected = false;

        /** network send queue */
        zip::network::send_queue send_queue;

        /** position in the log */
        position position;

        /** latest GSN assigned to a request for this client */
        uint64_t latest_gsn = std::numeric_limits<uint64_t>::max();

    };

    /**
     * This struct stores the state of a subscriber.
     */
    struct subscriber_state {

        /** Initialize the subscriber state. */
        subscriber_state(uint64_t subscriber_id, unsigned long num_queues, std::vector<zip::network::send_queue> send_queues):
        subscriber_id(subscriber_id), num_queues(num_queues), send_queues(std::move(send_queues)) {}

        /** ID of the subscriber */
        uint64_t subscriber_id;

        /** number of receive queues at the subscriber */
        unsigned long num_queues;

        /** network send queues */
        std::vector<zip::network::send_queue> send_queues;

    };

    /**
     * Return the state of the given client.
     *
     * @param client_id ID of the client
     *
     * @return reference to the state
     */
    client_state& get_client_state(uint64_t client_id);

    /**
     * This method is for performing control and ordering
     * service related requests like `order_gsn`.
     *
     * @param recv_queue the receive queue for this thread
     */
    void control(zip::network::recv_queue recv_queue);

    /**
     * This method is for performing client-related requests
     * like `insert_after`.
     *
     * @param recv_queue the receive queue for this thread
     */
    void client(zip::network::recv_queue recv_queue);

    /**
     * This method is for posting log entries to subscribers.
     *
     * @param index the index of this thread in the subscriber arrays
     */
    void subscriber(unsigned long index);

    /** network manager */
    zip::network::manager& manager_;

    /**
     * ID of the shard
     *
     * NOTE: can be anything in the range
     *       `[0, zip::consts::NUM_SHARDS - 1]`
     *
     */
    uint64_t shard_id_;

    /** ID of the replica within a shard */
    uint64_t replica_id_;

    /** signal to stop the processing thread */
    std::atomic<bool> stop_ = false;

    /** log for this storage server replica */
    log log_;

    /** map from client IDs to their state */
    std::array<std::atomic<client_state*>, zip::consts::MAX_CLIENTS> client_state_ {};

    /** network send queue to the ordering service */
    zip::network::send_queue order_;

    /** vector of processing threads */
    std::vector<std::thread> threads_;

    /** number of client threads */
    unsigned long client_threads_;

    /** number of subscriber threads */
    unsigned long subscriber_threads_;

    /** maximum number of outstanding subscriber entries */
    unsigned long subscriber_depth_;

    /** lock for accessing subscriber state */
    std::mutex subscriber_lock_;

    /** set of all the subscriber IDs */
    std::unordered_set<uint64_t> subscribers_;

    /** list of subscribers added or removed */
    using change_t = std::variant<subscriber_state, uint64_t>;
    zip::util::bag<change_t> subscriber_changes_;

    /** finished message for the clients and subscribers */
    zip::api::storage_finished finished_;

};

} // namespace storage

} // namespace zip
