#include <array>
#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <shared_mutex>
#include <thread>
#include <utility>
#include <vector>

#include "app/app.h"
#include "network/send_queue.h"
#include "storage/log.h"
#include "util/consts.h"

namespace zip {

/** Forward declarations for messages. */
namespace api { struct storage_intro; struct subscriber_log_entry; }

/** Forward declaration for network buffer, manager, and receive queue. */
namespace network { class buffer; class manager; class recv_queue; }

namespace storage {

/**
 * Represents a single replica in a storage
 * shard.
 */
class storage {

public:

    /**
     * Initialise a storage server.
     *
     * @param manager         network manager
     * @param memory          memory manager
     * @param client_cpus     CPUs to use for processing client requests
     * @param subscriber_cpus CPUs to use for processing subscriber requests
     * @param order           address of the ordering server
     * @param intro           intro message for this storage server
     * @param shard_id        ID of the shard
     * @param replica_id      ID of the replica within a shard
     * @param app             co-located application
     */
    storage(
        zip::network::manager& manager,
        std::set<unsigned int> client_cpus,
        std::set<unsigned int> subscriber_cpus,
#ifdef COLOCATED_ZIPKAT
        std::set<unsigned int> get_cpus,
#endif
        std::string order,
        zip::api::storage_intro& intro,
        uint64_t shard_id,
        uint64_t replica_id,
        std::unique_ptr<zip::app::Application> app
    );

    /**
     * Deinitialise a server.
     */
    ~storage();

    /**
     * Add the given client.
     *
     * @param client_id  ID of the client
     * @param send_queue network send queue of the client
     */
    void add_client(uint64_t client_id, zip::network::send_queue send_queues);
    //void add_client(uint64_t client_id, std::vector<zip::network::send_queue> send_queues);

    /**
     * Add the given subscriber.
     *
     * @param subscriber_id ID of the subscriber
     * @param start_gsn     starting GSN with which to stream
     * @param send_queue    network send queue of the subscriber
     */
    void add_subscriber(uint64_t subscriber_id, uint64_t start_gsn, zip::network::send_queue send_queue);
    // HACK: for colocated app only
    void add_subscriber(uint64_t subscriber_id, uint64_t start_gsn, zip::network::send_queue* send_queue);

private:

    /**
     * This struct stores the state of a client.
     */
    struct client_state {

        /** Initialise the client state. */
        client_state(uint64_t client_id_, log& log): client_id(client_id_), position(client_id_, log) {}

        /** ID of the client */
        uint64_t client_id;

        /** whether the client has connected */
        std::atomic<bool> connected = false;

        /** whether the client has disconnected */
        bool disconnected = false;

        /** whether the client has been finalised */
        bool finalised = false;

        /** network send queue */
        zip::network::send_queue send_queue;

#ifdef COLOCATED_ZIPKAT
        zip::network::send_queue get_send_queue;
        zip::network::send_queue dumb_queue;
        zip::network::send_queue dumb_queue2;
        zip::network::send_queue dumb_queue3;
#endif

        /** position in the log */
        position position;

    };

    /**
     * This struct stores the state of a subscriber.
     */
    struct subscriber_state {

        /** Initialise the subscriber state. */
        subscriber_state(uint64_t subscriber_id, uint64_t start_gsn, zip::network::send_queue send_queue):
            subscriber_id(subscriber_id), start_gsn(start_gsn), send_queue(std::move(send_queue)) {}

        /** Initialise the subscriber state. */
        // TODO: deduplicate
        subscriber_state(uint64_t subscriber_id, uint64_t start_gsn, zip::network::send_queue* send_queue):
            subscriber_id(subscriber_id), start_gsn(start_gsn), colocated_send_queue(send_queue) {}

        /** ID of the subscriber */
        uint64_t subscriber_id;

        /** starting GSN from which to stream entries */
        uint64_t start_gsn;

        /** network send queue */
        zip::network::send_queue send_queue;

        // HACK: for colocated app. zipkat for example, the send queue will be
        // the client_states_'s send queue.
        // TODO: it might be good to make it shard_ptr
        zip::network::send_queue* colocated_send_queue = nullptr;
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
     * This method is for performing subscribers-related
     * requests like `subscriber_finished` and posting
     * messages to them.
     *
     * @param recv_queue the receive queue for this thread
     * @param subscriber the index of this thread in the state arrays
     */
#if defined(SUBSCRIBER_THREAD_HANDLE_GET) || !defined(COLOCATED_ZIPKAT)
    void subscriber(zip::network::recv_queue recv_queue, unsigned long subscriber);
#else
    void subscriber(unsigned long subscriber);
#endif

#ifdef GET_THREAD_HANDLE_GET
    void get_thread(zip::network::recv_queue recv_queue, unsigned int get_thread_id);
#endif

    /**
     * Copy the given log entry into the given buffer.
     *
     * @param buffer the given buffer
     * @param entry the given entry
     *
     * @return the message to be sent
     */
    zip::api::subscriber_log_entry& copy_entry(zip::network::buffer& buffer, log::entry* entry);

    // TODO: fill param
    /**
     * Send entry to the subscriber.
     */
    void send_entry(subscriber_state& subscriber, log::entry& entry,
                    bool synchronize = false, unsigned int destination = 0);

    uint64_t subscriber_id_to_index(uint64_t id);

private:
    /** network manager */
    zip::network::manager& manager_;

    /**
     * ID of the shard
     *
     * NOTE: can be anything in the range
     *       `[0, zip::consts::NUM_SHARDS - 1]`
     *
     * */
    const uint64_t shard_id_;

    /** ID of the replica within a shard */
    uint64_t replica_id_;

    /** signal to stop the processing thread */
    std::atomic<bool> stop_ = false;

    /** log for this storage server replica */
    log log_;

    /** map from client IDs to their state */
    std::array<std::atomic<client_state*>, zip::consts::MAX_CLIENTS> client_state_{};

    /** network send queue to the ordering service */
    zip::network::send_queue order_;

    /** vector of processing threads */
    std::vector<std::thread> threads_;

    /** number of client threads */
    const unsigned int client_threads_;

    /** number of subscriber threads */
    const unsigned int subscriber_threads_;

    /** lock for protecting subscribers set */
    std::mutex subscriber_lock_;

#ifdef COLOCATED_ZIPKAT
    /** number of get threads */
    const unsigned int get_threads_;

    /** finishing client that needs to be removed from subscriber by the subscriber thread */
    std::atomic<int64_t> finishing_client_;
#endif

    /** vector of list of subscribers for each thread */
    std::unique_ptr<std::list<subscriber_state>[]> subscribers_;

    /** vector of subscriber queue size for each thread */
    std::unique_ptr<std::atomic<unsigned long>[]> num_subscribers_;

    /** colocated application */
    std::unique_ptr<app::Application> app_;

    /** finished message for the clients and subscribers */
    zip::api::storage_finished finished_;

};

} // namespace storage
} // namespace zip
