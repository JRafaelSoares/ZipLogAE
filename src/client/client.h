#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <list>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "api/api.h"
#include "network/buffer.h"
#include "network/recv_queue.h"
#include "network/send_queue.h"
#include "util/consts.h"

//#define ZIPKAT_SEPARATE_THREAD 1

namespace zip {

/** Forward declaration for manager. */
namespace network { class manager; }

namespace client {

/**
 * This class represents a Ziplog client.
 */
class client {

public:

    /**
     * This struct contains the data for a single
     * request.
     */
    struct request {

        /** start time of the request */
        std::chrono::high_resolution_clock::time_point begin;

        /** buffer with the request to be sent */
        zip::network::buffer* buffer;

        /** location of the response */
        std::atomic<uint64_t> response;

#ifdef COLOCATED_ZIPKAT
        /** application return */
        uint64_t application_return;
#endif
    };

    /**
     * Initialise a client.
     *
     * @param manager    the network manager
     * @param order      the address of the ordering server
     * @param client_id  the ID of the client
     * @param shard_id   the ID of the shard
     * @param cpu_id     the CPU to run the process on
     * @param rate       the rate at which to limit sending requests
     */
    client(
        zip::network::manager& manager,
        std::string order,
        uint64_t client_id,
        uint64_t shard_id,
        unsigned int cpu_id,
        unsigned long rate
    );

    /**
     * Deinitialise a client.
     */
    ~client();

    /**
     * Perform an insert after request on the given client.
     *
     * @request the request to perform
     */
    void insert_after(request& request);

    uint64_t client_id() { return client_id_; }

#ifdef COLOCATED_ZIPKAT
    /**
     * This struct contains the data for a zipkat get request.
     */
    struct zipkat_get_request {

        /** buffer with the request to be sent */
        std::string key;

        /** timestamp when the key-value was stored */
        std::atomic<uint64_t> timestamp;

        /** returned vallue */
        std::string value;
    };

    // TODO: make this more general
    /**
     * Perform a zipkat GET request
     *
     * @request the request to perform
     */
    void zipkat_get(zipkat_get_request& request);
#endif
    
private:

    /**
     * This method processes this client's requests in a loop.
     */
    void loop();

    /** current client's network receive queue */
    zip::network::recv_queue recv_queue_;

#ifdef COLOCATED_ZIPKAT
    /**
     * This method processes this client's zipkat get requests in a loop.
     */
    void zipkat_get_loop();

    /** zipkat get receive queue */
    zip::network::recv_queue zipkat_get_recv_queue_;
    std::vector<zip::network::recv_queue> dumb_queues_;
#endif

    void send_to_replicas(zip::network::buffer& buffer, unsigned long length, unsigned int dest, bool sync);
    void send_to_replicas(void* buffer, unsigned long length, unsigned int dest, bool sync);

    /**
     * ID of the client
     *
     * NOTE: client ID can be a unique ID
     *       in the range `[0, zip::consts::NUM_CLIENTS - 1]`
     * */
    uint64_t client_id_;

    /** ID of the shard this client is connected to */
    uint64_t shard_id_;

    /** intro message for this client */
    zip::api::client_intro intro_;

    /** network manager */
    zip::network::manager& manager_;

    /** network send queue to the ordering service */
    zip::network::send_queue order_;

    /** client thread for processing requests */
#ifdef COLOCATED_ZIPKAT
    std::vector<std::thread> threads_;
#else
    std::thread thread_;
#endif

    /** signal to stop the processing thread */
    std::atomic<bool> stop_ = false;

    /** whether the client is currently processing a request */
    std::atomic_flag busy_ = ATOMIC_FLAG_INIT;

    /** location to store the request */
    std::atomic<request*> request_ = nullptr;

    /** array of lists to store all the buffers in use */
    std::array<std::list<zip::network::buffer>, zip::consts::NUM_BUFFER_SIZES> used_buffers_;

#ifdef COLOCATED_ZIPKAT
    /** lock for storage_queues_ */
    std::shared_mutex queue_lock_;

    /** send queue to replicas */
    std::unordered_map<uint64_t, zip::network::send_queue> replica_queues_;
    std::vector<zip::network::send_queue> get_send_queues_;

#ifdef ZIPKAT_SEPARATE_THREAD
    std::atomic<unsigned int> zipkat_get_destination_;
#endif

    /** whether the zipkat get is currently processing a request */
    std::atomic_flag zipkat_get_busy_ = ATOMIC_FLAG_INIT;

    /** vector of vector to store all the buffers in use */
    std::array<std::list<zip::network::buffer>, zip::consts::NUM_BUFFER_SIZES> zipkat_used_buffers_;

    /** location to store the request */
    std::atomic<zipkat_get_request*> zipkat_get_request_ = nullptr;
#endif

};

} // namespace client
} // namespace zip
