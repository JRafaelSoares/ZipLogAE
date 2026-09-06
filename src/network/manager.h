#pragma once

#include <array>
#include <bitset>
#include <concepts>
#include <cstdint>
#include <cstdlib>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <infiniband/verbs.h>
#include <poll.h>
#include <signal.h>

#include "util/concurrent.h"
#include "util/consts.h"

/** Forward declaration for socket structs. */
struct sockaddr_in;

namespace zip::network {

/** Forward declaration for send and receive queues. */
class recv_queue;
class send_queue;
class buffer;

/**
 * Manages all the global resources associated
 * with networking.
 */
class manager {

public:

    /** Delete the default constructor. */
    manager() = delete;

    /** Delete the copy assignment operator. */
    manager& operator=(const manager&) = delete;

    /** Delete the copy constructor. */
    manager(const manager&) = delete;

    /** Delete the move assignment operator. */
    manager& operator=(manager&&) = delete;

    /** Delete the move constructor. */
    manager(manager&&) = delete;

    /**
     * Initialize the network manager as a simple client.
     *
     * @param device network device to initialize
     * @param port   network device port to use
     * @param gid    network GID index to use
     * @param numa   NUMA node to use for memory allocation
     */
    manager(std::string device, uint8_t port, int gid, int numa = -1);

    /**
     * Deinitialize the manager.
     */
    ~manager();

    /**
     * Bind the manager to the given port
     * to listen for client connections.
     *
     * @param port the TCP/IP port to listen on
     */
    void bind_server(uint16_t port);

    /**
     * Accept an incoming client connection and create a send
     * queue to the destination.
     *
     * @param message    the first message to send while connecting
     * @param length     length of the message
     *
     * @return the destination send queue, message received while connecting,
     *         it's length, and the address of the connecting server
     *         if the connection exists
     */
    std::optional<std::tuple<send_queue, void*, unsigned long, sockaddr_in>>
    accept(void* message, unsigned long length);

    /**
     * Connect to the given server and create a send queue to
     * the destination.
     *
     * @param server_ip  IP address of the server
     * @param port       port of the server
     * @param message    the first message to send while connecting
     * @param length     length of the message
     *
     * @return the destination send queue, message received while connecting,
     *         and it's length
     */
    std::optional<std::tuple<send_queue, void*, unsigned long>>
    connect(std::string server_ip, uint16_t port, void* message, unsigned long length);

    /**
     * Connect to the given server and create a send queue to
     * the destination.
     *
     * @param address    IP address of the server
     * @param message    the first message to send while connecting
     * @param length     length of the message
     *
     * @return the destination send queue, message received while connecting,
     *         and it's length
     */
    std::optional<std::tuple<send_queue, void*, unsigned long>>
    connect(sockaddr_in address, void* message, unsigned long length);

    /**
     * Accept an incoming client connection and create a send
     * queue to the destination.
     *
     * @param message    the first message to send while connecting
     * @param length     length of the message
     * @param num_queues number of send/receive queue pairs to setup
     *
     * NOTE: the client and server exchange `num_queues` arguments
     *       and create the minimum of the number of queues requested
     *       by either of them. However, if one of them sets this
     *       argument to 0, then they choose the other non-zero argument
     *       to decide the number of queues. Additionally, if both
     *       are zero then they create one receive queue.
     *
     * @return the destination send queues, message received while connecting,
     *         it's length, and the address of the connecting server
     *         if the connection exists
     */
    std::optional<std::tuple<std::vector<send_queue>, void*, unsigned long, sockaddr_in>>
    accept(void* message, unsigned long length, unsigned long num_queues);

    /**
     * Connect to the given server and create a send queue to
     * the destination.
     *
     * @param server_ip  IP address of the server
     * @param port       port of the server
     * @param message    the first message to send while connecting
     * @param length     length of the message
     * @param num_queues number of send/receive queue pairs to setup
     *
     * NOTE: the client and server exchange `num_queues` arguments
     *       and create the minimum of the number of queues requested
     *       by either of them. However, if one of them sets this
     *       argument to 0, then they choose the other non-zero argument
     *       to decide the number of queues. Additionally, if both
     *       are zero then they create one receive queue.
     *
     * @return the destination send queues, message received while connecting,
     *         and it's length
     */
    std::optional<std::tuple<std::vector<send_queue>, void*, unsigned long>>
    connect(std::string server_ip, uint16_t port, void* message, unsigned long length, unsigned long num_queues);

    /**
     * Connect to the given server and create a send queue to
     * the destination.
     *
     * @param address    IP address of the server
     * @param message    the first message to send while connecting
     * @param length     length of the message
     * @param num_queues number of send/receive queue pairs to setup
     *
     * NOTE: the client and server exchange `num_queues` arguments
     *       and create the minimum of the number of queues requested
     *       by either of them. However, if one of them sets this
     *       argument to 0, then they choose the other non-zero argument
     *       to decide the number of queues. Additionally, if both
     *       are zero then they create one receive queue.
     *
     * @return the destination send queues, message received while connecting,
     *         and it's length
     */
    std::optional<std::tuple<std::vector<send_queue>, void*, unsigned long>>
    connect(sockaddr_in address, void* message, unsigned long length, unsigned long num_queues);

    /**
     * Get a number of free network buffers.
     *
     * @param num_buffers the number of buffers to allocate
     *
     * @return the buffers
     */
    std::vector<buffer> get_buffers(unsigned long num_buffers);

    /**
     * Create the given number of network receive queues.
     *
     * @param num_queues the number of queues to create
     *
     * @return the created queues
     */
    std::vector<recv_queue> create_recv_queues(unsigned long num_queues);

    /**
     * Poll for any failures that have occurred
     * and perform the callback on the failed
     * send queue object.
     *
     * @param callback the function to execute
     */
    template <typename Callback> requires std::invocable<Callback, void*, unsigned long>
    void process_failure(Callback callback) {
        // check whether a new RDMA event is available
        if (auto event = events_.dequeue()) {
            // try to get the context from the event
            handshake_message* context;
            switch (event->event_type) {
            case IBV_EVENT_QP_FATAL:
            case IBV_EVENT_QP_REQ_ERR:
            case IBV_EVENT_QP_ACCESS_ERR:
            case IBV_EVENT_PATH_MIG_ERR:
                context = static_cast<handshake_message*>(event->element.qp->qp_context);
                break;
            default:
                context = nullptr;
            }

            // if we were able to extract a valid context
            // then apply the callback
            if (context != nullptr) {
                callback(context->data, context->data_length);
            }
        }
    }

    // For triggering client failures.
    void deactivate_srq(recv_queue& queue);

private:

    /** Make buffer and send_queue friend of this class */
    friend class send_queue;
    friend class buffer;

    /** A struct for handshake message. */
    struct handshake_message {

        uint64_t data_length; /** length of the data */
        uint8_t  data[0];     /** beginning of the the data */

    } __attribute__((packed));

    /**
     * Return a buffer to the free-list.
     *
     * @param buffer    address of the buffer
     * @param local_key local key for network device access to this buffer
     */
    void put_buffer(void* buffer, uint32_t local_key);

    /**
     * Free a slot for a receive buffer to be used.
     *
     * @return index index into the receive buffer array
     */
    void free_recv_buffer(unsigned long index);

    /**
     * Create network queues for connecting to
     * a destination.
     *
     * @return the send queue, the receive queue, and the completion queue
     */
    std::tuple<ibv_qp*, ibv_qp*, ibv_cq*> create_queues();

    /**
     * Create a network receive buffer that a destination
     * can write messages to.
     *
     * @return the index, address, and remote key of the buffer
     */
    std::tuple<unsigned long, void*, uint32_t> create_receive_buffer();

    /**
     * Poll for an RDMA network event.
     */
    void poll_events(std::stop_token stop_token);

    /** RDMA context */
    ibv_context* context_;

    /** RDMA protection domain */
    ibv_pd* protection_domain_;

    /** RDMA device */
    ibv_device* device_;

    /** NUMA node for the RDMA device */
    int numa_;

    /** RDMA XRC domain */
    ibv_xrcd* xrc_domain_;

    /** RDMA local device ID */
    uint16_t local_id_;

    /** RDMA device port */
    uint8_t device_port_;

    /** GID index of the local device */
    int gid_;

    /** GID of the local device */
    ibv_gid local_gid_;

    /** lock to protect XRC receive queues state */
    std::mutex xrc_lock_;

    /** vector of shared receive queue numbers */
    std::vector<uint32_t> xrc_queue_nums_;

    /** lock to protect server state */
    std::mutex server_lock_;

    /** whether the manager is a server */
    bool server_;

    /** socket to listen on for connections */
    int socket_;

    /** poll fd for a `::poll` call on socket */
    pollfd poll_;

    /** signal set to ignore signals */
    sigset_t signal_;

    /** queue to hold all the async RDMA events */
    zip::util::queue<ibv_async_event> events_;

    /** thread for polling events */
    std::jthread event_thread_;

    /** lock to protect memory allocation state */
    std::mutex memory_lock_;

    /** memory pages registered with the network device */
    std::list<ibv_mr*> page_list_;

    /** free list of buffers */
    std::list<std::pair<void*, uint32_t>> free_list_;

    /** receive buffer address for each queue pair */
    std::array<void*, zip::consts::rdma::MAX_QUEUE_PAIRS> receive_buffers_ {};

    /** receive buffer remote keys for each queue pair */
    std::array<uint32_t, zip::consts::rdma::MAX_QUEUE_PAIRS> receive_keys_ {};

    /** bitmap of whether the given index is in use */
    std::bitset<zip::consts::rdma::MAX_QUEUE_PAIRS> free_receive_buffers_;

    /** lock to protect handshake messages list */
    std::mutex handshake_lock_;

    /** list of all the handshake messages received */
    std::list<std::unique_ptr<handshake_message, decltype(std::free)*>> handshakes_;

};

} // namespace zip::network
