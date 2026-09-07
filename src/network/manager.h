#pragma once

#include <array>
#include <cstdint>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <infiniband/verbs.h>
#include <poll.h>
#include <signal.h>

#include "util/consts.h"
#include "util/util.h"

/** Forward declaration for socket structs. */
struct sockaddr_in;

namespace zip {
namespace network {

/** Forward declaration for send and receive queues. */
class recv_queue;
class send_queue;
class buffer;

/** Forward declaration for destination info. */
namespace detail { struct destination_info; }

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
     * Initialise the network manager as a simple client.
     *
     * @param device network device to initialise
     * @param port   network device port to use
     * @param gid    network GID index to use
     */
    manager(std::string device, uint8_t port, int gid);

    /**
     * Deinitialise the manager.
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
    std::optional<std::tuple<send_queue, zip::util::unique_ptr_malloc<void>, unsigned long, sockaddr_in>>
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
    std::tuple<send_queue, zip::util::unique_ptr_malloc<void>, unsigned long>
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
    std::tuple<send_queue, zip::util::unique_ptr_malloc<void>, unsigned long>
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
    std::optional<std::tuple<std::vector<send_queue>, zip::util::unique_ptr_malloc<void>, unsigned long, sockaddr_in>>
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
    std::tuple<std::vector<send_queue>, zip::util::unique_ptr_malloc<void>, unsigned long>
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
    std::tuple<std::vector<send_queue>, zip::util::unique_ptr_malloc<void>, unsigned long>
    connect(sockaddr_in address, void* message, unsigned long length, unsigned long num_queues);

    /**
     * Get a number of free network buffers of the given length.
     *
     * @param length      the minimum length of the buffer
     * @param num_buffers the number of buffers to allocate
     *
     * @return the buffers
     */
    std::list<buffer> get_buffers(unsigned long length, unsigned long num_buffers);

    /**
     * Create the given number of network receive queues.
     *
     * @param num_queues the number of queues to create
     *
     * @return the created queues
     */
    std::vector<recv_queue> create_recv_queues(unsigned long num_queues);

private:

    /** Make buffer and send_queue friend of this class */
    friend class send_queue;
    friend class buffer;

    /**
     * Return a buffer to the free-list.
     *
     * @param buffer    address of the buffer
     * @param local_key local key for network device access to this buffer
     * @param index     size index of this buffer
     */
    void put_buffer(void* buffer, uint32_t local_key, unsigned long index);

    /**
     * Free a slot for a receive buffer to be used.
     *
     * @return index index into the receive buffer array
     */
    void free_receive_buffer(unsigned long index);

    /**
     * Allocate a single huge page and divide it into
     * buffers and add them to free list.
     *
     * NOTE: requires the `buffer_lock_[index]` to be taken.
     *
     * @param index the size index of the buffer
     */
    void allocate_buffers(unsigned long index);

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

    /** IBVerbs context */
    ibv_context* context_;

    /** IBVerbs protection domain */
    ibv_pd* protection_domain_;

    /** IBVerbs device */
    ibv_device* device_;

    /** IBVerbs XRC domain */
    ibv_xrcd* xrc_domain_;

    /** IBVerbs local device ID */
    uint16_t local_id_;

    /** IBVerbs device port */
    uint8_t device_port_;

    /** GID index of the local device */
    int gid_;

    /** GID of the local device */
    ibv_gid local_gid_;

    /** lock to protect the state of the manager */
    std::mutex lock_;

    /** vector of shared receive queue numbers */
    std::vector<uint32_t> xrc_queue_nums_;

    /** whether the manager is a server */
    bool server_;

    /** socket to listen on for connections */
    int socket_;

    /** poll fd for a `::poll` call on socket */
    pollfd poll_;

    /** signal set to ignore signals */
    sigset_t signal_;

    /** memory pages registered with the network device */
    std::list<ibv_mr*> page_list_;

    /** map from buffer lengths to free buffers */
    std::array<std::list<std::pair<void*, uint32_t>>, zip::consts::NUM_BUFFER_SIZES> free_list_;

    /** array of buffers to receive the data in for each queue pair */
    std::array<ibv_mr*, zip::consts::rdma::MAX_QUEUE_PAIRS> receive_buffers_{};   

    /** bitmap of whether the given index is in use */
    std::array<bool, zip::consts::rdma::MAX_QUEUE_PAIRS> free_receive_buffers_;   

};

} // namespace network
} // namespace zip
