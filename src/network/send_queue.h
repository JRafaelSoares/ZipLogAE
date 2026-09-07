#pragma once

#include <cstdint>
#include <utility>
#include <mutex>
#include <vector>

#include "util/consts.h"

/** Forward declaration for IBVerbs structs. */
struct ibv_cq;
struct ibv_qp;

namespace zip {
namespace network {

/** Forward declaration for buffer and manager. */
class buffer;
class manager;

/**
 * This class represents a network send queue
 * that can send messages to a particular destination.
 */
class send_queue {

public:

    /** Use the implicit default constructor. */
    send_queue() = default;

    /** Delete the copy assignment operator. */
    send_queue& operator=(const send_queue&) = delete;

    /** Delete the copy constructor. */
    send_queue(const send_queue&) = delete;

    /**
     * Move assignment operator for the send queue object.
     *
     * @param other the other send queue to move
     *
     * @return a reference to this
     */
    inline send_queue& operator=(send_queue&& other) {
        // if the given object is different
        // then swap the elements
        if (this != &other) {
            std::swap(send_queue_, other.send_queue_);
            std::swap(recv_queue_, other.recv_queue_);
            std::swap(completion_queue_, other.completion_queue_);
            std::swap(num_queues_, other.num_queues_);
            std::swap(xrc_queue_nums_, other.xrc_queue_nums_);
            std::swap(sent_messages_, other.sent_messages_);
            std::swap(max_outstanding_, other.max_outstanding_);
            std::swap(remote_key_, other.remote_key_);
            std::swap(address_, other.address_);
            std::swap(manager_, other.manager_);
            std::swap(offset_, other.offset_);
            std::swap(index_, other.index_);
        }

        // return a reference to this
        return *this;
    }

    /**
     * Move constructor for the send queue object.
     *
     * @param other the other send queue to move
     */
    inline send_queue(send_queue&& other) {
        std::swap(send_queue_, other.send_queue_);
        std::swap(recv_queue_, other.recv_queue_);
        std::swap(completion_queue_, other.completion_queue_);
        std::swap(num_queues_, other.num_queues_);
        std::swap(xrc_queue_nums_, other.xrc_queue_nums_);
        std::swap(sent_messages_, other.sent_messages_);
        std::swap(max_outstanding_, other.max_outstanding_);
        std::swap(remote_key_, other.remote_key_);
        std::swap(address_, other.address_);
        std::swap(manager_, other.manager_);
        std::swap(offset_, other.offset_);
        std::swap(index_, other.index_);
    }

    /**
     * Initialise a send queue.
     *
     * @param send_queue       the network send queue
     * @param recv_queue       the network receive queue
     * @param completion_queue the associated completion queue
     * @param xrc_queue_nums   the XRC shared receive queue numbers
     * @param index            the index into the destination receive buffers array
     * @param address          the address of the destination receive buffer
     * @param remote_key       the destination remote access key
     * @param manager          the network manager
     */
    send_queue(ibv_qp* send_queue, ibv_qp* recv_queue, ibv_cq* completion_queue, std::vector<uint32_t> xrc_queue_nums, unsigned long index, void* address, uint32_t remote_key, manager& manager);

    /**
     * Deinitialise a send_queue.
     */
    ~send_queue();

    /**
     * Set the maximum number of outstanding messages.
     *
     * @param max_outstanding number of outstanding messages
     */
    void set_max_outstanding(unsigned long max_outstanding);

    /**
     * Send the buffer to the destination.
     *
     * @param buffer      the buffer to send
     * @param length      length of the buffer to send
     * @param synchronize whether to synchronize the send queue
     */
    void send(buffer& buffer, unsigned long length, bool synchronize = false);

    /**
     * Send the buffer to the destination inline.
     *
     * @param buffer      the buffer to send
     * @param length      length of the buffer to send
     * @param synchronize whether to synchronize the send queue
     */
    void send(void* buffer, unsigned long length, bool synchronize = false);

    /**
     * Send the buffer to the destination.
     *
     * @param buffer      the buffer to send
     * @param length      length of the buffer to send
     * @param destination destination shared receieve queue to send to
     * @param synchronize whether to synchronize the send queue
     */
    void send(buffer& buffer, unsigned long length, unsigned long destination, bool synchronize = false);

    /**
     * Send the buffer to the destination inline.
     *
     * @param buffer      the buffer to send
     * @param length      length of the buffer to send
     * @param destination destination shared receieve queue to send to
     * @param synchronize whether to synchronize the send queue
     */
    void send(void* buffer, unsigned long length, unsigned long destination, bool synchronize = false);

private:

    /** send queue for the destination */
    ibv_qp* send_queue_ = nullptr;

    /** receive queue for the destination */
    ibv_qp* recv_queue_ = nullptr;

    /** associated completion queue */
    ibv_cq* completion_queue_ = nullptr;

    /** destination XRC queue numbers */
    std::vector<uint32_t> xrc_queue_nums_;

    /** number of destination queues */
    unsigned long num_queues_;

    /** index into the receive buffers array */
    unsigned long index_;

    /** address of the destination receive buffer */
    void* address_;

    /** destination remote access key */
    uint32_t remote_key_;

    /** beginning offset for the next message */
    std::mutex l_;
    unsigned long offset_ = 0;

    /** number of messages sent */
    unsigned long sent_messages_ = 0;

    /** maximum number of outstanding messages */
    unsigned long max_outstanding_ = zip::consts::rdma::MAX_OUTSTANDING_REQUESTS;

    /** pointer to the network manager */
    manager* manager_ = nullptr;

};



} // namespace network
} // namespace zip
