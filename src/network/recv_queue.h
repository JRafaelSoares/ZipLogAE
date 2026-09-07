#pragma once

#include <array>
#include <list>
#include <type_traits>
#include <utility>

#include "network/buffer.h"
#include "util/consts.h"

/** Forward declaration for IBVerbs structs. */
struct ibv_cq;
struct ibv_srq;
struct ibv_mr;

namespace zip {
namespace network {

/**
 * This class denotes a network receive queue
 * that can receive messages from any destination.
 */
class recv_queue {

public:

    /** Use the implicit default constructor. */
    recv_queue() = default;

    /** Delete the copy assignment operator. */
    recv_queue& operator=(const recv_queue&) = delete;

    /** Delete the copy constructor. */
    recv_queue(const recv_queue&) = delete;

    /**
     * Move constructor for the receive queue object.
     *
     * @param other the other receive queue
     */
    inline recv_queue(recv_queue&& other) {
        std::swap(recv_queue_, other.recv_queue_);
        std::swap(completion_queue_, other.completion_queue_);
        std::swap(receive_buffers_, other.receive_buffers_);
    }

    /**
     * Move assignment operator for the receive queue object.
     *
     * @param other the other receive queue
     *
     * @return a reference to this receive queue object
     */
    inline recv_queue& operator=(recv_queue&& other) {
        if (this != &other) {
            std::swap(recv_queue_, other.recv_queue_);
            std::swap(completion_queue_, other.completion_queue_);
            std::swap(receive_buffers_, other.receive_buffers_);
        }
        return *this;
    }

    /**
     * Initialise a receive queue with the given shared receive queue.
     *
     * @param recv_queue       the shared receive queue
     * @param completion_queue the completion queue associated with them
     * @param receive_buffers  the array of receive buffers
     */
    recv_queue(ibv_srq* recv_queue, ibv_cq* completion_queue, std::array<ibv_mr*, zip::consts::rdma::MAX_QUEUE_PAIRS>& receive_buffers);

    /**
     * Deinitialise a receive queue.
     */
    ~recv_queue();

    /**
     * Try to do a non-blocking receive on the queue
     * and execute the callback on the received message.
     *
     * @param function the callback to execute on the message
     */
    template <typename Callback>
    inline std::enable_if_t<std::is_invocable_v<decltype(&Callback::operator()), Callback&, void*>, void>
    recv(Callback function) {
        std::array<void*, zip::consts::rdma::RECEIVE_BATCH_SIZE> result{};
        auto num_buffers = recv(result);
        for (unsigned long index = 0; index < num_buffers; index++) {
            function(result[index]);
        }
        if (num_buffers) arm(num_buffers);
    }

private:

    /**
     * Do a non-blocking receive on the given queue.
     *
     * @param result where to store the received buffers
     *
     * @return number of received buffers
     */
    unsigned long recv(std::array<void*, zip::consts::rdma::RECEIVE_BATCH_SIZE>& result);

    /**
     * Post the given number of work requests to the queue.
     *
     * @param num_requests number of requests to post
     */
    void arm(unsigned long num_requests);

    /** shared receive queue */
    ibv_srq* recv_queue_ = nullptr;

    /** associated completion queue */
    ibv_cq* completion_queue_ = nullptr;

    /** pointer to the buffer array */
    std::array<ibv_mr*, zip::consts::rdma::MAX_QUEUE_PAIRS>* receive_buffers_ = nullptr;

};

} // namespace network
} // namespace zip
