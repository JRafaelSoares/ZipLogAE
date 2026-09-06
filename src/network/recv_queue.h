#pragma once

#include <array>
#include <concepts>
#include <memory>
#include <utility>
#include <vector>

#include <infiniband/verbs.h>

#include "util/consts.h"

namespace zip::network {

/**
 * This class denotes a network receive queue
 * that can receive messages from any destination.
 */
class recv_queue {

public:

    /** Use the default implicit constructor. */
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
        std::swap(recv_buffers_, other.recv_buffers_);
        std::swap(num_posted_, other.num_posted_);
        std::swap(recv_requests_, other.recv_requests_);
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
            std::swap(recv_buffers_, other.recv_buffers_);
            std::swap(num_posted_, other.num_posted_);
            std::swap(recv_requests_, other.recv_requests_);
        }
        return *this;
    }

    /**
     * Initialize a receive queue with the given shared receive queue.
     *
     * @param recv_queue       the shared receive queue
     * @param completion_queue the completion queue associated with them
     * @param recv_buffers  the array of receive buffers
     */
    recv_queue(ibv_srq* recv_queue, ibv_cq* completion_queue, std::array<void*, zip::consts::rdma::MAX_QUEUE_PAIRS>& recv_buffers);

    /**
     * Deinitialize a receive queue.
     */
    ~recv_queue();

    /**
     * Try to do a non-blocking receive on the queue
     * and execute the callback on the received messages.
     *
     * @param function the callback to execute on the messages
     */
    template <unsigned long N = zip::consts::rdma::RECEIVE_BATCH_SIZE, typename Callback>
    requires std::invocable<Callback, void*, unsigned long> && (N > 0 && N <= zip::consts::rdma::RECEIVE_BATCH_SIZE)
    inline void recv(Callback function) {
        std::array<std::pair<void*, unsigned long>, zip::consts::rdma::RECEIVE_BATCH_SIZE> result;
        auto num_buffers = recv(result, N);
        for (unsigned long index = 0; index < num_buffers; index++) {
            function(std::assume_aligned<zip::consts::rdma::MESSAGE_ALIGNMENT>(result[index].first), result[index].second);
        }
        if (num_posted_ < zip::consts::rdma::MAX_RECEIVE_REQUESTS / 2) {
            arm(zip::consts::rdma::MAX_RECEIVE_REQUESTS - num_posted_);
        }
    }

private:

    friend class manager;

    /**
     * Do a non-blocking receive on the given queue.
     *
     * @param result   where to store the received buffers
     * @param max_poll maximum number of completions to poll
     *
     * @return number of received buffers
     */
    unsigned long recv(std::array<std::pair<void*, unsigned long>, zip::consts::rdma::RECEIVE_BATCH_SIZE>& result, unsigned long max_poll);

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

    /** pointer to the receive buffers array */
    std::array<void*, zip::consts::rdma::MAX_QUEUE_PAIRS>* recv_buffers_ = nullptr;

    /** number of receive buffers posted on the receive queue */
    unsigned long num_posted_ = 0;

    /** prepared bunch of receive work requests */
    std::vector<ibv_recv_wr> recv_requests_;

};

} // namespace zip::network
