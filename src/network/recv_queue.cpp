#include "network/recv_queue.h"

#include <cstdint>
#include <vector>

#include <infiniband/verbs.h>

#include "util/log.h"

namespace zip::network {

/** create a logger for this file */
static zip::util::logger logger("recv_queue");

recv_queue::recv_queue(ibv_srq* recv_queue, ibv_cq* completion_queue, std::array<void*, zip::consts::rdma::MAX_QUEUE_PAIRS>& recv_buffers):
recv_queue_(recv_queue), completion_queue_(completion_queue), recv_buffers_(&recv_buffers) {
    // prepare the receive requests
    recv_requests_.resize(zip::consts::rdma::MAX_RECEIVE_REQUESTS, {});
    for (unsigned long i = 1; i < zip::consts::rdma::MAX_RECEIVE_REQUESTS; i++) {
        recv_requests_[i - 1].next = &recv_requests_[i];
    }

    // post the maximum number of receive requests
    arm(zip::consts::rdma::MAX_RECEIVE_REQUESTS);
}

recv_queue::~recv_queue() {
    // destroy the objects if they're valid
    if (recv_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_srq(recv_queue_), "could not destroy receive queue");
    if (completion_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_cq(completion_queue_), "could not destroy completion queue");
}

unsigned long recv_queue::recv(std::array<std::pair<void*, unsigned long>, zip::consts::rdma::RECEIVE_BATCH_SIZE>& result, unsigned long max_poll) {
    // try to poll the receive completion queue
    ibv_wc wc[zip::consts::rdma::RECEIVE_BATCH_SIZE];
    int num_polled = ibv_poll_cq(completion_queue_, max_poll, wc);
    num_posted_ -= num_polled;

    // for each completion event, verify it
    for (int i = 0; i < num_polled; i++) {
        // we received a completion event, check it's status
        ZIP_ASSERT_EQ(wc[i].status, IBV_WC_SUCCESS, "network error occurred");
        ZIP_ASSERT_EQ(wc[i].opcode, IBV_WC_RECV_RDMA_WITH_IMM, "unrecognized completion queue entry");
        ZIP_ASSERT(wc[i].wc_flags | IBV_WC_WITH_IMM, "immediate value not available");

        // set the receive parameters using the imm data
        unsigned long index = wc[i].imm_data & (zip::consts::rdma::MAX_QUEUE_PAIRS - 1),
                      offset = wc[i].imm_data / zip::consts::rdma::MAX_QUEUE_PAIRS;
        result[i].first = static_cast<uint8_t*>(recv_buffers_->operator[](index)) + offset;
        result[i].second = wc[i].byte_len;
    }

    // return the result
    return num_polled;
}

void recv_queue::arm(unsigned long num_requests) {
    // prepare the receive requests
    auto old = recv_requests_[num_requests - 1].next;
    recv_requests_[num_requests - 1].next = nullptr;

    // post the requests to the receive queue
    ibv_recv_wr* bad_wr;
    ZIP_ASSERT_ZERO(ibv_post_srq_recv(recv_queue_, recv_requests_.data(), &bad_wr), "could not post buffers to receive queue")

    // update the state
    recv_requests_[num_requests - 1].next = old;
    num_posted_ += num_requests;
}

} // namespace zip::network
