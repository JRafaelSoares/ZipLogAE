#include "network/recv_queue.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <ext/alloc_traits.h>
#include <infiniband/verbs.h>

#include "network/buffer.h"
#include "util/log.h"
#include "util/util.h"

namespace zip {
namespace network {

/** create a logger for this file */
static zip::util::logger logger("recv_queue");

recv_queue::recv_queue(ibv_srq* recv_queue, ibv_cq* completion_queue, std::array<ibv_mr*, zip::consts::rdma::MAX_QUEUE_PAIRS>& receive_buffers):
recv_queue_(recv_queue), completion_queue_(completion_queue), receive_buffers_(&receive_buffers) {
    // post the maximum number of receive requests
    arm(zip::consts::rdma::MAX_RECEIVE_REQUESTS);
}

recv_queue::~recv_queue() {
    // destroy the objects if they're valid
    if (recv_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_srq(recv_queue_), "could not destroy receive queue");
    if (completion_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_cq(completion_queue_), "could not destroy completion queue");
}

unsigned long recv_queue::recv(std::array<void*, zip::consts::rdma::RECEIVE_BATCH_SIZE>& result) {
    // try to poll the receive completion queue
    ibv_wc wc[zip::consts::rdma::RECEIVE_BATCH_SIZE];
    int num_polled = ibv_poll_cq(completion_queue_, zip::consts::rdma::RECEIVE_BATCH_SIZE, wc);

    // for each completion event, verify it
    for (int i = 0; i < num_polled; i++) {
        // we received a completion event, check it's status
        ZIP_ASSERT_EQ(wc[i].status, IBV_WC_SUCCESS, "network error occurred");
        ZIP_ASSERT_EQ(wc[i].opcode, IBV_WC_RECV_RDMA_WITH_IMM, "unrecognised completion queue entry");
        ZIP_ASSERT(wc[i].wc_flags | IBV_WC_WITH_IMM, "unrecognised completion queue entry");

        // set the receive parameters using the imm data
        unsigned long index = wc[i].imm_data & (zip::consts::rdma::MAX_QUEUE_PAIRS - 1), offset = wc[i].imm_data / zip::consts::rdma::MAX_QUEUE_PAIRS;
        result[i] = static_cast<uint8_t*>(receive_buffers_->operator[](index)->addr) + offset;
    }

    // return the result
    return num_polled;
}

void recv_queue::arm(unsigned long num_requests) {
    // prepare the default number of receive requests
    std::vector<ibv_recv_wr> wrs(num_requests); ibv_recv_wr* bad_wr;
    for (unsigned long i = 0; i < num_requests; i++) {
        wrs[i] = { .next = (i == num_requests - 1) ? nullptr : &wrs[i + 1] };
    }

    // post the requests to the receive queue
    ZIP_ASSERT_ZERO(ibv_post_srq_recv(recv_queue_, wrs.data(), &bad_wr), "could not post buffers to receive queue")
}

} // namespace network
} // namespace zip
