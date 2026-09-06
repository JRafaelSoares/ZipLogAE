#include "network/send_queue.h"

#include <cstdint>
#include <vector>

#include <infiniband/verbs.h>

#include "network/buffer.h"
#include "network/manager.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/util.h"

namespace zip::network {

/** create a logger for this file */
static zip::util::logger logger("send_queue");

send_queue::send_queue(
    ibv_qp* send_queue,
    ibv_qp* recv_queue,
    ibv_cq* completion_queue,
    unsigned long index,
    void* address,
    uint32_t remote_key,
    std::vector<uint32_t> xrc_queue_nums,
    manager& manager
):
send_queue_(send_queue), recv_queue_(recv_queue), completion_queue_(completion_queue), index_(index),
address_(address), remote_key_(remote_key), xrc_queue_nums_(xrc_queue_nums), manager_(&manager) {}

send_queue::~send_queue() {
    // destroy the objects if they're valid
    if (manager_ != nullptr) manager_->free_recv_buffer(index_);
    if (send_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_qp(send_queue_), "could not destroy send queue");
    if (recv_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_qp(recv_queue_), "could not destroy receive queue");
    if (completion_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_cq(completion_queue_), "could not destroy completion queue");
}

void send_queue::set_max_outstanding(unsigned long max_outstanding) {
    ZIP_ASSERT(max_outstanding <= zip::consts::rdma::MAX_OUTSTANDING_REQUESTS, "invalid value of maximum outstanding messages");
    max_outstanding_ = max_outstanding;
}

void send_queue::set_assert_on_failure(bool assert_on_failure) {
    assert_on_failure_ = assert_on_failure;
}

int send_queue::send(void* buffer, unsigned long length, bool synchronize) {
    return send(buffer, length, 0, synchronize);
}

namespace detail {

/**
 * Wait until a valid completion queue entry is
 * found on the given completion queue.
 */
inline int poll(ibv_cq* completion_queue) {
    // wait until we receive a completion and check it's status
    ibv_wc wc; int err; while ((err = ibv_poll_cq(completion_queue, 1, &wc)) == 0) zip::util::relax();
    return wc.status;
}

} // namesapce detail

#define HANDLE_ERROR(expr, errstr) {     \
    int err = (expr);                    \
    if (assert_on_failure_) [[likely]] { \
        ZIP_ASSERT_ZERO(err, errstr);    \
    } else if (err != 0) {               \
        return err;                      \
    }                                    \
}

int send_queue::send(void* buffer, unsigned long length, unsigned long destination, bool synchronize) {
    // verify the length of the send buffers
    ZIP_ASSERT(length <= zip::consts::rdma::MAX_INLINE_DATA, "could not send message this large");

    // synchronize if required
    if (sent_messages_ >= max_outstanding_) {
        HANDLE_ERROR(detail::poll(completion_queue_), "could not poll completion queue");
        sent_messages_ = 0;
    }

    // set the synchronization flag
    unsigned int flags = IBV_SEND_INLINE;
    if (synchronize || ++sent_messages_ >= max_outstanding_) {
        flags |= IBV_SEND_SIGNALED;
    }

    // calculate the destination address offset, and update for the next send
    auto offset = offset_ + length <= zip::consts::RECEIVE_BUFFER_SIZE ? offset_ : 0;
    auto addr = reinterpret_cast<uint64_t>(address_) + offset;
    auto imm = static_cast<uint32_t>(index_ | offset * zip::consts::rdma::MAX_QUEUE_PAIRS);
    offset_ = zip::util::align<zip::consts::rdma::MESSAGE_ALIGNMENT>(offset + length);

    // create the memory descriptor and work request
    ibv_sge sge {
        .addr = reinterpret_cast<uint64_t>(buffer),
        .length = static_cast<uint32_t>(length)
    };
    ibv_send_wr wr {
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = flags,
        .imm_data = imm,
        .wr = {
            .rdma = {
                .remote_addr = addr,
                .rkey = remote_key_
            }
        },
        .qp_type = {
            .xrc = {
                .remote_srqn = xrc_queue_nums_[destination % xrc_queue_nums_.size()]
            }
        }
    };

    // post the work request
    ibv_send_wr* bad_wr;
    HANDLE_ERROR(ibv_post_send(send_queue_, &wr, &bad_wr), "could not post send request");

    // if we need to poll for completion then
    // do so until we receive the completion event
    if (synchronize) {
        HANDLE_ERROR(detail::poll(completion_queue_), "could not poll completion queue");
        sent_messages_ = 0;
    }

    // return success
    return 0;
}

int send_queue::send(buffer& buffer, unsigned long length, bool synchronize) {
    return send(buffer, length, 0, synchronize);
}

int send_queue::send(buffer& buffer, unsigned long length, unsigned long destination, bool synchronize) {
    // verify the length of the send buffers
    ZIP_ASSERT(length <= zip::consts::SEND_BUFFER_SIZE, "could not send message this large");

    // synchronize if required
    if (sent_messages_ >= max_outstanding_) {
        HANDLE_ERROR(detail::poll(completion_queue_), "could not poll completion queue");
        sent_messages_ = 0;
    }

    // set the synchronization flag
    unsigned int flags = 0;
    if (synchronize || ++sent_messages_ >= max_outstanding_) {
        flags |= IBV_SEND_SIGNALED;
    }

    // set the inline flag
    if (length <= zip::consts::rdma::MAX_INLINE_DATA) {
        flags |= IBV_SEND_INLINE;
    }

    // calculate the destination address offset, and update for the next send
    auto offset = offset_ + length <= zip::consts::RECEIVE_BUFFER_SIZE ? offset_ : 0;
    auto addr = reinterpret_cast<uint64_t>(address_) + offset;
    auto imm = static_cast<uint32_t>(index_ | offset * zip::consts::rdma::MAX_QUEUE_PAIRS);
    offset_ = zip::util::align<zip::consts::rdma::MESSAGE_ALIGNMENT>(offset + length);

    // create the memory descriptor and work request
    ibv_sge sge {
        .addr = reinterpret_cast<uint64_t>(buffer.buffer_),
        .length = static_cast<uint32_t>(length),
        .lkey = buffer.local_key_
    };
    ibv_send_wr wr {
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = flags,
        .imm_data = imm,
        .wr = {
            .rdma = {
                .remote_addr = addr,
                .rkey = remote_key_
            }
        },
        .qp_type = {
            .xrc = {
                .remote_srqn = xrc_queue_nums_[destination % xrc_queue_nums_.size()]
            }
        }
    };

    // post the work request
    ibv_send_wr* bad_wr;
    HANDLE_ERROR(ibv_post_send(send_queue_, &wr, &bad_wr), "could not post send request");

    // if we need to poll for completion then
    // do so until we receive the completion event
    if (synchronize) {
        HANDLE_ERROR(detail::poll(completion_queue_), "could not poll completion queue");
        sent_messages_ = 0;
    }

    // return success
    return 0;
}

#undef HANDLE_ERROR

} // namespace zip::network
