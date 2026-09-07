#include "network/send_queue.h"

#include <cstdint>
#include <string>

#include <infiniband/verbs.h>

#include "network/buffer.h"
#include "network/manager.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/util.h"

namespace zip {
namespace network {

/** create a logger for this file */
static zip::util::logger logger("send_queue");

send_queue::send_queue(
    ibv_qp* send_queue,
    ibv_qp* recv_queue,
    ibv_cq* completion_queue,
    std::vector<uint32_t> xrc_queue_nums,
    unsigned long index,
    void* address,
    uint32_t remote_key,
    manager& manager
):
send_queue_(send_queue), recv_queue_(recv_queue), completion_queue_(completion_queue),
xrc_queue_nums_(xrc_queue_nums), num_queues_(xrc_queue_nums.size()),
index_(index), address_(address), remote_key_(remote_key), manager_(&manager) {}

send_queue::~send_queue() {
    // destroy the objects if they're valid
    if (send_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_qp(send_queue_), "could not destroy send queue");
    if (recv_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_qp(recv_queue_), "could not destroy receive queue");
    if (completion_queue_ != nullptr) ZIP_ASSERT_ZERO(ibv_destroy_cq(completion_queue_), "could not destroy completion queue");
    if (manager_ != nullptr) manager_->free_receive_buffer(index_);
}

void send_queue::set_max_outstanding(unsigned long max_outstanding) {
    ZIP_ASSERT(max_outstanding && max_outstanding <= zip::consts::rdma::MAX_OUTSTANDING_REQUESTS, "invalid value of maximum outstanding messages");
    max_outstanding_ = max_outstanding;
}

void send_queue::send(void* buffer, unsigned long length, bool synchronize) {
    return send(buffer, length, 0, synchronize);
}

void send_queue::send(void* buffer, unsigned long length, unsigned long destination, bool synchronize) {
    // verify the length of the packet
    ZIP_ASSERT(length <= zip::consts::rdma::MAX_INLINE_DATA, "could not send message this large");

    // create the memory descriptor
    struct ibv_sge sge {
        .addr = reinterpret_cast<uint64_t>(buffer),
        .length = static_cast<uint32_t>(length)
    };

    // set the relevant flag
    unsigned int flags = IBV_SEND_INLINE;

    // synchronize the send queue if we have been asked to
    // or if too many messages have been posted on the queue
    if (synchronize || ++sent_messages_ >= max_outstanding_) {
        flags = flags | IBV_SEND_SIGNALED;
        sent_messages_ = 0;
    }

    // update the destination address offset
#if 0
    unsigned long offset;
    std::unique_lock l(l_);
    {
        offset = offset_ + length <= zip::consts::HUGE_PAGE_SIZE ? offset_ : 0;
        offset_ = offset + length;
    }
#else
    auto offset = offset_ + length <= zip::consts::HUGE_PAGE_SIZE ? offset_ : 0;
    offset_ = offset + length;
#endif

    // create the work request
    struct ibv_send_wr wr {
        .wr_id = 0,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = flags,
        .imm_data = static_cast<uint32_t>(index_ | offset * zip::consts::rdma::MAX_QUEUE_PAIRS),
        .wr = {
            .rdma = {
                .remote_addr = reinterpret_cast<uint64_t>(static_cast<uint8_t*>(address_) + offset),
                .rkey = remote_key_
            }
        },
        .qp_type = {
            .xrc = {
                .remote_srqn = xrc_queue_nums_[destination % num_queues_]
            }
        }
    };

    //auto target = (void*)reinterpret_cast<uint64_t>(static_cast<uint8_t*>(address_) + offset);
    //logger.info("send to destination=", destination, ", srqn=", xrc_queue_nums_[destination % num_queues_], " at ", target, " with length=", length);

    // post the send work request
    struct ibv_send_wr *bad_wr;
    ZIP_ASSERT_ZERO(ibv_post_send(send_queue_, &wr, &bad_wr), "could not post send request");

    // if we need to poll for completion then
    // do so until we receive the completion event
    if (flags & IBV_SEND_SIGNALED) {
        while (true) {
            ibv_wc wc;
            if (ibv_poll_cq(completion_queue_, 1, &wc) > 0) {
                // we received a completion event, check it's
                // status and return
                ZIP_ASSERT_EQ(wc.opcode, IBV_WC_RDMA_WRITE, "received unknown network completion");
                ZIP_ASSERT_EQ(wc.status, IBV_WC_SUCCESS, "network error occurred");
                return;
            }
        }
    }
}

void send_queue::send(buffer& buffer, unsigned long length, bool synchronize) {
    return send(buffer, length, 0, synchronize);
}

void send_queue::send(buffer& buffer, unsigned long length, unsigned long destination, bool synchronize) {
    // verify the length of the packet
    ZIP_ASSERT(length <= buffer.length_, "could not send message this large");

    // create the memory descriptor
    struct ibv_sge sge {
        .addr = reinterpret_cast<uint64_t>(buffer.buffer_),
        .length = static_cast<uint32_t>(length),
        .lkey = buffer.local_key_
    };

    // if the data is small enough for sending
    // inline then set the relevant flag
    unsigned int flags = 0;
    if (length <= zip::consts::rdma::MAX_INLINE_DATA)
        flags = flags | IBV_SEND_INLINE;

    // synchronize the send queue if we have been asked to
    // or if too many messages have been posted on the queue
    if (synchronize || ++sent_messages_ >= max_outstanding_) {
        flags = flags | IBV_SEND_SIGNALED;
        sent_messages_ = 0;
    }

    // get and update the destination address offset
#if 0
    unsigned long offset;
    std::unique_lock l(l_);
    {
        offset = offset_ + length <= zip::consts::HUGE_PAGE_SIZE ? offset_ : 0;
        offset_ = offset + length;
    }
#else
    auto offset = offset_ + length <= zip::consts::HUGE_PAGE_SIZE ? offset_ : 0;
    offset_ = offset + length;
#endif

    // create the work request
    struct ibv_send_wr wr {
        .wr_id = 0,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = flags,
        .imm_data = static_cast<uint32_t>(index_ | offset * zip::consts::rdma::MAX_QUEUE_PAIRS),
        .wr = {
            .rdma = {
                .remote_addr = reinterpret_cast<uint64_t>(static_cast<uint8_t*>(address_) + offset),
                .rkey = remote_key_
            }
        },
        .qp_type = {
            .xrc = {
                .remote_srqn = xrc_queue_nums_[destination % num_queues_]
            }
        }
    };

    //auto target = (void*)reinterpret_cast<uint64_t>(static_cast<uint8_t*>(address_) + offset);
    //logger.info("send to destination=", destination, ", srqn=", xrc_queue_nums_[destination % num_queues_], " at ", target, " with length=", length);

    // post the send work request
    struct ibv_send_wr *bad_wr;
    ZIP_ASSERT_ZERO(ibv_post_send(send_queue_, &wr, &bad_wr), "could not post send request");

    // if we need to poll for completion then
    // do so until we receive the completion event
    if (flags & IBV_SEND_SIGNALED) {
        while (true) {
            ibv_wc wc;
            if (ibv_poll_cq(completion_queue_, 1, &wc) > 0) {
                // we received a completion event, check it's
                // status and return
                ZIP_ASSERT_EQ(wc.opcode, IBV_WC_RDMA_WRITE, "received unknown network completion");
                ZIP_ASSERT_EQ(wc.status, IBV_WC_SUCCESS, "network error occurred");
                return;
            }
        }
    }
}

} // namespace network
} // namespace zip
