#include "network/manager.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <utility>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include "network/buffer.h"
#include "network/recv_queue.h"
#include "network/send_queue.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/util.h"

namespace zip {
namespace network {

/** create a logger for this file */
static zip::util::logger logger("manager");

manager::manager(std::string device, uint8_t port, int gid): device_port_(port), gid_(gid), server_(false) {
    // query the given network device
    int num_devices = 0;
    auto devices = ibv_get_device_list(&num_devices);

    // find the device by it's name in the list
    while (--num_devices >= 0) {
        if (device == devices[num_devices]->name) break;
    }
    ZIP_ASSERT(num_devices >= 0, "Failed to find RDMA device ", device);

    // get the device and create the
    // context and protection domain
    device_ = devices[num_devices];
    context_ = ibv_open_device(device_);
    ZIP_ASSERT_NOT_NULL(context_, "could not create device context");
    protection_domain_ = ibv_alloc_pd(context_);
    ZIP_ASSERT_NOT_NULL(protection_domain_, "could not create protection domain");

    // query the device attributes and set the port
    ibv_port_attr port_attributes;
    ZIP_ASSERT_ZERO(ibv_query_port(context_, device_port_, &port_attributes), "could not query the given device port");
    local_id_ = port_attributes.lid;

    // verify the port is active
    ZIP_ASSERT_EQ(port_attributes.state, IBV_PORT_ACTIVE, "given device port is not active");
    ZIP_ASSERT(port_attributes.link_layer == IBV_LINK_LAYER_INFINIBAND || gid_ >= 0, "given device is not InfiniBand, need to specify a GID");

    // verify the GID index
    if (gid_ >= 0) {
        ZIP_ASSERT(gid_ < port_attributes.gid_tbl_len, "invalid GID index");
        ZIP_ASSERT_ZERO(ibv_query_gid(context_, device_port_, gid_, &local_gid_), "could not query given device port GID");
    }

    // create the XRC domain
    ibv_xrcd_init_attr xrcd_attributes {
        .comp_mask = IBV_XRCD_INIT_ATTR_FD | IBV_XRCD_INIT_ATTR_OFLAGS,
        .fd = -1,
        .oflags = O_CREAT
    };
    xrc_domain_ = ibv_open_xrcd(context_, &xrcd_attributes);
    ZIP_ASSERT_NOT_NULL(xrc_domain_, "could not create XRC domain");

    // preallocate buffers for the free list
    for (unsigned long index = 0; index < zip::consts::NUM_BUFFER_SIZES; index++) {
        allocate_buffers(index);
    }

    // mark all receive buffer indices as free
    free_receive_buffers_.fill(true);
}

void manager::bind_server(uint16_t port) {
    // set the server switch
    auto lock = std::unique_lock(lock_);
    server_ = true;

    // create a socket to listen on
    socket_ = socket(AF_INET, SOCK_STREAM, 0);
    ZIP_ASSERT(socket_ >= 0, "could not open server socket");

    // create the address to bind to
    sockaddr_in address;
    memset(&(address), 0, sizeof(sockaddr_in));
    address.sin_family = AF_INET;
    address.sin_port = htons(port);

    // set socket options
    int enabled = 1;
    ZIP_ASSERT_ZERO(setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(int)), "could not set socket option to reuse address");
    ZIP_ASSERT_ZERO(fcntl(socket_, F_SETFL, O_NONBLOCK), "could not set socket option to non-blocking");

    // bind to the address and port
    ZIP_ASSERT_ZERO(bind(socket_, reinterpret_cast<sockaddr*>(&address), sizeof(sockaddr_in)), "could not bind to local address and port");

    // listen on the socket
    ZIP_ASSERT_ZERO(listen(socket_, 128), "could not listen on server socket");

    // setup the poll fd for the socket
    poll_ = {
        .fd = socket_,
        .events = POLLIN
    };

    // make the signal empty
    sigemptyset(&signal_);
}

manager::~manager() {
    // deregister memory regions and free the underlying pages
    for (auto& mr: page_list_) {
        auto page = mr->addr;
        ZIP_ASSERT_ZERO(ibv_dereg_mr(mr), "could not destroy memory region");
        std::free(page);
    }

    // deallocate the XRC domain, the protection domain and destroy the device
    ZIP_ASSERT_ZERO(ibv_close_xrcd(xrc_domain_), "could not destroy XRC domain");
    ZIP_ASSERT_ZERO(ibv_dealloc_pd(protection_domain_), "could not destroy protection domain");
    ZIP_ASSERT_ZERO(ibv_close_device(context_), "could not destroy device context");
}

std::list<buffer> manager::get_buffers(unsigned long length, unsigned long num_buffers) {
    // figure out the size index of the length requested
    ZIP_ASSERT(length <= zip::consts::BUFFER_SIZES.back(), "could not allocate buffers of given size");
    auto index = zip::util::size_index(length);

    // allocate a buffer of that length if available, otherwise,
    // allocate a new page and return the newly allocated buffers
    auto lock = std::unique_lock(lock_);
    while (free_list_[index].size() < num_buffers) {
        allocate_buffers(index);
    }

    // create a resulting list
    std::list<buffer> result;
    auto& list = free_list_[index];

    // add the requisite number of buffers to the list
    auto it = list.begin();
    for (unsigned long i = 0; i < num_buffers; i++, it++) {
        result.emplace_back(it->first, it->second, index, *this);
    }

    // erase the elements from the free list and return
    list.erase(list.begin(), it);
    return result;
}

void manager::put_buffer(void* buffer, uint32_t local_key, unsigned long index) {
    // put the buffer back in free list
    auto lock = std::unique_lock(lock_);
    free_list_[index].emplace_back(buffer, local_key);
}

void manager::allocate_buffers(unsigned long index) {
    // allocate the huge page
    auto page = zip::util::allocate_huge_page(zip::consts::HUGE_PAGE_SIZE);

    // register the allocated memory with the network device
    auto flags = IBV_ACCESS_LOCAL_WRITE;
    auto mr = ibv_reg_mr(protection_domain_, page, zip::consts::HUGE_PAGE_SIZE, flags);
    ZIP_ASSERT_NOT_NULL(mr, "could not register memory region");
    page_list_.emplace_back(mr);

    // create the buffers and add to free list
    auto length = zip::consts::BUFFER_SIZES[index];
    for (unsigned long j = 0; j < zip::consts::HUGE_PAGE_SIZE; j += length) {
        auto buffer = static_cast<void*>(static_cast<uint8_t*>(page) + j);
        free_list_[index].emplace_back(buffer, mr->lkey);
    }
}

std::tuple<ibv_qp*, ibv_qp*, ibv_cq*> manager::create_queues() {
    // create the completion queue
    auto completion_queue = ibv_create_cq(context_, zip::consts::rdma::MAX_OUTSTANDING_REQUESTS, nullptr, nullptr, 0);
    ZIP_ASSERT_NOT_NULL(completion_queue, "could not create completion queue");

    // set send queue initialisation attributes
    ibv_qp_init_attr_ex init_attributes {
        .send_cq = completion_queue,
        .cap = {
            .max_send_wr = zip::consts::rdma::MAX_OUTSTANDING_REQUESTS,
            .max_send_sge = zip::consts::rdma::MAX_SCATTER_GATHER_ELEMENTS,
            .max_inline_data = zip::consts::rdma::MAX_INLINE_DATA
        },
        .qp_type = IBV_QPT_XRC_SEND,
        .sq_sig_all = 0,
        .comp_mask = IBV_QP_INIT_ATTR_PD,
        .pd = protection_domain_
    };

    // allocate the send queue
    auto send_queue = ibv_create_qp_ex(context_, &init_attributes);
    ZIP_ASSERT_NOT_NULL(send_queue, "could not create send queue");

    // set recv queue initialisation attributes
    init_attributes = {
        .qp_type = IBV_QPT_XRC_RECV,
        .comp_mask = IBV_QP_INIT_ATTR_XRCD,
        .xrcd = xrc_domain_
    };

    // allocate the recv queue
    auto recv_queue = ibv_create_qp_ex(context_, &init_attributes);
    ZIP_ASSERT_NOT_NULL(recv_queue, "could not create receive queue");

    // set queue pair attributes
    unsigned int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_RELAXED_ORDERING;
    ibv_qp_attr attributes {
        .qp_state = IBV_QPS_INIT,
        .qp_access_flags = access,
        .pkey_index = 0,
        .port_num = device_port_
    };

    // initialise the send and receive queues
    auto flags = IBV_QP_STATE | IBV_QP_PORT | IBV_QP_PKEY_INDEX | IBV_QP_ACCESS_FLAGS;
    ZIP_ASSERT_ZERO(ibv_modify_qp(send_queue, &attributes, flags), "could not mark send queue as INIT");
    ZIP_ASSERT_ZERO(ibv_modify_qp(recv_queue, &attributes, flags), "could not mark receive queue as INIT");

    // return the queues
    return {send_queue, recv_queue, completion_queue};
}

std::tuple<unsigned long, void*, uint32_t> manager::create_receive_buffer() {
    // find a free index for the queue pair buffer
    auto lock = std::unique_lock(lock_);
    unsigned long index = 0;
    for (; index < zip::consts::rdma::MAX_QUEUE_PAIRS; index++) {
        if (free_receive_buffers_[index]) {
            free_receive_buffers_[index] = false;
            break;
        }
    }
    ZIP_ASSERT_NEQ(index, zip::consts::rdma::MAX_QUEUE_PAIRS, "cannot create more queue pair receive buffers");

    // if the index does not already have an associated page
    if (receive_buffers_[index] == nullptr) {
        // create a new hugepage as a receive buffer
        // and register it with the network device
        auto flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_RELAXED_ORDERING;
        auto page = zip::util::allocate_huge_page(zip::consts::HUGE_PAGE_SIZE);
        auto mr = ibv_reg_mr(protection_domain_, page, zip::consts::HUGE_PAGE_SIZE, flags);
        ZIP_ASSERT_NOT_NULL(mr, "could not register memory region");

        // save the page in the state
        page_list_.emplace_back(mr);
        receive_buffers_[index] = mr;
    }

    // return the index, address and the remote key
    auto mr = receive_buffers_[index];
    return {index, mr->addr, mr->rkey};
}

void manager::free_receive_buffer(unsigned long index) {
    // mark the index as available
    auto lock = std::unique_lock(lock_);
    free_receive_buffers_[index] = true;
}

std::vector<recv_queue> manager::create_recv_queues(unsigned long num_queues) {
    // create a vector for receive queues
    auto lock = std::unique_lock(lock_);
    std::vector<recv_queue> queues;

    // create the given number of receive queues
    while (num_queues-- > 0) {
        // create the completion queue
        ibv_cq* completion_queue = ibv_create_cq(context_, zip::consts::rdma::MAX_RECEIVE_REQUESTS, nullptr, nullptr, 0);
        ZIP_ASSERT_NOT_NULL(completion_queue, "could not create completion queue");

        // set shared receive queue initialisation attributes
        ibv_srq_init_attr_ex attributes {
            .attr = {
                .max_wr = zip::consts::rdma::MAX_RECEIVE_REQUESTS,
                .max_sge = zip::consts::rdma::MAX_SCATTER_GATHER_ELEMENTS
            },
            .comp_mask = IBV_SRQ_INIT_ATTR_TYPE | IBV_SRQ_INIT_ATTR_XRCD | IBV_SRQ_INIT_ATTR_CQ | IBV_SRQ_INIT_ATTR_PD,
            .srq_type = IBV_SRQT_XRC,
            .pd = protection_domain_,
            .xrcd = xrc_domain_,
            .cq = completion_queue
        };

        // allocate the shared receive queue
        auto shared_recv_queue = ibv_create_srq_ex(context_, &attributes);
        ZIP_ASSERT_NOT_NULL(shared_recv_queue, "could not create shared receive queue");

        // save the shared receive queue number
        uint32_t xrc_queue_num;
        ZIP_ASSERT_ZERO(ibv_get_srq_num(shared_recv_queue, &xrc_queue_num), "could not obtain shared receive queue number");
        xrc_queue_nums_.emplace_back(xrc_queue_num);

        // create the receive queue object
        queues.emplace_back(shared_recv_queue, completion_queue, receive_buffers_);
    }

    // return the created queues
    return queues;
}

namespace detail {

/**
 * A struct that contains the infomation for
 * a single queue pair.
 */
struct __attribute__((packed)) queue_info {

    unsigned long index;          /** index of the receive buffer */
    void*         address;        /** address of the receive buffer */
    uint32_t      remote_key;     /** remote key for accessing the network receive buffer */
    uint32_t      send_queue_num; /** send queue number */
    uint32_t      recv_queue_num; /** receive queue number */
    uint32_t      psn;            /** PSN for this queue pair */

    /** Simple constructor to set the values. */
    queue_info(unsigned long index, void* address, uint32_t remote_key, uint32_t send_queue_num, uint32_t recv_queue_num, uint32_t psn):
    index(index), address(address), remote_key(remote_key), send_queue_num(send_queue_num), recv_queue_num(recv_queue_num), psn(psn) {}

};

/**
 * A struct to exchange destination information.
 */
struct __attribute__((packed)) destination_info {

    uint16_t local_id;       /** local device ID */
    ibv_gid  local_gid;      /** local device port GID */
    uint64_t num_queues;     /** number of send/receive queues in this struct */
    uint64_t num_xrc_queues; /** number of XRC queue numbers in this struct */
    uint64_t message_length; /** handshake message length */
    uint8_t  rest[0];        /** beginning of the rest of the serialised message */

    /**
     * Initialise a destination info struct.
     */
    destination_info(
        uint16_t local_id,
        ibv_gid local_gid,
        std::vector<queue_info>& queue_infos,
        std::vector<uint32_t>& xrc_queue_nums,
        void* message,
        unsigned long length
    ): local_id(local_id), local_gid(local_gid), num_queues(queue_infos.size()),
       num_xrc_queues(xrc_queue_nums.size()), message_length(length) {
        // serialise the arguments into the right location
        std::memcpy(this->queue_infos(), queue_infos.data(), num_queues * sizeof(queue_info));
        std::memcpy(this->xrc_queue_nums(), xrc_queue_nums.data(), num_xrc_queues * sizeof(uint32_t));
        std::memcpy(this->message(), message, message_length);
    }

    /**
     * Return the size of the struct.
     *
     * @param num_queue_pairs number of queue pairs
     * @param message_length  length of the handshake message
     *
     * @return size of this struct
     */
    static inline unsigned long length(unsigned long num_queues, unsigned long num_xrc_queues, unsigned long message_length) {
        return sizeof(destination_info) + num_queues * sizeof(queue_info) + num_xrc_queues * sizeof(uint32_t) + message_length;
    }

    /**
     * Return the beginning of queue infos.
     *
     * @return the pointer
     */
    inline queue_info* queue_infos() {
        return reinterpret_cast<queue_info*>(rest);
    }

    /**
     * Return the beginning of XRC queue numbers.
     *
     * @return the pointer
     */
    inline uint32_t* xrc_queue_nums() {
        return reinterpret_cast<uint32_t*>(queue_infos() + num_queues);
    }

    /**
     * Return the beginning of the message.
     *
     * @return the pointer
     */
    inline void* message() {
        return static_cast<void*>(xrc_queue_nums() + num_xrc_queues);
    }

} __attribute__((packed));

/**
 * Serialise connection information into the given buffer.
 *
 * @param buffer      the buffer to serialise into
 * @param local_id    the local device ID
 * @param local_gid   the local device port GID
 * @param queue_infos the information for the queues
 * @param message     handshake message to send
 * @param length      the length of the handshake message
 *
 * @return the size of the serialised message
 */
uint64_t serialise_connection(
    zip::util::unique_ptr_malloc<destination_info>& buffer,
    uint16_t local_id,
    ibv_gid local_gid,
    std::vector<queue_info>& queue_infos,
    std::vector<uint32_t>& xrc_queue_nums,
    void* message,
    unsigned long length
) {
    // calculate the size of the serialised message
    auto serialised_size = destination_info::length(queue_infos.size(), xrc_queue_nums.size(), length);
    buffer = zip::util::malloc_unique<destination_info>(serialised_size);

    // create the struct to be serialised
    new (buffer.get()) destination_info(
        local_id,
        local_gid,
        queue_infos,
        xrc_queue_nums,
        message,
        length
    );

    // return the size of the allocated buffer
    return serialised_size;
}

/**
 * Deserialise connection information from the given buffer.
 *
 * @param buffer the buffer to deserialise from
 * @param length length of the received buffer
 *
 * @return local device ID, local device port, local device GID, queue pair
 *         information, the handshake message received and it's length
 */
std::tuple<uint16_t, ibv_gid, std::vector<queue_info>, std::vector<uint32_t>, void*, unsigned long>
deserialise_connection(zip::util::unique_ptr_malloc<destination_info>& buffer, uint64_t length) {
    // verify the length of the serialised buffer
    auto serialised_size = destination_info::length(buffer->num_queues, buffer->num_xrc_queues, buffer->message_length);
    ZIP_ASSERT_EQ(length, serialised_size, "incorrect size of the message to serialise");

    // get the bufferrmation from the serialised struct
    auto queue_infos = std::vector<queue_info>(buffer->queue_infos(), buffer->queue_infos() + buffer->num_queues);
    auto xrc_queue_nums = std::vector<uint32_t>(buffer->xrc_queue_nums(), buffer->xrc_queue_nums() + buffer->num_xrc_queues);

    // return as a tuple
    return {
        buffer->local_id,
        buffer->local_gid,
        queue_infos,
        xrc_queue_nums,
        buffer->message(),
        buffer->message_length
    };
}

/**
 * Activate the send and receive queues with the given parameters.
 *
 * @param send_queue       the send queue
 * @param recv_queue       the receive queue
 * @param local_gid        local device port GID index
 * @param local_info       information of the local queue
 * @param remove_local_id  remote local device ID
 * @prarm remote_local_gid remote local device port GID
 * @param remote_port      remote device port
 * @param remote_info      information of the remote queue
 */
void activate_queues(
    ibv_qp* send_queue,
    ibv_qp* recv_queue,
    uint8_t local_port,
    int local_gid,
    queue_info local_info,
    uint16_t remote_id,
    ibv_gid remote_gid,
    queue_info remote_info
) {
    // create the attribute to mark the receive queue as RTR
    ibv_qp_attr attribute = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_4096,
        .rq_psn = remote_info.psn,
        .dest_qp_num = remote_info.send_queue_num,
        .ah_attr = {
            .dlid = remote_id,
            .is_global = local_gid >= 0,
            .port_num = local_port
        },
        .min_rnr_timer = 12
    };

    // set the GID that was requested
    if (local_gid >= 0) {
        attribute.ah_attr.grh = {
            .dgid = remote_gid,
            .sgid_index = static_cast<uint8_t>(local_gid),
            .hop_limit = 1
        };
    }

    // set the receive queue to RTR state
    auto flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MIN_RNR_TIMER | IBV_QP_MAX_DEST_RD_ATOMIC;
    ZIP_ASSERT_ZERO(ibv_modify_qp(recv_queue, &attribute, flags), "could not mark receive queue as RTR");

    // create the attribute to mark the receive queue as RTS
    attribute = {
        .qp_state = IBV_QPS_RTS,
        .sq_psn = local_info.psn,
        .timeout = 14
    };

    // set the receive queue to RTS state
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_SQ_PSN;
    ZIP_ASSERT_ZERO(ibv_modify_qp(recv_queue, &attribute, flags), "could not mark receive queue as RTS");

    // create the attribute to mark the send queue as RTR
    attribute = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_4096,
        .rq_psn = remote_info.psn,
        .dest_qp_num = remote_info.recv_queue_num,
        .ah_attr = {
            .dlid = remote_id,
            .is_global = local_gid >= 0,
            .port_num = local_port
        }
    };

    // set the GID that was requested
    if (local_gid >= 0) {
        attribute.ah_attr.grh = {
            .dgid = remote_gid,
            .sgid_index = static_cast<uint8_t>(local_gid),
            .hop_limit = 1
        };
    }

    // set the send queue to RTR state
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
    ZIP_ASSERT_ZERO(ibv_modify_qp(send_queue, &attribute, flags), "could not mark send queue as RTR");

    // create the attribute to mark the receive queue as RTS
    attribute = {
        .qp_state = IBV_QPS_RTS,
        .sq_psn = local_info.psn,
        .timeout = 14,
        .retry_cnt = 7,
        .rnr_retry = 7
    };

    // set the receive queue to RTS state
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_SQ_PSN | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    ZIP_ASSERT_ZERO(ibv_modify_qp(send_queue, &attribute, flags), "could not mark send queue as RTS");
}

/**
 * Send data to the given socket.
 *
 * @param socket socket to send data to
 * @param data   buffer that contains data
 * @param length length of the data to send
 */
void send(int socket, void* data, unsigned long length) {
    unsigned long offset = 0;
    while (offset < length) {
        auto sent = ::send(socket, static_cast<char*>(data) + offset, length - offset, 0);
        ZIP_ASSERT(sent > 0, "could not send data");
        offset += sent;
    }
}

/**
 * Receive data from the given socket.
 *
 * @param socket socket to receive data from
 * @param data   buffer to put data in
 * @param length length of the data to receive
 */
void recv(int socket, void* data, unsigned long length) {
    unsigned long offset = 0;
    while (offset < length) {
        auto received = ::recv(socket, static_cast<char*>(data) + offset, length - offset, 0);
        ZIP_ASSERT(received > 0, "could not receive data");
        offset += received;
    }
}

} // namespace detail

std::optional<std::tuple<send_queue, zip::util::unique_ptr_malloc<void>, unsigned long, sockaddr_in>>
manager::accept(void* message, unsigned long length) {
    // try create the queue and check if it was successful
    auto result = accept(message, length, 1);
    if (!result) return {};

    // otherwise unpack the vector and return the tuple
    auto& [queues, received_message, received_length, address] = *result;
    return {{std::move(queues.front()), std::move(received_message), received_length, address}};
}

std::optional<std::tuple<std::vector<send_queue>, zip::util::unique_ptr_malloc<void>, unsigned long, sockaddr_in>>
manager::accept(void* message, unsigned long length, unsigned long num_queues) {
    // make sure that this manager is a server
    ZIP_ASSERT(server_, "accept called on a non-server manager");

    // create a address location
    sockaddr_in address;
    socklen_t addr_length = sizeof(sockaddr_in);

    // wait to accept an incoming connection
    auto ret = ppoll(&poll_, 1, &zip::consts::ACCEPT_TIMEOUT, &signal_);
    ZIP_ASSERT(ret >= 0 || errno ==  EINTR, "could not poll socket");

    // if there was a timeout or signal, then return empty
    if (ret == 0 || errno == EINTR) return {};

    // try to accept the socket
    ZIP_ASSERT_EQ(poll_.revents, POLLIN, "unknown event received");
    auto socket = ::accept(socket_, reinterpret_cast<sockaddr*>(&address), &addr_length);
    ZIP_ASSERT(socket >= 0, "could not open connection socket");

    // exchange and calculate the number of queue pairs
    uint64_t mine_num_queues = num_queues, other_num_queues;
    detail::recv(socket, &other_num_queues, sizeof(uint64_t));
    detail::send(socket, &mine_num_queues, sizeof(uint64_t));
    if (mine_num_queues == 0 && other_num_queues == 0)
        num_queues = 1;
    else if (mine_num_queues == 0)
        num_queues = other_num_queues;
    else if (other_num_queues == 0)
        num_queues = mine_num_queues;
    else
        num_queues = std::min(mine_num_queues, other_num_queues);

    // create vectors for storing RDMA queues
    std::vector<ibv_qp*> send_queues, recv_queues;
    std::vector<ibv_cq*> completion_queues;
    std::vector<detail::queue_info> queue_infos;

    // create the queues to setup the connection
    // and get random PSNs (24 bits)
    for (unsigned long i = 0; i < num_queues; i++) {
        auto [send_queue, recv_queue, completion_queue] = create_queues();
        auto [index, address, remote_key] = create_receive_buffer();
        queue_infos.emplace_back(index, address, remote_key, send_queue->qp_num, recv_queue->qp_num, std::rand() & ((1 << 24) - 1));
        completion_queues.emplace_back(completion_queue);
        recv_queues.emplace_back(recv_queue);
        send_queues.emplace_back(send_queue);
    }

    // copy local information into the send buffer
    auto lock = std::unique_lock(lock_);
    zip::util::unique_ptr_malloc<detail::destination_info> send_buf;
    auto send_buf_size = detail::serialise_connection(
        send_buf,
        local_id_,
        local_gid_,
        queue_infos,
        xrc_queue_nums_,
        message,
        length
    );

    // release the lock
    lock.unlock();

    // exchange buffer sizes
    uint64_t recv_buf_size;
    detail::recv(socket, &recv_buf_size, sizeof(uint64_t));
    detail::send(socket, &send_buf_size, sizeof(uint64_t));

    // allocate the receive buffer and exchange the data with the other side and close
    auto recv_buf = zip::util::malloc_unique<detail::destination_info>(recv_buf_size);
    detail::recv(socket, recv_buf.get(), recv_buf_size);
    detail::send(socket, send_buf.get(), send_buf_size);
    close(socket);

    // get the information from the recv buffer
    auto [
        remote_id,
        remote_gid,
        remote_queue_infos,
        xrc_queue_nums,
        received_message,
        received_length
    ] = detail::deserialise_connection(recv_buf, recv_buf_size);

    // activate the queues with the received information
    for (unsigned long i = 0; i < num_queues; i++) {
        detail::activate_queues(
            send_queues[i],
            recv_queues[i],
            device_port_,
            gid_,
            queue_infos[i],
            remote_id,
            remote_gid,
            remote_queue_infos[i]
        );
    }

    // create the send queue objects
    std::vector<zip::network::send_queue> queues;
    for (unsigned long i = 0; i < num_queues; i++) {
        queues.emplace_back(
            send_queues[i],
            recv_queues[i],
            completion_queues[i],
            xrc_queue_nums,
            remote_queue_infos[i].index,
            remote_queue_infos[i].address,
            remote_queue_infos[i].remote_key,
            *this
        );
    }

    // copy the handshake message
    auto handshake = zip::util::malloc_unique<void>(received_length);
    std::memcpy(handshake.get(), received_message, received_length);

    // return the queues and the handshake message and the address
    return {{std::move(queues), std::move(handshake), received_length, address}};
}

std::tuple<send_queue, zip::util::unique_ptr_malloc<void>, unsigned long>
manager::connect(std::string server_ip, uint16_t port, void* message, unsigned long length) {
    // create the queue and return the unpacked the vector
    auto [queues, received_message, received_length] = connect(server_ip, port, message, length, 1);
    return {std::move(queues.front()), std::move(received_message), received_length};
}

std::tuple<std::vector<send_queue>, zip::util::unique_ptr_malloc<void>, unsigned long>
manager::connect(std::string server_ip, uint16_t port, void* message, unsigned long length, unsigned long num_queues) {
    // create the address of the server
    sockaddr_in address;
    std::memset(&address, 0, sizeof(sockaddr_in));
    address.sin_family = AF_INET;
    inet_pton(AF_INET, server_ip.c_str(), &address.sin_addr);
    address.sin_port = htons(port);

    // connect to the server
    return connect(address, message, length, num_queues);
}

std::tuple<send_queue, zip::util::unique_ptr_malloc<void>, unsigned long>
manager::connect(sockaddr_in address, void* message, unsigned long length) {
    // create the queue and return the unpacked the vector
    auto [queues, received_message, received_length] = connect(address, message, length, 1);
    return {std::move(queues.front()), std::move(received_message), received_length};
}

std::tuple<std::vector<send_queue>, zip::util::unique_ptr_malloc<void>, unsigned long>
manager::connect(sockaddr_in address, void* message, unsigned long length, unsigned long num_queues) {
    // create the socket and connect to the server
    auto socket = ::socket(AF_INET, SOCK_STREAM, 0);
    ZIP_ASSERT(socket >= 0, "could not create connection socket");
    ZIP_ASSERT_ZERO(::connect(socket, reinterpret_cast<sockaddr*>(&address), sizeof(sockaddr_in)), "could not connect to the server");

    // exchange and calculate the number of queue pairs
    uint64_t mine_num_queues = num_queues, other_num_queues;
    detail::send(socket, &mine_num_queues, sizeof(uint64_t));
    detail::recv(socket, &other_num_queues, sizeof(uint64_t));
    if (mine_num_queues == 0 && other_num_queues == 0)
        num_queues = 1;
    else if (mine_num_queues == 0)
        num_queues = other_num_queues;
    else if (other_num_queues == 0)
        num_queues = mine_num_queues;
    else
        num_queues = std::min(mine_num_queues, other_num_queues);

    // create vectors for storing RDMA queues
    std::vector<ibv_qp*> send_queues, recv_queues;
    std::vector<ibv_cq*> completion_queues;
    std::vector<detail::queue_info> queue_infos;

    // create the queues to setup the connection
    // and get random PSNs (24 bits)
    for (unsigned long i = 0; i < num_queues; i++) {
        auto [send_queue, recv_queue, completion_queue] = create_queues();
        auto [index, address, remote_key] = create_receive_buffer();
        queue_infos.emplace_back(index, address, remote_key, send_queue->qp_num, recv_queue->qp_num, std::rand() & ((1 << 24) - 1));
        completion_queues.emplace_back(completion_queue);
        recv_queues.emplace_back(recv_queue);
        send_queues.emplace_back(send_queue);
    }

    // copy local information into the send buffer
    auto lock = std::unique_lock(lock_);
    zip::util::unique_ptr_malloc<detail::destination_info> send_buf;
    auto send_buf_size = detail::serialise_connection(
        send_buf,
        local_id_,
        local_gid_,
        queue_infos,
        xrc_queue_nums_,
        message,
        length
    );

    // release the lock
    lock.unlock();

    // exchange buffer sizes
    uint64_t recv_buf_size;
    detail::send(socket, &send_buf_size, sizeof(uint64_t));
    detail::recv(socket, &recv_buf_size, sizeof(uint64_t));

    // allocate the receive buffer and exchange the data with the other side and close
    auto recv_buf = zip::util::malloc_unique<detail::destination_info>(recv_buf_size);
    detail::send(socket, send_buf.get(), send_buf_size);
    detail::recv(socket, recv_buf.get(), recv_buf_size);
    close(socket);

    // get the information from the recv buffer
    auto [
        remote_id,
        remote_gid,
        remote_queue_infos,
        xrc_queue_nums,
        received_message,
        received_length
    ] = detail::deserialise_connection(recv_buf, recv_buf_size);

    // activate the queues with the received information
    for (unsigned long i = 0; i < num_queues; i++) {
        detail::activate_queues(
            send_queues[i],
            recv_queues[i],
            device_port_,
            gid_,
            queue_infos[i],
            remote_id,
            remote_gid,
            remote_queue_infos[i]
        );
    }

    // create the send queue objects
    std::vector<zip::network::send_queue> queues;
    for (unsigned long i = 0; i < num_queues; i++) {
        queues.emplace_back(
            send_queues[i],
            recv_queues[i],
            completion_queues[i],
            xrc_queue_nums,
            remote_queue_infos[i].index,
            remote_queue_infos[i].address,
            remote_queue_infos[i].remote_key,
            *this
        );
    }

    // copy the handshake message
    auto handshake = zip::util::malloc_unique<void>(received_length);
    std::memcpy(handshake.get(), received_message, received_length);

    // return the queues and the handshake message and the address
    return {std::move(queues), std::move(handshake), received_length};
}

} // namespace network
} // namespace zip
