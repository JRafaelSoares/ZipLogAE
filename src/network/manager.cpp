#include "network/manager.h"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
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

namespace zip::network {

/** create a logger for this file */
static zip::util::logger logger("manager");

manager::manager(std::string device, uint8_t port, int gid, int numa):
device_port_(port), gid_(gid), server_(false) {
    // query the given network device
    int num_devices = 0;
    auto devices = ibv_get_device_list(&num_devices);

    // find the device by its name in the list
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

    // try to get the NUMA node for the device
    auto path = std::filesystem::path(device_->ibdev_path) / "device/numa_node";
    auto file = std::ifstream(path);
    int device_numa = -1; file >> device_numa;

    // set the NUMA node to use for memory allocation
    numa_ = (numa == -1) ? device_numa : numa;

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

    // free the devices list
    ibv_free_device_list(devices);

    // mark all receive buffer indices as free
    free_receive_buffers_.set();

    // start the thread to poll for RDMA events
    event_thread_ = std::jthread(std::bind_front(&manager::poll_events, this));
}

void manager::poll_events(std::stop_token stop_token) {
    // change the blocking mode of the async event fd
    auto flags = fcntl(context_->async_fd, F_GETFL);
    ZIP_ASSERT_ZERO(fcntl(context_->async_fd, F_SETFL, flags | O_NONBLOCK), "failed to set file descriptor parameters");

    // setup the signal mask for the `poll` call
    sigset_t signal;
    sigemptyset(&signal);

    // setup the poll fd for the async event fd
    pollfd poll {
        .fd = context_->async_fd,
        .events = POLLIN
    };

    // placeholder for the async event
    ibv_async_event event;

    // poll for an event and append it to the list
    while (!stop_token.stop_requested()) {
        // wait until an event occurs
        auto ret = ppoll(&poll, 1, &zip::consts::POLL_TIMEOUT, &signal);
        ZIP_ASSERT(ret >= 0 || errno ==  EINTR, "could not poll async event fd");

        // if there was a timeout or signal, then continue
        if (ret == 0 || errno == EINTR) continue;

        // try to accept the socket
        if (ibv_get_async_event(context_, &event) == 0) {
            logger.info("Get event: ", event.event_type);
            events_.enqueue(event);
            ibv_ack_async_event(&event);
        }
    }
}

void manager::bind_server(uint16_t port) {
    // set the server switch
    auto lock = std::unique_lock(server_lock_);
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
    // stop the error event thread
    event_thread_.request_stop();
    event_thread_.join();

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

std::vector<buffer> manager::get_buffers(unsigned long num_buffers) {
    // return buffers if available, otherwise, allocate a
    // new page and return the newly allocated buffers
    auto lock = std::unique_lock(memory_lock_);
    while (free_list_.size() < num_buffers) {
        // create a new huge page as a send buffer
        // and register it with the network device
        auto page = zip::util::allocate_huge_page(zip::consts::HUGE_PAGE_SIZE, numa_);
        auto flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_RELAXED_ORDERING;
        auto mr = ibv_reg_mr(protection_domain_, page, zip::consts::HUGE_PAGE_SIZE, flags);
        ZIP_ASSERT_NOT_NULL(mr, "could not register memory region");
        page_list_.emplace_back(mr);

        // create the send buffers and add to free list
        for (unsigned long j = 0; j < zip::consts::HUGE_PAGE_SIZE; j += zip::consts::SEND_BUFFER_SIZE) {
            free_list_.emplace_back(static_cast<uint8_t*>(page) + j, mr->lkey);
        }
    }

    // create a resulting list
    std::vector<buffer> result;

    // add the requisite number of buffers to the list
    auto it = free_list_.begin();
    for (unsigned long i = 0; i < num_buffers; i++, it++) {
        result.emplace_back(it->first, it->second, *this);
    }

    // erase the elements from the free list and return
    free_list_.erase(free_list_.begin(), it);
    return result;
}

void manager::put_buffer(void* buffer, uint32_t local_key) {
    // put the buffer back in free list
    auto lock = std::unique_lock(memory_lock_);
    free_list_.emplace_back(buffer, local_key);
}

std::tuple<ibv_qp*, ibv_qp*, ibv_cq*> manager::create_queues() {
    // create the completion queue
    auto completion_queue = ibv_create_cq(context_, 1, nullptr, nullptr, 0);
    ZIP_ASSERT_NOT_NULL(completion_queue, "could not create completion queue");

    // set send queue initialization attributes
    ibv_qp_init_attr_ex init_attributes {
        .send_cq = completion_queue,
        .cap = {
            .max_send_wr = zip::consts::rdma::MAX_OUTSTANDING_REQUESTS,
            .max_send_sge = 1,
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

    // set recv queue initialization attributes
    init_attributes = {
        .qp_type = IBV_QPT_XRC_RECV,
        .comp_mask = IBV_QP_INIT_ATTR_XRCD,
        .xrcd = xrc_domain_
    };

    // allocate the recv queue
    auto recv_queue = ibv_create_qp_ex(context_, &init_attributes);
    ZIP_ASSERT_NOT_NULL(recv_queue, "could not create receive queue");

    // set queue pair attributes
    unsigned int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    ibv_qp_attr attributes {
        .qp_state = IBV_QPS_INIT,
        .qp_access_flags = access,
        .pkey_index = 0,
        .port_num = device_port_
    };

    // initialize the send and receive queues
    auto flags = IBV_QP_STATE | IBV_QP_PORT | IBV_QP_PKEY_INDEX | IBV_QP_ACCESS_FLAGS;
    ZIP_ASSERT_ZERO(ibv_modify_qp(send_queue, &attributes, flags), "could not mark send queue as INIT");
    ZIP_ASSERT_ZERO(ibv_modify_qp(recv_queue, &attributes, flags), "could not mark receive queue as INIT");

    // return the queues
    return {send_queue, recv_queue, completion_queue};
}

std::tuple<unsigned long, void*, uint32_t> manager::create_receive_buffer() {
    // find a free index for the queue pair buffer
    auto lock = std::unique_lock(memory_lock_);
    unsigned long index = free_receive_buffers_._Find_first();
    ZIP_ASSERT_NEQ(index, zip::consts::rdma::MAX_QUEUE_PAIRS, "cannot create more queue pair receive buffers");

    // if the index does not already have an associated page
    if (receive_buffers_[index] == nullptr) {
        // create a new huge page as a receive buffer
        // and register it with the network device
        auto page = zip::util::allocate_huge_page(zip::consts::HUGE_PAGE_SIZE, numa_);
        auto flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_RELAXED_ORDERING;
        auto mr = ibv_reg_mr(protection_domain_, page, zip::consts::HUGE_PAGE_SIZE, flags);
        ZIP_ASSERT_NOT_NULL(mr, "could not register memory region");
        page_list_.emplace_back(mr);

        // create the receive buffers
        for (unsigned long i = 0, j = 0; j < zip::consts::HUGE_PAGE_SIZE; i++, j += zip::consts::RECEIVE_BUFFER_SIZE) {
            receive_buffers_[index + i] = static_cast<uint8_t*>(page) + j;
            receive_keys_[index + i] = mr->rkey;
        }
    }

    // mark the index as used, and return the index, address and the remote key
    free_receive_buffers_.reset(index);
    return {index, receive_buffers_[index], receive_keys_[index]};
}

void manager::deactivate_srq(recv_queue& queue) {
    ZIP_ASSERT_ZERO(ibv_destroy_srq(queue.recv_queue_), "could not destroy SRQ");
}

void manager::free_recv_buffer(unsigned long index) {
    // mark the index as available
    auto lock = std::unique_lock(memory_lock_);
    free_receive_buffers_.set(index);
}

std::vector<recv_queue> manager::create_recv_queues(unsigned long num_queues) {
    // create a vector for receive queues
    auto lock = std::unique_lock(xrc_lock_);
    std::vector<recv_queue> queues;

    // create the given number of receive queues
    while (num_queues-- > 0) {
        // create the completion queue
        ibv_cq* completion_queue = ibv_create_cq(context_, zip::consts::rdma::MAX_RECEIVE_REQUESTS, nullptr, nullptr, 0);
        ZIP_ASSERT_NOT_NULL(completion_queue, "could not create completion queue");

        // set shared receive queue initialization attributes
        ibv_srq_init_attr_ex attributes {
            .attr = {
                .max_wr = zip::consts::rdma::MAX_RECEIVE_REQUESTS,
                .max_sge = 1
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
 * A struct that contains the information for
 * a single queue pair.
 */
struct queue_info {

    unsigned long index;          /** index of the receive buffer */
    void*         address;        /** address of the receive buffer */
    uint32_t      remote_key;     /** remote key for accessing the network receive buffer */
    uint32_t      send_queue_num; /** send queue number */
    uint32_t      recv_queue_num; /** receive queue number */

};

/**
 * A struct to exchange destination information.
 */
struct destination_info {

    uint16_t local_id;       /** local device ID */
    ibv_gid  local_gid;      /** local device port GID */
    //alignas(8) ibv_gid  local_gid;      /** local device port GID */
    uint64_t num_queues;     /** number of send/receive queues in this struct */
    uint64_t num_xrc_queues; /** number of XRC queue numbers in this struct */
    uint64_t message_length; /** handshake message length */
    uint8_t  rest[0];        /** beginning of the rest of the serialized message */

    /**
     * Initialize a destination info struct.
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
        // serialize the arguments into the right location
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

};

/**
 * Serialize connection information into the given buffer.
 *
 * @param local_id    the local device ID
 * @param local_gid   the local device port GID
 * @param queue_infos the information for the queues
 * @param message     handshake message to send
 * @param length      the length of the handshake message
 *
 * @return the serialized message and its size
 */
std::pair<std::unique_ptr<void, decltype(std::free)*>, uint64_t> serialize_connection(
    uint16_t local_id,
    ibv_gid local_gid,
    std::vector<queue_info>& queue_infos,
    std::vector<uint32_t>& xrc_queue_nums,
    void* message,
    unsigned long length
) {
    // calculate the size of the serialized message
    auto serialized_size = destination_info::length(queue_infos.size(), xrc_queue_nums.size(), length);
    auto buffer = std::unique_ptr<void, decltype(std::free)*>(std::malloc(serialized_size), std::free);

    // create the struct to be serialized
    new (buffer.get()) destination_info(
        local_id,
        local_gid,
        queue_infos,
        xrc_queue_nums,
        message,
        length
    );

    // return the buffer and its size
    return {std::move(buffer), serialized_size};
}

/**
 * Deserialize connection information from the given buffer.
 *
 * @param buffer the buffer to deserialize from
 * @param length length of the received buffer
 *
 * @return local device ID, local device port, local device GID, queue pair
 *         information, the handshake message received and its length
 */
std::tuple<uint16_t, ibv_gid, std::vector<queue_info>, std::vector<uint32_t>, void*, unsigned long>
deserialize_connection(std::unique_ptr<void, decltype(std::free)*>& buffer, uint64_t length) {
    // verify the length of the serialized buffer
    auto info = static_cast<destination_info*>(buffer.get());
    auto serialized_size = destination_info::length(info->num_queues, info->num_xrc_queues, info->message_length);
    ZIP_ASSERT_EQ(length, serialized_size, "incorrect size of the message to deserialize");

    // get the information from the serialized struct
    auto queue_infos = std::vector<queue_info>(info->queue_infos(), info->queue_infos() + info->num_queues);
    auto xrc_queue_nums = std::vector<uint32_t>(info->xrc_queue_nums(), info->xrc_queue_nums() + info->num_xrc_queues);

    // return as a tuple
    return {
        info->local_id,
        info->local_gid,
        queue_infos,
        xrc_queue_nums,
        info->message(),
        info->message_length
    };
}

/**
 * Activate the send and receive queues with the given parameters.
 *
 * @param send_queue       the send queue
 * @param recv_queue       the receive queue
 * @param local_gid        local device port GID index
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
    uint16_t remote_id,
    ibv_gid remote_gid,
    queue_info remote_info
) {
    // create the attribute to mark the receive queue as RTR
    ibv_qp_attr attribute {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_4096,
        .dest_qp_num = remote_info.send_queue_num,
        .ah_attr = {
            .dlid = remote_id,
            .is_global = local_gid >= 0,
            .port_num = local_port
        },
        .min_rnr_timer = zip::consts::rdma::RNR_TIMER
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
        .timeout = zip::consts::rdma::TIMEOUT
    };

    // set the receive queue to RTS state
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_SQ_PSN;
    ZIP_ASSERT_ZERO(ibv_modify_qp(recv_queue, &attribute, flags), "could not mark receive queue as RTS");

    // create the attribute to mark the send queue as RTR
    attribute = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_4096,
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
        .timeout = zip::consts::rdma::TIMEOUT,
        .retry_cnt = zip::consts::rdma::RETRY,
        .rnr_retry = zip::consts::rdma::RNR_RETRY
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
 *
 * @return 0 if successful, errno otherwise
 */
int send(int socket, void* data, unsigned long length) {
    unsigned long offset = 0;
    while (offset < length) {
        auto sent = ::send(socket, static_cast<char*>(data) + offset, length - offset, 0);
        if (sent == -1) return errno;
        offset += sent;
    }
    return 0;
}

/**
 * Receive data from the given socket.
 *
 * @param socket socket to receive data from
 * @param data   buffer to put data in
 * @param length length of the data to receive
 *
 * @return 0 if successful, errno otherwise
 */
int recv(int socket, void* data, unsigned long length) {
    unsigned long offset = 0;
    while (offset < length) {
        auto received = ::recv(socket, static_cast<char*>(data) + offset, length - offset, 0);
        if (received == -1) return errno;
        offset += received;
    }
    return 0;
}

/**
 * Free the given RDMA queue pairs and completion queues.
 */
void free_queues(
    std::vector<ibv_qp*>& send_queues,
    std::vector<ibv_qp*>& recv_queues,
    std::vector<ibv_cq*>& completion_queues
) {
    for (auto sq: send_queues) ZIP_ASSERT_ZERO(ibv_destroy_qp(sq), "could not destroy send queue");
    for (auto rq: recv_queues) ZIP_ASSERT_ZERO(ibv_destroy_qp(rq), "could not destroy receive queue");
    for (auto cq: completion_queues) ZIP_ASSERT_ZERO(ibv_destroy_cq(cq), "could not destroy completion queue");
}

} // namespace detail

std::optional<std::tuple<send_queue, void*, unsigned long, sockaddr_in>>
manager::accept(void* message, unsigned long length) {
    // try to accept a connection and check if it was successful
    auto result = accept(message, length, 1);
    if (!result) return {};

    // otherwise unpack the vector and return the tuple
    auto& [queues, received_message, received_length, address] = *result;
    return {{std::move(queues.front()), received_message, received_length, address}};
}

std::optional<std::tuple<std::vector<send_queue>, void*, unsigned long, sockaddr_in>>
manager::accept(void* message, unsigned long length, unsigned long num_queues) {
    // make sure that this manager is a server
    auto lock = std::scoped_lock(server_lock_, xrc_lock_, handshake_lock_);
    ZIP_ASSERT(server_, "accept called on a non-server manager");

    // create a address location
    sockaddr_in address;
    socklen_t addr_length = sizeof(sockaddr_in);

    // wait to accept an incoming connection
    auto ret = ppoll(&poll_, 1, &zip::consts::POLL_TIMEOUT, &signal_);
    ZIP_ASSERT(ret >= 0 || errno ==  EINTR, "could not poll socket");

    // if there was a timeout or signal, then return empty
    if (ret == 0 || errno == EINTR) return {};

    // try to accept the socket
    auto socket = ::accept(socket_, reinterpret_cast<sockaddr*>(&address), &addr_length);
    if (socket < 0) return {};

    // exchange number of queues to create
    uint64_t mine_num_queues = num_queues, other_num_queues;
    if (   detail::recv(socket, &other_num_queues, sizeof(uint64_t))
        || detail::send(socket, &mine_num_queues, sizeof(uint64_t))) return {};

    // calculate the number of queues
    if (mine_num_queues == 0 && other_num_queues == 0) {
        num_queues = 1;
    } else if (mine_num_queues == 0) {
        num_queues = other_num_queues;
    } else if (other_num_queues == 0) {
        num_queues = mine_num_queues;
    }  else {
        num_queues = std::min(mine_num_queues, other_num_queues);
    }

    // create vectors for storing RDMA queues
    std::vector<ibv_qp*> send_queues, recv_queues;
    std::vector<ibv_cq*> completion_queues;
    std::vector<detail::queue_info> queue_infos;

    // create the queues to setup the connection
    for (unsigned long i = 0; i < num_queues; i++) {
        auto [send_queue, recv_queue, completion_queue] = create_queues();
        auto [index, address, remote_key] = create_receive_buffer();
        completion_queues.emplace_back(completion_queue);
        recv_queues.emplace_back(recv_queue);
        send_queues.emplace_back(send_queue);
        queue_infos.emplace_back(detail::queue_info {
            index,
            address,
            remote_key,
            send_queue->qp_num,
            recv_queue->qp_num
        });
    }

    // copy local information into the send buffer
    auto [send_buf, send_buf_size] = detail::serialize_connection(
        local_id_,
        local_gid_,
        queue_infos,
        xrc_queue_nums_,
        message,
        length
    );

    // exchange buffer sizes
    uint64_t recv_buf_size;
    if (   detail::recv(socket, &recv_buf_size, sizeof(uint64_t))
        || detail::send(socket, &send_buf_size, sizeof(uint64_t))) {
        detail::free_queues(send_queues, recv_queues, completion_queues);
        return {};
    }

    // allocate the receive buffer and exchange data with the other side
    auto recv_buf = std::unique_ptr<void, decltype(std::free)*>(std::malloc(recv_buf_size), std::free);
    if (   detail::recv(socket, recv_buf.get(), recv_buf_size)
        || detail::send(socket, send_buf.get(), send_buf_size)) {
        detail::free_queues(send_queues, recv_queues, completion_queues);
        return {};
    }

    // close the socket
    close(socket);

    // get the information from the receive buffer
    auto [
        remote_id,
        remote_gid,
        remote_queue_infos,
        xrc_queue_nums,
        received_message,
        received_length
    ] = detail::deserialize_connection(recv_buf, recv_buf_size);

    // activate the queues with the received information
    for (unsigned long i = 0; i < num_queues; i++) {
        detail::activate_queues(
            send_queues[i],
            recv_queues[i],
            device_port_,
            gid_,
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
            remote_queue_infos[i].index,
            remote_queue_infos[i].address,
            remote_queue_infos[i].remote_key,
            xrc_queue_nums,
            *this
        );
    }

    // copy the handshake message
    auto handshake = static_cast<handshake_message*>(std::malloc(sizeof(uint64_t) + received_length));
    std::memcpy(handshake->data, received_message, received_length);
    handshake->data_length = received_length;
    handshakes_.emplace_back(handshake, std::free);

    // set the context for the completion queue and the queue pairs
    for (unsigned long i = 0; i < num_queues; i++) {
        send_queues[i]->qp_context = handshake;
        recv_queues[i]->qp_context = handshake;
    }

    // return the queues and the handshake message and the address
    return {{std::move(queues), handshake->data, received_length, address}};
}

std::optional<std::tuple<send_queue, void*, unsigned long>>
manager::connect(std::string server_ip, uint16_t port, void* message, unsigned long length) {
    // try to connect to the server and check if it was successful
    auto result = connect(server_ip, port, message, length, 1);
    if (!result) return {};

    // otherwise unpack the vector and return the tuple
    auto& [queues, received_message, received_length] = *result;
    return {{std::move(queues.front()), received_message, received_length}};
}

std::optional<std::tuple<std::vector<send_queue>, void*, unsigned long>>
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

std::optional<std::tuple<send_queue, void*, unsigned long>>
manager::connect(sockaddr_in address, void* message, unsigned long length) {
    // try to connect to the server and check if it was successful
    auto result = connect(address, message, length, 1);
    if (!result) return {};

    // otherwise unpack the vector and return the tuple
    auto& [queues, received_message, received_length] = *result;
    return {{std::move(queues.front()), received_message, received_length}};
}

std::optional<std::tuple<std::vector<send_queue>, void*, unsigned long>>
manager::connect(sockaddr_in address, void* message, unsigned long length, unsigned long num_queues) {
    // create the socket and connect to the server
    auto lock = std::scoped_lock(xrc_lock_, handshake_lock_);
    auto socket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (::connect(socket, reinterpret_cast<sockaddr*>(&address), sizeof(sockaddr_in))) return {};

    // exchange number of queues to create
    uint64_t mine_num_queues = num_queues, other_num_queues;
    if (   detail::send(socket, &mine_num_queues, sizeof(uint64_t))
        || detail::recv(socket, &other_num_queues, sizeof(uint64_t))) return {};

    // calculate the number of queues
    if (mine_num_queues == 0 && other_num_queues == 0) {
        num_queues = 1;
    } else if (mine_num_queues == 0) {
        num_queues = other_num_queues;
    } else if (other_num_queues == 0) {
        num_queues = mine_num_queues;
    }  else {
        num_queues = std::min(mine_num_queues, other_num_queues);
    }

    // create vectors for storing RDMA queues
    std::vector<ibv_qp*> send_queues, recv_queues;
    std::vector<ibv_cq*> completion_queues;
    std::vector<detail::queue_info> queue_infos;

    // create the queues to setup the connection
    for (unsigned long i = 0; i < num_queues; i++) {
        auto [send_queue, recv_queue, completion_queue] = create_queues();
        auto [index, address, remote_key] = create_receive_buffer();
        completion_queues.emplace_back(completion_queue);
        recv_queues.emplace_back(recv_queue);
        send_queues.emplace_back(send_queue);
        queue_infos.emplace_back(detail::queue_info {
            index,
            address,
            remote_key,
            send_queue->qp_num,
            recv_queue->qp_num
        });
    }

    // copy local information into the send buffer
    auto [send_buf, send_buf_size] = detail::serialize_connection(
        local_id_,
        local_gid_,
        queue_infos,
        xrc_queue_nums_,
        message,
        length
    );

    // exchange buffer sizes
    uint64_t recv_buf_size;
    if (   detail::send(socket, &send_buf_size, sizeof(uint64_t))
        || detail::recv(socket, &recv_buf_size, sizeof(uint64_t))) {
        detail::free_queues(send_queues, recv_queues, completion_queues);
        return {};
    }

    // allocate the receive buffer and exchange data with the other side
    auto recv_buf = std::unique_ptr<void, decltype(std::free)*>(std::malloc(recv_buf_size), std::free);
    if (   detail::send(socket, send_buf.get(), send_buf_size)
        || detail::recv(socket, recv_buf.get(), recv_buf_size)) {
        detail::free_queues(send_queues, recv_queues, completion_queues);
        return {};
    }

    // close the socket
    close(socket);

    // get the information from the receive buffer
    auto [
        remote_id,
        remote_gid,
        remote_queue_infos,
        xrc_queue_nums,
        received_message,
        received_length
    ] = detail::deserialize_connection(recv_buf, recv_buf_size);

    // activate the queues with the received information
    for (unsigned long i = 0; i < num_queues; i++) {
        detail::activate_queues(
            send_queues[i],
            recv_queues[i],
            device_port_,
            gid_,
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
            remote_queue_infos[i].index,
            remote_queue_infos[i].address,
            remote_queue_infos[i].remote_key,
            xrc_queue_nums,
            *this
        );
    }

    // copy the handshake message
    auto handshake = static_cast<handshake_message*>(std::malloc(sizeof(uint64_t) + received_length));
    std::memcpy(handshake->data, received_message, received_length);
    handshake->data_length = received_length;
    handshakes_.emplace_back(handshake, std::free);

    // set the context for the completion queue and the queue pairs
    for (unsigned long i = 0; i < num_queues; i++) {
        send_queues[i]->qp_context = handshake;
        recv_queues[i]->qp_context = handshake;
    }

    // return the queues and the handshake message and the address
    return {{std::move(queues), handshake->data, received_length}};
}

} // namespace zip::network
