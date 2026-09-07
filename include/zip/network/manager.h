#pragma once

#include <bitset>
#include <list>
#include <mutex>
#include <vector>

#include <infiniband/verbs.h>

#include <zip/util/consts.h>

/** Forward declaration for socket structs. */
struct sockaddr_in;

namespace zip::network {

/** Forward declaration for send and receive queues. */
class buffer;

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
     * Initialize the network manager as a simple client.
     *
     * @param numa   NUMA node to use for memory allocation
     */
    manager(int numa = -1);

    /**
     * Deinitialize the manager.
     */
    ~manager();

    /**
     * Get a number of free network buffers.
     *
     * @param num_buffers the number of buffers to allocate
     *
     * @return the buffers
     */
    std::vector<std::unique_ptr<buffer>> get_buffers(unsigned long num_buffers);

    /**
     * @brief Get one buffer
     *
     * @return buffer
     */
    std::unique_ptr<buffer> get_buffer();

private:

    /** Make buffer  friend of this class */
    friend class buffer;

    /**
     * Return a buffer to the free-list.
     *
     * @param buffer    address of the buffer
     * @param local_key local key for network device access to this buffer
     */
    void put_buffer(void* buffer);

    /**
     * Free a slot for a receive buffer to be used.
     *
     * @return index index into the receive buffer array
     */
    void free_recv_buffer(unsigned long index);

    /** NUMA node for the RDMA device */
    int numa_;

    /** lock to protect memory allocation state */
    std::mutex memory_lock_;

    /** memory pages registered with the network device */
    std::list<void*> page_list_;

    /** sizes of memory pages in page_list_ */
    std::list<uint32_t> page_sizes_;

    /** free list of buffers */
    std::list<void*> free_list_;

    /** bitmap of whether the given index is in use */
    std::bitset<zip::consts::rdma::MAX_QUEUE_PAIRS> free_receive_buffers_;

};

} // namespace zip::network
