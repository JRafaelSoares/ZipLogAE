#include <zip/network/manager.h>

#include <utility>
#include <netdb.h>
#include <zip/network/buffer.h>
#include <zip/network/transport.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/util.h>

namespace zip::network {

/** create a logger for this file */
static zip::util::logger logger("manager");

/// Initialize the global running flag for graceful shutdown
std::atomic<bool> transport_factory::running_ = true;

manager::manager(int numa)
{
    numa_ = (numa == -1) ? -1 : numa;
    free_receive_buffers_.set();
}

manager::~manager() {
    logger.trace("Will destroy buffers");
    auto lock = std::unique_lock(memory_lock_);
    auto size_it = page_sizes_.begin();
    for(auto addr: page_list_)
    {
        zip::util::free_huge_page(addr, *size_it);
        ++size_it;
    }
}

std::vector<std::unique_ptr<buffer>> manager::get_buffers(unsigned long num_buffers) {
    // return buffers if available, otherwise, allocate a
    // new page and return the newly allocated buffers
    auto lock = std::unique_lock(memory_lock_);
    while (free_list_.size() < num_buffers) {
        // create a new huge page as a send buffer
        // and register it with the network device
        logger.trace("Will alloc more buffers");
        auto page = zip::util::allocate_huge_page(zip::consts::HUGE_PAGE_SIZE, numa_);
        page_list_.emplace_back(page);
        page_sizes_.emplace_back(zip::consts::HUGE_PAGE_SIZE);

        // create send buffers and add them to the free list
        for(unsigned long j = 0; j < zip::consts::HUGE_PAGE_SIZE; j+= zip::consts::SEND_BUFFER_SIZE)
        {
            free_list_.emplace_back(static_cast<uint8_t*>(page) + j);
        }
    }

    // create a resulting list
    std::vector<std::unique_ptr<buffer>> result;

    // add the requisite number of buffers to the list
    auto it = free_list_.begin();
    for (unsigned long i = 0; i < num_buffers; i++, it++) {
        result.emplace_back(std::make_unique<buffer>(*it, *this));
    }

    // erase the elements from the free list and return
    free_list_.erase(free_list_.begin(), it);
    return result;
}

std::unique_ptr<buffer> manager::get_buffer()
{
    auto ret = get_buffers(1);
    return std::move(ret.front());
}

void manager::put_buffer(void* buffer) {
    // put the buffer back in free list
    auto lock = std::unique_lock(memory_lock_);
    free_list_.emplace_back(buffer);
}

void manager::free_recv_buffer(unsigned long index) {
    // mark the index as available
    auto lock = std::unique_lock(memory_lock_);
    free_receive_buffers_.set(index);
}

} // namespace zip::network
