#include <zip/util/util.h>

#include <cstdint>
#include <cstring>

#include <numa.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>

#include <zip/util/consts.h>
#include <zip/util/logger.h>

namespace zip::util {

/** create a logger for this file */
static logger logger("util");

namespace detail {

void pin_thread(pthread_t thread, uint16_t cpu_id) {
    // create the CPU set
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(cpu_id, &cpu_set);

    // try to set affinity
    auto err = pthread_setaffinity_np(thread, sizeof(cpu_set), &cpu_set);
    if (err != 0) {
        logger.warn("Failed to set thread affinity: ", std::strerror(err));
    }
}

} // namespace detail

void pin_thread(std::thread& thread, uint16_t cpu_id) {
    detail::pin_thread(thread.native_handle(), cpu_id);
}

void pin_thread(uint16_t cpu_id) {
    detail::pin_thread(pthread_self(), cpu_id);
}

std::pair<std::string, uint16_t> split_address(std::string address) {
    auto index = address.find(':');
    uint16_t port;
    std::string addr;

    // split the address if it contains
    // a colon
    if (index != std::string::npos) {
        addr = address.substr(0, index);
        port = std::stoi(address.substr(index + 1));
    } else {
        addr = address;
        port = zip::consts::DEFAULT_SERVER_PORT;
    }

    // return both the address and port
    return {addr, port};
}

profiler::profiler(const char* file, const char* func, int line):
begin_(std::chrono::high_resolution_clock::now()), file_(file), func_(func), line_(line) {}

profiler::~profiler() {
    // mark the end of tracing and print the duration
    auto us = time_in_us(std::chrono::high_resolution_clock::now() - begin_);
    logger.info("Tracing at ", file_, ":", line_, " in function `", func_, "` took ", us, " us");
}

void* allocate_huge_page(uint32_t size, int numa) {
    // try to map an anonymous hugepage
    ZIP_ASSERT_ZERO(size % zip::consts::HUGE_PAGE_SIZE, "size not a multiple of huge page size");
    auto ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB, -1, 0);
    ZIP_ASSERT_NEQ(ptr, MAP_FAILED, "could not allocate memory");

    // move the page to the requested NUMA and allocate it
    if (numa != -1) numa_tonode_memory(ptr, size, numa);
    ZIP_ASSERT_ZERO(madvise(ptr, size, MADV_POPULATE_WRITE), "could not mark allocated memory as populated");
    return ptr;
}

void free_huge_page(void* ptr, uint32_t size) {
    // use munmap to deallocate mmap'd huge page memory
    ZIP_ASSERT_ZERO(munmap(ptr, size), "could not deallocate huge page memory");
}

} // namespace zip::util
