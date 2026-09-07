#include "util/util.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>

#include "util/consts.h"
#include "util/log.h"

namespace zip {
namespace util {

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
        logger.warn("Failed to set thread affinity on cpu ", cpu_id, " : ", std::strerror(err));
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
    std::string ip;

    // split the address if it contains
    // a colon
    if (index != std::string::npos) {
        ip = address.substr(0, index);
        port = std::stoi(address.substr(index + 1));
    } else {
        ip = address;
        port = zip::consts::DEFAULT_PORT;
    }

    // return both the address and port
    return {ip, port};
}

profiler::profiler(const char* file, const char* func, int line):
begin_(std::chrono::high_resolution_clock::now()), file_(file), func_(func), line_(line) {}

profiler::~profiler() {
    // mark the end of tracing and print the duration
    auto us = time_in_us(std::chrono::high_resolution_clock::now() - begin_);
    logger.info("Tracing at ", file_, ":", line_, " in function `", func_, "` took ", us, " us");
}

void* allocate_huge_page(unsigned long size) {
    // allocate the memory and mark it as a huge page
    auto ptr = std::aligned_alloc(zip::consts::HUGE_PAGE_SIZE, size);
    ZIP_ASSERT_NOT_NULL(ptr, "could not allocate memory");
    ZIP_ASSERT_ZERO(madvise(ptr, size, MADV_HUGEPAGE), "could not mark allocated memory as huge page");
    return ptr;
}

} // namespace util
} // namespace zip
