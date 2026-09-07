#pragma once

#include <bit>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <ranges>
#include <string>
#include <thread>
#include <utility>

#include <emmintrin.h>

namespace zip::util {

/** Use inside spin locks to increase efficiency. */
static inline void relax() { _mm_pause(); }

/**
 * Return the duration in microseconds.
 */
template <typename R, typename P>
static inline uint64_t time_in_us(std::chrono::duration<R, P> d) {
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

/**
 * Return the time in microseconds since epoch.
 */
template <typename C, typename D>
static inline uint64_t time_in_us(std::chrono::time_point<C, D> d) {
    return time_in_us(d.time_since_epoch());
}

/**
 * Returns the current time since UNIX epoch in microseconds.
 */
static inline uint64_t curr_time_in_us() {
    return time_in_us(std::chrono::high_resolution_clock::now());
}

/**
 * Return the duration in nanoseconds.
 */
template <typename R, typename P>
static inline uint64_t time_in_ns(std::chrono::duration<R, P> d) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
}

/**
 * Return the time in nanoseconds since epoch.
 */
template <typename C, typename D>
static inline uint64_t time_in_ns(std::chrono::time_point<C, D> d) {
    return time_in_ns(d.time_since_epoch());
}

/**
 * Return the time from nanoseconds since epoch.
 */
static inline std::chrono::high_resolution_clock::time_point
time_from_ns(uint64_t ns) { return std::chrono::high_resolution_clock::time_point{} + std::chrono::nanoseconds(ns); }

/**
 * Return the time from microseconds since epoch.
 */
static inline std::chrono::high_resolution_clock::time_point
time_from_us(uint64_t us) { return std::chrono::high_resolution_clock::time_point{} + std::chrono::microseconds(us); }

/**
 * Returns the current time since UNIX epoch in nanoseconds.
 */
static inline uint64_t curr_time_in_ns() {
    return time_in_ns(std::chrono::high_resolution_clock::now());
}

/**
 * Return the next multiple of the given number.
 *
 * @param v the number to round up
 *
 * @tparam N the number to round up to, which is a power of 2
 * @tparam T the type of the result
 *
 * @return the rounded up number
 */
template <uintmax_t N, std::unsigned_integral T> requires (std::has_single_bit(N))
static inline T align(T v) {
    return ((v - 1) & ~(static_cast<T>(N) - 1)) + static_cast<T>(N);
}

/**
 * Return the next multiple of the given number.
 *
 * @param v the number to round up
 *
 * @tparam N the number to round up to
 * @tparam T the type of the result
 *
 * @return the rounded up number
 */
template <uintmax_t N, std::unsigned_integral T> requires (N > 0 && !std::has_single_bit(N))
static inline T align(T v) {
    return static_cast<T>(N) * ((v + static_cast<T>(N)  - 1) / static_cast<T>(N));
}

/**
 * This is a wrapper class for a iterator which
 * wraps around the container.
 */
template <std::ranges::input_range T>
class wraparound_iterator {

public:

    /**
     * Instantiate the implicit default constructor, copy constructor,
     * and copy assignment operator.
     */
    wraparound_iterator() = default;
    wraparound_iterator(const wraparound_iterator&) = default;
    wraparound_iterator& operator=(const wraparound_iterator&) = default;

    /**
     * Initialize a wraparound iterator.
     *
     * @param container the original container
     */
    wraparound_iterator(T& container): container_(&container), iterator_(std::ranges::end(container)) {}

    /**
     * Return the current value and increment the
     * internal iterator and optionally wrap around
     * if we have reached the end.
     *
     * @return the current value
     */
    inline std::ranges::range_reference_t<T> get_and_increment() {
        if (iterator_ == std::ranges::end(*container_)) {
            iterator_ = std::ranges::begin(*container_);
        }
        auto current = iterator_;
        std::advance(iterator_, 1);
        return *current;
    }

private:

    /** the original container */
    T* container_;

    /** the iterator that we will wrap around */
    std::ranges::iterator_t<T> iterator_;

};

/**
 * Split the given address into address and port.
 *
 * @param address the address in "address:port" form
 *
 * @return the address and port
 */
std::pair<std::string, uint16_t> split_address(std::string address);

/**
 * Pin a thread to the given CPU.
 */
void pin_thread(std::thread& thread, uint16_t cpu_id);

/**
 * Pin the current thread to the given CPU.
 */
void pin_thread(uint16_t cpu_id);

/**
 * Profile a function using RAII.
 */
class profiler {

public:

    /**
     * Initialize the profiler.
     *
     * @param file the __FILE__ placeholder
     * @param func the __FUNCTION__ placeholder
     * @param line the __LINE__ placeholder
     */
    profiler(const char* file, const char* func, int line);

    /**
     * Deinitialize a profiler and print the duration.
     */
    ~profiler();

private:

    /** record when profiling began */
    std::chrono::high_resolution_clock::time_point begin_;

    /** record the file */
    const char* file_;

    /** record the function */
    const char* func_;

    /** record the line */
    int line_;

};

/** A simple macro to create a uniquely named profiler object. */
#define CONCAT_(prefix, suffix) prefix ## suffix
#define CONCAT(prefix, suffix) CONCAT_(prefix, suffix)
#define ZIP_PROFILE zip::util::profiler CONCAT(__profiler__, __LINE__)(__FILE__, __FUNCTION__, __LINE__);

/**
 * Spin sleep for given duration.
 *
 * @param duration the duration to sleep for
 */
template <typename R, typename P>
static inline void sleep_for(std::chrono::duration<R, P> duration) {
    if (duration > std::chrono::duration<R, P>(0)) {
        auto begin = std::chrono::high_resolution_clock::now();
        while (duration > (std::chrono::high_resolution_clock::now() - begin)) relax();
    }
}

/**
 * Allocate memory backed by huge pages of the given size.
 *
 * @param size the amount of memory to allocate
 * @param numa the NUMA node to allocate on
 *
 * @return the pointer to the allocated memory
 */
void* allocate_huge_page(uint32_t size, int numa = -1);

/**
 * Allocate memory backed by huge pages of the given size
 * and return a pointer of the given type.
 *
 * @param num  the number of objects of the type to allocate
 * @param numa the NUMA node to allocate on
 *
 * @return the pointer to the allocated memory
 */
template <typename T>
T* allocate_huge_page(uint32_t num = 1, int numa = -1) {
    return static_cast<T*>(allocate_huge_page(sizeof(T) * num, numa));
}

/**
 * Free memory that was allocated by allocate_huge_page.
 * Uses munmap to deallocate mmap'd huge page memory.
 *
 * @param ptr  the pointer to free
 * @param size the size of the allocation
 */
void free_huge_page(void* ptr, uint32_t size);

struct net_info {
    std::string host;
    uint16_t port;
};

net_info parse_ipv4(std::string& ipv4_str);

} // namespace zip::util
