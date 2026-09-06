#pragma once

#include <bit>
#include <chrono>
#include <concepts>
#include <cstdint>
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
static inline unsigned long time_in_us(std::chrono::duration<R, P> d) {
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

/**
 * Return the time in microseconds since epoch.
 */
template <typename C, typename D>
static inline unsigned long time_in_us(std::chrono::time_point<C, D> d) {
    return time_in_us(d.time_since_epoch());
}

/**
 * Returns the current time since Unix epoch in microseconds.
 */
static inline unsigned long curr_time_in_us() {
    return time_in_us(std::chrono::high_resolution_clock::now());
}

/**
 * Return the duration in nanoseconds.
 */
template <typename R, typename P>
static inline unsigned long time_in_ns(std::chrono::duration<R, P> d) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
}

/**
 * Return the time in nanoseconds since epoch.
 */
template <typename C, typename D>
static inline unsigned long time_in_ns(std::chrono::time_point<C, D> d) {
    return time_in_ns(d.time_since_epoch());
}

/**
 * Returns the current time since UNIX epoch in nanoseconds.
 */
static inline unsigned long curr_time_in_ns() {
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
template <unsigned long N, typename T> requires std::unsigned_integral<T> && (std::has_single_bit(N))
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
template <unsigned long N, typename T> requires std::unsigned_integral<T> && (N > 0 && !std::has_single_bit(N))
static inline T align(T v) {
    return static_cast<T>(N) * ((v + static_cast<T>(N)  - 1) / static_cast<T>(N));
}

/**
 * This is a wrapper class for a iterator which
 * wraps around the container.
 */
template <typename T>
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
    wraparound_iterator(T& container): container_(&container), iterator_(container.end()) {}

    /**
     * Return the current iterator and increment the
     * internal iterator and optionally wrap around
     * if we have reached the end.
     *
     * @return the original iterator
     */
    inline T::iterator get_and_increment() {
        if (iterator_ == container_->end()) {
            iterator_ = container_->begin();
        }
        return iterator_++;
    }

private:

    /** the original container */
    T* container_;

    /** the iterator that we will wrap around */
    T::iterator iterator_;

};

/**
 * Split the given address into IP and port.
 *
 * @param address the address in "IP:port" form
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
void* allocate_huge_page(unsigned long size, int numa = -1);

} // namespace zip::util
