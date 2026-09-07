#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "util/consts.h"

namespace zip {
namespace util {

/**
 * Return the duration in microseconds.
 */
template <typename R, typename P>
static inline unsigned long time_in_us(std::chrono::duration<R, P> d) {
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

/**
 * Returns the current time since Unix epoch in microseconds.
 */
static inline unsigned long curr_time_in_us() {
    return time_in_us(std::chrono::high_resolution_clock::now().time_since_epoch());
}

/**
 * Returns more than half of the
 * given number.
 *
 * @param v the given number
 *
 * @tprarm N any integral type
 *
 * @return more than half of the given number
 */
template <typename N>
static inline std::enable_if_t<std::is_integral_v<N>, N> more_than_half(N v) {
    return (v > 2) ? (v / 2) + 1 : v;
}

/**
 * Returns the nearest power of two greater than
 * or equal to the given number.
 *
 * @param v the given number
 *
 * @tparam N any unsigned integral type
 *           with at most 64 bits
 *
 * @return nearest power of two
 */
template <typename N>
static inline std::enable_if_t<std::is_unsigned_v<N> && sizeof(N) <= 8, N> nearest_power_of_two(N v) {
    v--; v |= v >> 1; v |= v >> 2; v |= v >> 4;
    if constexpr (sizeof(N) >= 2) v |= v >> 8;
    if constexpr (sizeof(N) >= 4) v |= v >> 16;
    if constexpr (sizeof(N) >= 8) v |= v >> 32;
    return v + 1;
}

/**
 * Given a buffer size, return it's index
 * in the `zip::consts::BUFFER_SIZES`
 * array.
 *
 * NOTE: we clamp the size between the min and maximum
 *       buffer sizes before calculating, the caller must
 *       ensure that they send correct values.
 *
 * NOTE: this function very much depends on
 *       what the values are in `zip::consts::BUFFER_SIZES`,
 *       update this accordingly if you change that constant.
 *
 * @param size the size of the buffer
 *
 * @return the size index
 */
static inline unsigned long size_index(unsigned long size) {
    auto min = zip::consts::BUFFER_SIZES.front(), max = zip::consts::BUFFER_SIZES.back();
    auto log = __builtin_ctzl(nearest_power_of_two(std::clamp(size, min, max)));
    return ((log + (log % 2)) - __builtin_ctzl(min)) / 2;
}

/**
 * Given a buffer size, return the element
 * in the `zip::consts::BUFFER_SIZES`
 * array that is it's least upper bound.
 *
 * NOTE: we clamp the size between the min and maximum
 *       buffer sizes before calculating, the caller must
 *       ensure that they send correct values.
 *
 * NOTE: this function very much depends on
 *       what the values are in `zip::consts::BUFFER_SIZES`,
 *       update this accordingly if you change that constant.
 *
 * @param size the size of the buffer
 *
 * @return the least upper bound
 */
static inline unsigned long buffer_size(unsigned long size) {
    auto min = zip::consts::BUFFER_SIZES.front(), max = zip::consts::BUFFER_SIZES.back();
    auto log = __builtin_ctzl(nearest_power_of_two(std::clamp(size, min, max)));
    return 1 << (log + (log % 2));
}

/**
 * This is a wrapper class for a iterator which
 * wraps around the container.
 */
template <typename T>
class wraparound_iterator {

public:

    /**
     * Initialise a wraparound iterator.
     *
     * @param container the original container
     */
    wraparound_iterator(T& container): container_(container), iterator_(container.begin()) {}

    /**
     * Return the current iterator and increment the
     * internal iterator and optionally wrap around
     * if we have reached the end.
     *
     * @return the original iterator
     */
    inline typename T::iterator get_and_increment() {
        auto orig = iterator_++;
        if (iterator_ == container_.end()) {
            iterator_ = container_.begin();
        }
        return orig;
    }

private:

    /** the original container */
    T& container_;

    /** the iterator that we will wrap around */
    typename T::iterator iterator_;

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
 * Assert a condition with the given message.
 *
 * NOTE: an object called logger with of type
 *       `zip::util::log` must be in scope.
 */
#define ZIP_ASSERT(condition, message...) {                                              \
    if (!(condition)) {                                                                  \
        logger.error("Assert failed at ", __FILE__, ":", __LINE__, " with ", message);   \
        std::exit(1);                                                                    \
    }                                                                                    \
}                                                                                        \

/**
 * Assert two values are equal with the given message.
 *
 * NOTE: an object called logger with of type
 *       `zip::util::log` must be in scope.
 */
#define ZIP_ASSERT_EQ(expression, expected, message...) {                                                             \
    auto value = (expression);                                                                                        \
    if (value != expected) {                                                                                          \
        logger.error("Assert failed at ", __FILE__, ":", __LINE__, " (", value, " != ", expected, ") with ", message); \
        std::exit(1);                                                                                                 \
    }                                                                                                                 \
}

/**
 * Assert two values are not equal with the given message.
 *
 * NOTE: an object called logger with of type
 *       `zip::util::log` must be in scope.
 */
#define ZIP_ASSERT_NEQ(expression, expected, message...) {                                                            \
    auto value = (expression);                                                                                        \
    if (value == expected) {                                                                                          \
        logger.error("Assert failed at ", __FILE__, ":", __LINE__, " (", value, " == ", expected, ") with ", message); \
        std::exit(1);                                                                                                 \
    }                                                                                                                 \
}

/**
 * Assert the given value is 0 with the given message.
 */
#define ZIP_ASSERT_ZERO(expression, message...) ZIP_ASSERT_EQ(expression, 0, message)

/**
 * Assert the given value is not nullptr with the given message.
 */
#define ZIP_ASSERT_NOT_NULL(expression, message...) ZIP_ASSERT_NEQ(expression, nullptr, message)

/**
 * Profile a function using RAII.
 */
class profiler {

public:

    /**
     * Initialise the profiler.
     *
     * @param file the __FILE__ placeholder
     * @param func the __FUNCTION__ placeholder
     * @param line the __LINE__ placeholder
     */
    profiler(const char* file, const char* func, int line);

    /**
     * Deinitialise a profiler and print the duration.
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
        while (duration > (std::chrono::high_resolution_clock::now() - begin)) {}
    }
}

/**
 * Convenience class to `free` a
 * `unique_ptr` allocated using `malloc`.
 */
template<typename T>
struct free_deleter {
    inline void operator()(T* t) {
        std::free(t);
    }
};

/**
 * Convenient to write type to denote `unique_ptr`s
 * allocated using `malloc`.
 */
template <typename T>
using unique_ptr_malloc = std::unique_ptr<T, free_deleter<T>>;

/**
 * Allocate an object using `malloc` and return
 * the result wrapped in the right `unique_ptr`
 * type.
 *
 * @tparam T the type of the pointer
 *
 * @param size the amount of memory to allocate
 *
 * @return the `unique_ptr` object
 */
template <typename T>
static inline unique_ptr_malloc<T> malloc_unique(unsigned long size) {
    auto ptr = static_cast<T*>(std::malloc(size));
    return unique_ptr_malloc<T>(ptr);
}

/**
 * Allocate memory backed by huge pages of the given size.
 *
 * @param size the amount of memory to allocate
 *
 * @return the pointer to the allocated memory
 */
void* allocate_huge_page(unsigned long size);

} // namespace util
} // namespace zip
