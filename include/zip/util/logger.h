#pragma once

#include <cstdlib>
#include <iostream>
#include <string>
#include <sstream>

#include <zip/util/util.h>
#include <zip/util/consts.h>

namespace zip::util {

namespace detail {

/**
 * Use templates to print a variable number of
 * function arguments.
 */
static inline void join([[maybe_unused]] std::ostream& buf) {}
template <typename Arg, typename... Args>
static inline void join(std::ostream& buf, Arg arg, Args... args) {
    if constexpr (std::same_as<Arg, shard_t>) buf << static_cast<uint32_t>(arg);
    else if constexpr (std::same_as<Arg, replica_t>) buf << static_cast<uint32_t>(arg);
    else buf << arg;
    join(buf, args...);
}

} // namespace detail

class logger {

public:

    /**
     * Initialize a logger.
     *
     * @name name of the logger
     */
    logger(std::string name): name_(name) {}

    /**
     * Static function to be able to assert from inside header files.
     */
    template<typename... Args>
    static inline void logger_assert(Args&&... args) {
        auto time = zip::util::curr_time_in_us();
        auto buf = std::stringbuf();
        auto str = std::ostream(&buf);
        detail::join(str, "[ERROR] [ASSERT] [", time, "] ", args..., ".\n");
        std::clog << buf.str();
    }

/**
 * A macro for defining templates for
 * logging functions.
 */
#define DEFINE_LOG(func, LEVEL)                                                            \
    template<typename... Args>                                                             \
    inline void func(Args&&... args) {                                                     \
        if constexpr (zip::consts::LOG_LEVEL <= zip::consts::LEVEL) {                      \
            auto time = zip::util::curr_time_in_us();                                      \
            auto buf = std::stringbuf();                                                   \
            auto str = std::ostream(&buf);                                                 \
            detail::join(str, "[" #LEVEL "] [", name_, "] [", time, "] ", args..., ".\n"); \
            std::clog << buf.str();                                                        \
        }                                                                                  \
    }

/**
 * Define loggers for each log level.
 */
DEFINE_LOG(trace, TRACE)
DEFINE_LOG(debug, DEBUG)
DEFINE_LOG(info, INFO)
DEFINE_LOG(warn, WARN)
DEFINE_LOG(error, ERROR)
#undef DEFINE_LOG

private:

    /** define a name for this logger */
    std::string name_;

};

/**
 * Assert a condition with the given message.
 *
 * NOTE: an object called logger with of type
 *       `zip::util::log` must be in scope.
 */
#define ZIP_ASSERT(condition, message...) {                                                         \
    if (!(condition)) {                                                                             \
        zip::util::logger::logger_assert("Assert failed at ", __FILE__, ":", __LINE__, " with ", message); \
        std::exit(1);                                                                               \
    }                                                                                               \
}                                                                                                   \

/**
 * Assert two values are equal with the given message.
 *
 * NOTE: an object called logger with of type
 *       `zip::util::log` must be in scope.
 */
#define ZIP_ASSERT_EQ(expression, expected, message...) {                                                                           \
    auto value = (expression);                                                                                                      \
    if (value != expected) {                                                                                                        \
        zip::util::logger::logger_assert("Assert failed at ", __FILE__, ":", __LINE__, " (", value, " != ", expected, ") with ", message); \
        std::exit(1);                                                                                                               \
    }                                                                                                                               \
}

/**
 * Assert two values are not equal with the given message.
 *
 * NOTE: an object called logger with of type
 *       `zip::util::log` must be in scope.
 */
#define ZIP_ASSERT_NEQ(expression, expected, message...) {                                                                          \
    auto value = (expression);                                                                                                      \
    if (value == expected) {                                                                                                        \
        zip::util::logger::logger_assert("Assert failed at ", __FILE__, ":", __LINE__, " (", value, " == ", expected, ") with ", message); \
        std::exit(1);                                                                                                               \
    }                                                                                                                               \
}

/**
 * Assert the given value is 0 with the given message.
 */
#define ZIP_ASSERT_ZERO(expression, message...) ZIP_ASSERT_EQ(expression, 0, message)

/**
 * Assert the given value is not nullptr with the given message.
 */
#define ZIP_ASSERT_NOT_NULL(expression, message...) ZIP_ASSERT_NEQ(expression, nullptr, message)

} // namespace zip::util
