#pragma once

#include <iostream>
#include <string>
#include <sstream>

#include "util/util.h"
#include "util/consts.h"

namespace zip {
namespace util {

namespace detail {

/**
 * Use templates to print a variable number of
 * function arguments.
 */
template <typename Arg>
static inline void join(std::ostream& buf, Arg&& arg) {
    buf << arg;
}
template<typename Arg, typename... Args>
static inline void join(std::ostream& buf, Arg&& arg, Args&&... args) {
    buf << arg;
    join(buf, args...);
}

} // namespace detail

class logger {

public:

    /**
     * Initialise a logger.
     *
     * @name name of the logger
     */
    logger(std::string name): name_(name) {}

/**
 * A macro for defining templates for
 * logging functions.
 */
#define DEFINE_LOG(func, LEVEL)                                                            \
    template<typename... Args>                                                             \
    inline void func(Args&&... args) {                                                     \
        if constexpr (zip::consts::LOG_LEVEL <= LEVEL) {                                   \
            auto time = zip::util::curr_time_in_us();                                      \
            auto buf = std::stringbuf();                                                   \
            auto str = std::ostream(&buf);                                                 \
            detail::join(str, "[" #LEVEL "] [", name_, "] [", time, "] ", args..., ".\n"); \
            std::cerr << buf.str();                                                        \
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

} // namespace util
} // namespace zip
