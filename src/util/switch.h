#pragma once

#include <cassert>
#include <functional>

#include "api/api.h"
#include "network/buffer.h"
#include "network/recv_queue.h"
#include "util/log.h"
#include "util/util.h"

namespace zip {
namespace util {

/**
 * Templates to check whether the given type of function
 * takes in a single argument which is a reference
 * to a subclass of `zip::api::message`.
 */
template<typename T> struct is_api_callback_helper { static constexpr bool value = false; };
template<typename R, typename Class, typename... Args> struct is_api_callback_helper<R(Class::*)(Args...) const>: is_api_callback_helper<R(Class::*)(Args...)> {};
template<typename R, typename Class, typename... Args> struct is_api_callback_helper<R(Class::*)(Args...) const volatile>: is_api_callback_helper<R(Class::*)(Args...)> {};
template<typename R, typename Class, typename... Args> struct is_api_callback_helper<R(Class::*)(Args...) const volatile noexcept>: is_api_callback_helper<R(Class::*)(Args...)> {};
template<typename R, typename Class, typename... Args> struct is_api_callback_helper<R(Class::*)(Args...) volatile noexcept>: is_api_callback_helper<R(Class::*)(Args...)> {};
template<typename R, typename Class, typename... Args> struct is_api_callback_helper<R(Class::*)(Args...) const noexcept>: is_api_callback_helper<R(Class::*)(Args...)> {};
template<typename R, typename Class, typename... Args> struct is_api_callback_helper<R(Class::*)(Args...) noexcept>: is_api_callback_helper<R(Class::*)(Args...)> {};
template<typename R, typename Class, typename Arg> struct is_api_callback_helper<R(Class::*)(Arg&)> {
    static constexpr bool value = std::is_base_of_v<zip::api::message<Arg>, Arg>;
    using type = Arg;
};

/** Checks whether the given type is a valid callback. */
template<typename T> struct is_api_callback: is_api_callback_helper<decltype(&T::operator())> {};

/** Base case for the functions `apply`. */
static inline void apply(zip::util::logger& logger, void* buffer) {
    logger.warn("Received an unknown type of message (", *static_cast<uint64_t*>(buffer), ")");
    assert(false);
}

/**
 * Takes a bunch of lambdas and applies them one
 * by one to the message.
 *
 * @param logger   the logger object to use
 * @param buffer   the memory buffer with the message
 * @param function callback to apply if the message matches the type
 *                 of the function
 *
 * NOTE: `function`s must be lambdas which take exactly
 *       one parameter, which is a lvalue reference to
 *       a class T which inherit from `zip::api::message<T>`.
 */
template <typename Arg, typename... Args>
static inline std::enable_if_t<is_api_callback<Arg>::value, void>
apply(zip::util::logger& logger, void* buffer, Arg function, Args... functions) {
    // get the type of the function argument, and compare
    // the tag of the message to the tag of the type
    using Type = typename is_api_callback<Arg>::type;
    if (Type::tag == *static_cast<uint64_t*>(buffer)) {
        auto& message = *static_cast<Type*>(buffer);
        function(message);
    } else {
        apply(logger, buffer, functions...);
    }
}

/**
 *
 * Perform a receive on the given queue and if
 * a message is received, apply the matching
 * lambda to it.
 *
 * @param logger     the logger object to use
 * @param recv_queue the given network receive queue
 * @param functions  callbacks to apply if the message matches the type
 *                   of the function
 */
template <typename... Args>
static inline void recv_apply(zip::util::logger& logger, zip::network::recv_queue& recv_queue, Args... functions) {
    recv_queue.recv([&] (void* buffer) {
        apply(logger, buffer, functions...);
    });
}

} // namespace util
} // namespace zip
