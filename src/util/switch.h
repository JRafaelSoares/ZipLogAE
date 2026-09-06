#pragma once

#include <concepts>

#include "api/api.h"
#include "network/manager.h"
#include "network/recv_queue.h"
#include "util/log.h"
#include "util/util.h"

namespace zip::util {

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
template<typename R, typename Class, typename Arg> requires std::derived_from<Arg, zip::api::message<Arg>>
struct is_api_callback_helper<R(Class::*)(Arg&)> { using type = Arg; };

/** Checks whether the given type is a valid callback. */
template<typename T> struct is_api_callback: is_api_callback_helper<decltype(&T::operator())> {};

/** Base case for the functions `apply`. */
static inline void apply(zip::util::logger& logger, void* buffer, [[maybe_unused]] unsigned long length) {
    logger.warn("No action available for message type (", *static_cast<uint64_t*>(buffer), ")");
}

/**
 * Takes a bunch of lambdas and applies them one
 * by one to the message.
 *
 * @param logger   the logger object to use
 * @param buffer   the memory buffer with the message
 * @param length   the length of the received buffer
 * @param function callback to apply if the message matches the type
 *                 of the function
 *
 * NOTE: `function`s must be lambdas which take exactly
 *       one parameter, which is a lvalue reference to
 *       a class T which inherit from `zip::api::message<T>`.
 */
template <typename Arg, typename... Args>
static inline void apply(zip::util::logger& logger, void* buffer, unsigned long length, Arg function, Args... functions) {
    // get the type of the function argument, and compare
    // the tag of the message to the tag of the type
    using T = is_api_callback<Arg>::type;
    if (*static_cast<uint64_t*>(buffer) == T::tag) {
        auto& message = *std::launder(static_cast<T*>(buffer));
        ZIP_ASSERT_EQ(length, message.length(), "length of the received packet is invalid");
        function(message);
    } else {
        apply(logger, buffer, length, functions...);
    }
}

/**
 * Perform a receive on the given queue and if
 * a message is received, apply the matching
 * lambda to it.
 *
 * @param logger     the logger object to use
 * @param recv_queue the given network receive queue
 * @param functions  callbacks to apply if the message matches the type
 *                   of the function
 */
template <unsigned long N = zip::consts::rdma::RECEIVE_BATCH_SIZE, typename... Args>
static inline void recv_apply(zip::util::logger& logger, zip::network::recv_queue& recv_queue, Args... functions) {
    recv_queue.recv<N>([&] (void* buffer, unsigned long length) {
        apply(logger, buffer, length, functions...);
    });
}

/**
 * Try to poll for a failure event and apply the matching
 * lambda to it.
 *
 * @param logger     the logger object to use
 * @param manager    the given network manager
 * @param functions  callbacks to apply if the message matches the type
 *                   of the function
 */
template <typename... Args>
static inline void failure_apply(zip::util::logger& logger, zip::network::manager& manager, Args... functions) {
    manager.process_failure([&] (void* buffer, unsigned long length) {
        apply(logger, buffer, length, functions...);
    });
}

} // namespace zip::util
