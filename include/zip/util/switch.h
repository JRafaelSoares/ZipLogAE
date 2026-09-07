#pragma once

#include <concepts>
#include <type_traits>

#include <zip/api/api.h>
#include <zip/util/logger.h>

namespace zip::util {

namespace detail {

/**
 * Templates to check whether the given type is a function that takes
 * in a single argument which is a reference to a subclass of `zip::api::message`.
 */
template <typename T> struct extract_arg {};
template <typename T> requires std::is_pointer_v<T> struct extract_arg<T>: extract_arg<std::remove_pointer_t<T>> {};
template <typename T> requires std::is_const_v<T> struct extract_arg<T>: extract_arg<std::remove_const_t<T>> {};
template <typename T> requires std::is_volatile_v<T> struct extract_arg<T>: extract_arg<std::remove_volatile_t<T>> {};
template <typename T> requires std::is_reference_v<T> struct extract_arg<T>: extract_arg<std::remove_reference_t<T>> {};
template <typename T> requires requires { &T::operator(); } struct extract_arg<T>: extract_arg<decltype(&T::operator())> {};
template <typename Class, typename Func> struct extract_arg<Func Class::*>: extract_arg<Func> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) & noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) && noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) &&>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) &>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const & noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const && noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const &&>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const &>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const volatile & noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const volatile && noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const volatile &&>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const volatile &>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const volatile noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const volatile>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) const>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) volatile & noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) volatile && noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) volatile &&>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) volatile &>: extract_arg<R(Args...)> {};
template <typename R, typename... Args> struct extract_arg<R(Args...) volatile noexcept>: extract_arg<R(Args...)> {};
template <typename R, typename Arg> struct extract_arg<R(Arg&)> { using type = Arg; };

/** Checks whether the given type has a way to call it with parameters. */
template <typename T>
concept is_message_callback = requires { typename extract_arg<T>::type; }
    && std::derived_from<typename extract_arg<T>::type, zip::api::message<typename extract_arg<T>::type>>;

/** Get the type of the message for callback. */
template <is_message_callback T>
using callback_arg_t = extract_arg<T>::type;

} // namespace detail

/** Base case for the functions `apply`. */
static inline void apply(zip::util::logger& logger, void* buffer, [[maybe_unused]] uint32_t length) {
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
template <detail::is_message_callback Arg, typename... Args>
static inline void apply(zip::util::logger& logger, void* buffer, uint32_t length, Arg function, Args... functions) {
    // get the type of the function argument, and compare
    // the tag of the message to the tag of the type
    using T = detail::callback_arg_t<Arg>;
    if (*static_cast<uint32_t*>(buffer) == T::tag) {
        auto& message = *std::launder(static_cast<T*>(buffer));
        ZIP_ASSERT_EQ(length, message.length(), "length of the received packet is invalid");
        function(message);
    } else {
        apply(logger, buffer, length, functions...);
    }
}

/**
 * Transport-agnostic variant used by abstract transport endpoints.
 */
template <typename... Args>
static inline void recv_apply(zip::util::logger& logger, zip::network::recv_endpoint& recv_endpoint, Args... functions) {
    recv_endpoint.poll_once([&] (void* buffer, unsigned long length) {
        apply(logger, buffer, length, functions...);
    });
}

template <typename... Args>
static inline void recv_apply(zip::util::logger& logger, zip::network::recv_endpoint& recv_endpoint, uint32_t timeout, Args... functions) {
    recv_endpoint.poll(timeout, [&] (void* buffer, unsigned long length) {
        apply(logger, buffer, length, functions...);
    });
}
} // namespace zip::util
