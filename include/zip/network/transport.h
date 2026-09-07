#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <zip/api/api.h>
#include <zip/network/buffer.h>
#include <zip/util/util.h>

namespace zip::network {

/**
 * Abstract receive endpoint used by transport backends (e.g., ZMQ, eRPC).
 */
class recv_endpoint {
public:
    virtual ~recv_endpoint() = default;

    /** Blocking receive into the provided buffer. */
    virtual unsigned long blocking_recv(buffer& buf) = 0;

    /** Poll one message (if available) and invoke callback. */
    virtual void poll_once(const std::function<void(void*, unsigned long)>& callback) = 0;
    virtual void poll(uint32_t timeout, const std::function<void(void*, unsigned long)>& callback) = 0;

    /** Debug identifier for logging (url/session descriptor). */
    virtual const std::string& endpoint_id() const = 0;
};

/**
 * Abstract send endpoint used by transport backends (e.g., ZMQ, eRPC).
 */
class send_endpoint {
public:
    virtual ~send_endpoint() = default;

    virtual void send(void* message, unsigned long length) = 0;
    virtual void send(buffer& buf, unsigned long length) = 0;

    /**
     * Canonical request/reply helper shared by both transports.
     * The backend controls send semantics while the receive path is abstract.
     */
    virtual unsigned long request_reply(
        void* send_buf,
        unsigned long send_length,
        buffer& recv_buf,
        recv_endpoint& recv_ep
    ) = 0;

    /** Debug identifier for logging (url/session descriptor). */
    virtual const std::string& endpoint_id() const = 0;
};

/**
 * Factory and resource root for a transport backend.
 * Extended with lifecycle management for graceful shutdown.
 */
class transport_factory {
public:
    virtual ~transport_factory() = default;

    /// Initialize the transport infrastructure (called at startup)
    virtual void initialize() {}

    /// Initialize a connection to a remote endpoint
    virtual void initialize_connection(const std::string& remote_uri) {}

    /// Finalize the transport (called at shutdown)
    virtual void finalize() {}

    /// Global shutdown signal - stops all transport threads
    static void stop() {
        running_.store(false, std::memory_order_release);
    }

    /// Check if transport should continue running
    static bool is_running() {
        return running_.load(std::memory_order_acquire);
    }

    /// Reset running state (mainly for testing)
    static void reset_running() {
        running_.store(true, std::memory_order_release);
    }

    virtual std::unique_ptr<buffer> get_buffer() = 0;

    virtual std::unique_ptr<recv_endpoint> create_recv_endpoint(uint16_t port) = 0;

    virtual std::unique_ptr<send_endpoint> create_send_endpoint(const std::string& addr, uint16_t port, uint8_t remote_rpc_id) = 0;
    virtual std::unique_ptr<send_endpoint> create_send_endpoint(const std::string& url, uint8_t remote_rpc_id) = 0;
    virtual std::unique_ptr<send_endpoint> create_send_endpoint(zip::util::net_info net_info, uint8_t remote_rpc_id) = 0;
    virtual std::unique_ptr<send_endpoint> create_send_endpoint(zip::api::msg_net_info net_info, uint8_t remote_rpc_id) = 0;

protected:
    /// Global running flag - controls all transport threads
    /// When false, all threads should gracefully exit
    static std::atomic<bool> running_;
};

} // namespace zip::network

