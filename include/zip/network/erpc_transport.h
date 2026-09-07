#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

#include <rpc.h>

#include <zip/network/manager.h>
#include <zip/network/transport.h>

namespace erpc {
class Nexus;
template <class TTransport>
class Rpc;
}

namespace zip::network {

/**
 * eRPC receive endpoint scaffold.
 *
 * This keeps native eRPC receive state and an application-side queue for
 * payloads dispatched by the transport's request handler.
 */
class erpc_recv_endpoint final: public recv_endpoint {
public:
    explicit erpc_recv_endpoint(std::string endpoint_id);
    ~erpc_recv_endpoint() override;

    unsigned long blocking_recv(buffer& buf) override;
    void poll_once(const std::function<void(void*, unsigned long)>& callback) override;
    void poll(uint32_t timeout, const std::function<void(void*, unsigned long)>& callback) override;

    [[nodiscard]] const std::string& endpoint_id() const override;

private:
    void enqueue_native_message(const void* data, unsigned long length);
    bool try_pop_native_message(std::vector<uint8_t>& out_payload);
    bool wait_pop_native_message(std::vector<uint8_t>& out_payload, std::chrono::milliseconds timeout);

    friend class erpc_transport_factory;

    std::string endpoint_id_;
    uint16_t native_port_ = 0;
    uint8_t local_rpc_id_ = 0;
    std::mutex native_lock_;
    std::condition_variable native_cv_;
    std::deque<std::vector<uint8_t>> native_queue_;
};

/**
 * eRPC send endpoint scaffold.
 *
 * This mirrors the LazyLog layering pattern where transport-specific endpoint
 * objects isolate session/runtime internals from role logic.
 */
class erpc_send_endpoint final: public send_endpoint {
public:
    explicit erpc_send_endpoint(std::string endpoint_id, uint8_t remote_rpc_id);
    ~erpc_send_endpoint() override;

    void send(void* message, unsigned long length) override;
    void send(buffer& buf, unsigned long length) override;
    unsigned long request_reply(
        void* send_buf,
        unsigned long send_length,
        buffer& recv_buf,
        recv_endpoint& recv_ep
    ) override;
    [[nodiscard]] const std::string& endpoint_id() const override;

private:
    std::string endpoint_id_;
    uint8_t remote_rpc_id_;
};

/**
 * Native eRPC transport factory.
 *
 * This factory owns the eRPC runtime and endpoint/session bookkeeping used by
 * the native transport backend.
 */
class erpc_transport_factory final: public transport_factory {
public:
    explicit erpc_transport_factory(manager& manager, uint8_t erpc_value, std::string address, uint16_t port);

    void initialize() override;
    void initialize_connection(const std::string& remote_uri) override;
    void finalize() override;

    std::unique_ptr<buffer> get_buffer() override;
    std::unique_ptr<recv_endpoint> create_recv_endpoint(uint16_t port = -1) override;
    std::unique_ptr<send_endpoint> create_send_endpoint(const std::string& addr, uint16_t port, uint8_t remote_rpc_id) override;
    std::unique_ptr<send_endpoint> create_send_endpoint(const std::string& url, uint8_t remote_rpc_id) override;
    std::unique_ptr<send_endpoint> create_send_endpoint(zip::util::net_info net_info, uint8_t remote_rpc_id) override;
    std::unique_ptr<send_endpoint> create_send_endpoint(zip::api::msg_net_info net_info, uint8_t remote_rpc_id) override;
    static void set_local_rpc_id_(uint8_t local_rpc_id);
    static void set_global_rpc_id_(uint8_t local_rpc_id);

    // LazyLog-style runtime ownership scaffolding for native eRPC rollout.
    static std::string addr_;
    static uint16_t port_;
    static std::mutex init_lock_;
    static std::string local_uri_;
    static erpc::Nexus* nexus_;
    static std::atomic<uint8_t> global_rpc_id_;
    static thread_local uint8_t updated_global_rpc_id_;
    static thread_local std::atomic<int> rpc_use_count_;
    static thread_local erpc::Rpc<erpc::CTransport>* rpc_;
    static thread_local std::unordered_map<std::string, int> session_cache_;
    static thread_local std::unordered_map<std::string, uint8_t> remote_rpc_id_cache_;
    static thread_local uint8_t local_rpc_id_;
    static std::atomic<bool> native_transport_enabled_;
    static uint8_t erpc_value_;
    static std::mutex recv_registry_lock_;
    static std::unordered_map<uint8_t, erpc_recv_endpoint*> recv_registry_;

    // Next native eRPC slice plugs into these helpers, following LazyLog's
    // transport runtime model.
    static void run_event_loop_once();
    static void run_event_loop(uint32_t timeout);
    static int get_or_create_session(const std::string& remote_uri, uint8_t remote_rpc_id);
    static bool native_transport_enabled();
    static bool dispatch_native_message(uint8_t local_rpc_id, const void* data, unsigned long length);

private:
    manager& manager_;
};

} // namespace zip::network

