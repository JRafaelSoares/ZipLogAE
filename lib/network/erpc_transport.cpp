#include <zip/util/logger.h>

#include <zip/network/erpc_transport.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>


#include <rpc.h>

#include <zip/network/erpc_constants.h>

namespace zip::network {

static zip::util::logger logger("erpc_transport");
std::string erpc_transport_factory::addr_;
uint16_t erpc_transport_factory::port_;
std::mutex erpc_transport_factory::init_lock_;
erpc::Nexus* erpc_transport_factory::nexus_ = nullptr;
std::atomic<uint8_t> erpc_transport_factory::global_rpc_id_(0);
thread_local std::atomic<int> erpc_transport_factory::rpc_use_count_(0);
thread_local erpc::Rpc<erpc::CTransport>* erpc_transport_factory::rpc_ = nullptr;
thread_local std::unordered_map<std::string, int> erpc_transport_factory::session_cache_;
thread_local std::unordered_map<std::string, uint8_t> erpc_transport_factory::remote_rpc_id_cache_;
thread_local uint8_t erpc_transport_factory::local_rpc_id_ = 0;
thread_local uint8_t erpc_transport_factory::updated_global_rpc_id_ = std::numeric_limits<uint8_t>::max();

std::string erpc_transport_factory::local_uri_;
std::atomic<bool> erpc_transport_factory::native_transport_enabled_(false);
std::mutex erpc_transport_factory::recv_registry_lock_;
std::unordered_map<uint8_t, erpc_recv_endpoint*> erpc_transport_factory::recv_registry_;
uint8_t erpc_transport_factory::erpc_value_;


namespace {

// Structure used to wait for completion of native RPC requests
struct native_rpc_waiter {
    std::atomic<bool> complete {false};
};

// Completion callback for native RPC requests. Sets the waiter flag to signal
// that the RPC request has been completed.
void native_rpc_cont([[maybe_unused]] void* context, void* tag) {
    auto* waiter = static_cast<native_rpc_waiter*>(tag);
    waiter->complete.store(true, std::memory_order_release);
}

// eRPC request type ID for ZipLog native messages
constexpr uint8_t kZiplogReqType = ZIPLOG_REQUEST_TYPE;
// Timeout in milliseconds for native RPC requests
constexpr int kNativeRequestTimeoutMs = NATIVE_REQUEST_TIMEOUT_MS;

// Wire header structure for native messages sent via eRPC.
// The receiving rpc-id already determines the destination thread, so the
// header only needs to carry payload length.
struct native_wire_header {
    uint32_t payload_length;
};

constexpr std::size_t kNativePayloadLengthOffset = 0;

void write_native_wire_header(void* dst, uint32_t payload_length) {
    auto* bytes = static_cast<uint8_t*>(dst);
    std::memcpy(bytes + kNativePayloadLengthOffset, &payload_length, sizeof(payload_length));
}

native_wire_header read_native_wire_header(const void* src) {
    native_wire_header header{};
    const auto* bytes = static_cast<const uint8_t*>(src);
    std::memcpy(&header.payload_length, bytes + kNativePayloadLengthOffset, sizeof(header.payload_length));
    return header;
}

// Size of the native wire header
constexpr std::size_t kNativeHeaderSize = NATIVE_HEADER_SIZE;
// Size of the ACK response for native requests
constexpr std::size_t kNativeAckSize = NATIVE_ACK_SIZE;
// Timeout in milliseconds for blocking receive operations
constexpr int kNativeBlockingRecvYieldMs = BLOCKING_RECV_TIMEOUT_MS;

// Allocate a unique local RPC ID for this thread/process
// Uses atomic allocation to ensure no conflicts across multiple ZipLog roles
uint8_t select_local_rpc_id() {
    // Local RPC IDs must be unique per thread/process; deriving them from the
    // listener port can collide when multiple ZipLog roles bootstrap eRPC in
    // the same process. LazyLog uses a monotonic atomic allocation here.

    if (erpc_transport_factory::updated_global_rpc_id_ != std::numeric_limits<uint8_t>::max())
    {
        return static_cast<uint8_t>(
        erpc_transport_factory::updated_global_rpc_id_ + erpc_transport_factory::local_rpc_id_
    );
    }
    return static_cast<uint8_t>(
        erpc_transport_factory::erpc_value_ + erpc_transport_factory::local_rpc_id_
    );
}

// Attempt to establish an eRPC session with a remote endpoint
// Tries repeatedly to connect and caches the successful session
int try_connect_session(const std::string& cache_key, const std::string& remote_uri, uint8_t remote_rpc_id) {
    const auto session = erpc_transport_factory::rpc_->create_session(remote_uri, remote_rpc_id);
    constexpr int kMaxConnectAttempts = 200;
    for (int i = 0; i < kMaxConnectAttempts; i++) {
        if (erpc_transport_factory::rpc_->is_connected(session)) {
            erpc_transport_factory::session_cache_.emplace(cache_key, session);
            return session;
        }
        erpc_transport_factory::run_event_loop_once();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return -1;
}

// Generate a cache key for session lookup based on remote URI and RPC ID
std::string session_cache_key(const std::string& remote_uri, uint8_t remote_rpc_id) {
    return remote_uri + "#" + std::to_string(static_cast<unsigned>(remote_rpc_id));
}

// eRPC request handler for native ZipLog messages.
// The current rpc-id identifies the receiving thread, so the payload is
// dispatched by rpc-id rather than by any port value in the message body.
void native_req_handler(erpc::ReqHandle* req_handle, [[maybe_unused]] void* context) {
    const auto* req = req_handle->get_req_msgbuf();
    auto& resp = req_handle->pre_resp_msgbuf_;
    auto* rpc_instance = erpc_transport_factory::rpc_;
    if (rpc_instance == nullptr) {
        return;
    }

    uint32_t ack = 0;
    if (req != nullptr && req->get_data_size() >= kNativeHeaderSize) {
        const auto header = read_native_wire_header(req->buf_);
        const auto payload_length = static_cast<std::size_t>(header.payload_length);
        if (req->get_data_size() >= kNativeHeaderSize + payload_length) {
            const auto* payload = reinterpret_cast<const uint8_t*>(req->buf_) + kNativeHeaderSize;
            if (erpc_transport_factory::dispatch_native_message(static_cast<uint8_t>(rpc_instance->get_rpc_id()), payload, payload_length)) {
                ack = 1;
            }
        }
    }

    erpc::Rpc<erpc::CTransport>::resize_msg_buffer(&resp, kNativeAckSize);
    std::memcpy(resp.buf_, &ack, sizeof(ack));
    rpc_instance->enqueue_response(req_handle, &resp);
}

// Send a native message via eRPC to a remote endpoint.
// The remote rpc-id selects the destination thread directly; the wire header
// only records payload length for validation.
bool send_native_request(const std::string& endpoint_id, void* send_buf, unsigned long send_length, uint8_t remote_rpc_id) {
    const auto session = erpc_transport_factory::get_or_create_session(endpoint_id, remote_rpc_id);
    if (session < 0 || erpc_transport_factory::rpc_ == nullptr) {
        return false;
    }

    const auto total_size = kNativeHeaderSize + send_length;
    auto req = erpc_transport_factory::rpc_->alloc_msg_buffer_or_die(total_size);
    auto resp = erpc_transport_factory::rpc_->alloc_msg_buffer_or_die(kNativeAckSize);

    native_wire_header header {
        .payload_length = static_cast<uint32_t>(send_length)
    };
    write_native_wire_header(req.buf_, header.payload_length);

    if (send_length > 0) {
        std::memcpy(reinterpret_cast<uint8_t*>(req.buf_) + kNativeHeaderSize, send_buf, send_length);
    }

    erpc::Rpc<erpc::CTransport>::resize_msg_buffer(&req, total_size);


    native_rpc_waiter waiter;
    erpc_transport_factory::rpc_->enqueue_request(
        session,
        kZiplogReqType,
        &req,
        &resp,
        native_rpc_cont,
        &waiter
    );

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kNativeRequestTimeoutMs);
    while (!waiter.complete.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
        erpc_transport_factory::run_event_loop_once();
    }

    bool delivered = false;
    if (waiter.complete.load(std::memory_order_acquire) && resp.get_data_size() >= sizeof(uint32_t)) {
        uint32_t ack = 0;
        std::memcpy(&ack, resp.buf_, sizeof(ack));
        delivered = (ack == 1);
    }

    erpc_transport_factory::rpc_->free_msg_buffer(resp);
    erpc_transport_factory::rpc_->free_msg_buffer(req);
    return delivered;
}

} // namespace

// eRPC session management event handler
// Placeholder for handling eRPC session events (connect, disconnect, timeout, etc.)
void erpc_sm_handler([[maybe_unused]] int session_num,
                     [[maybe_unused]] erpc::SmEventType event_type,
                     [[maybe_unused]] erpc::SmErrType err_type,
                     [[maybe_unused]] void* context) {
}

// Bootstrap the native eRPC runtime if not already initialized
// Creates a Nexus and RPC instance on first call, registers the native request handler
// Ensures proper initialization with global lock protection
bool ensure_native_runtime() {
    if (!erpc_transport_factory::native_transport_enabled()) {
        return false;
    }

    auto lock = std::lock_guard(erpc_transport_factory::init_lock_);

    if (erpc_transport_factory::nexus_ == nullptr) {
        erpc_transport_factory::nexus_ = new erpc::Nexus(erpc_transport_factory::local_uri_);
        erpc_transport_factory::nexus_->register_req_func(kZiplogReqType, native_req_handler);
    }
    if (erpc_transport_factory::rpc_ == nullptr) {
        const auto rpc_id = select_local_rpc_id();
        erpc_transport_factory::rpc_ = new erpc::Rpc<erpc::CTransport>(
            erpc_transport_factory::nexus_,
            nullptr,
            rpc_id,
            erpc_sm_handler,
            0
        );
    }

    return true;
}

// Constructor: Initialize a receive endpoint for eRPC transport.
// Each endpoint is registered by rpc-id so one service port can fan out to
// multiple thread-local endpoints without any port switching.
erpc_recv_endpoint::erpc_recv_endpoint(std::string endpoint_id)
: endpoint_id_(std::move(endpoint_id)) {
    local_rpc_id_ = select_local_rpc_id();
    auto lock = std::lock_guard(erpc_transport_factory::recv_registry_lock_);
    erpc_transport_factory::recv_registry_[local_rpc_id_] = this;

    auto init_lock = std::lock_guard(erpc_transport_factory::init_lock_);
    erpc_transport_factory::rpc_use_count_.fetch_add(1, std::memory_order_relaxed);
}

// Destructor: Clean up and unregister this receive endpoint
// Removes from global registry and decrements reference count
erpc_recv_endpoint::~erpc_recv_endpoint() {
    if (local_rpc_id_ != 0) {
        auto lock = std::lock_guard(erpc_transport_factory::recv_registry_lock_);
        if (auto it = erpc_transport_factory::recv_registry_.find(local_rpc_id_);
            it != erpc_transport_factory::recv_registry_.end() && it->second == this) {
            erpc_transport_factory::recv_registry_.erase(it);
        }
    }
    erpc_transport_factory::rpc_use_count_.fetch_sub(1, std::memory_order_relaxed);
}

// Blocking receive operation: Waits for a message from the native queue with timeout
// Runs the eRPC event loop while waiting, returns payload size or 0 on timeout
unsigned long erpc_recv_endpoint::blocking_recv(buffer& buf) {
    std::vector<uint8_t> payload;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kNativeBlockingRecvYieldMs);

    // Respect the global transport shutdown signal
    while (transport_factory::is_running() &&
           !wait_pop_native_message(payload, std::chrono::milliseconds(5))) {
        erpc_transport_factory::run_event_loop_once();
        if (std::chrono::steady_clock::now() >= deadline) {
            return 0;
        }
    }
    if (!payload.empty()) {
        std::memcpy(buf.buffer_, payload.data(), payload.size());
    }
    return payload.size();
}

// Non-blocking poll: Process one eRPC event loop iteration and check for queued messages
// Calls the provided callback if a message is found
void erpc_recv_endpoint::poll_once(const std::function<void(void*, unsigned long)>& callback) {
    erpc_transport_factory::run_event_loop_once();
    std::vector<uint8_t> payload;
    if (try_pop_native_message(payload)) {
        callback(payload.data(), payload.size());
    }
}

void erpc_recv_endpoint::poll(uint32_t timeout, const std::function<void(void*, unsigned long)>& callback) {

    erpc_transport_factory::run_event_loop(timeout);
    std::vector<uint8_t> payload;
    if (try_pop_native_message(payload)) {
        callback(payload.data(), payload.size());
    }
}

// Get the endpoint ID (host:port) for this receive endpoint
const std::string& erpc_recv_endpoint::endpoint_id() const {
    return endpoint_id_;
}

// Enqueue a native message to be received by this endpoint
// Used internally by the native request handler to dispatch incoming messages
void erpc_recv_endpoint::enqueue_native_message(const void* data, unsigned long length) {
    auto lock = std::unique_lock(native_lock_);
    native_queue_.emplace_back(length);
    if (length > 0) {
        std::memcpy(native_queue_.back().data(), data, length);
    }
    native_cv_.notify_one();
}

// Try to pop a message from the native queue without blocking
bool erpc_recv_endpoint::try_pop_native_message(std::vector<uint8_t>& out_payload) {
    auto lock = std::unique_lock(native_lock_);
    if (native_queue_.empty()) {
        return false;
    }
    out_payload = std::move(native_queue_.front());
    native_queue_.pop_front();
    return true;
}

// Wait for a message to arrive in the native queue with timeout
// Returns true and populates out_payload if a message is available, false on timeout
bool erpc_recv_endpoint::wait_pop_native_message(std::vector<uint8_t>& out_payload, std::chrono::milliseconds timeout) {
    auto lock = std::unique_lock(native_lock_);
    if (!native_cv_.wait_for(lock, timeout, [&] { return !native_queue_.empty(); })) {
        return false;
    }
    out_payload = std::move(native_queue_.front());
    native_queue_.pop_front();
    return true;
}

// Constructor: Initialize a send endpoint for eRPC transport
// Stores the endpoint ID and increments the RPC reference count
erpc_send_endpoint::erpc_send_endpoint(std::string endpoint_id, uint8_t remote_rpc_id)
: endpoint_id_(std::move(endpoint_id)), remote_rpc_id_(remote_rpc_id) {
    erpc_transport_factory::rpc_use_count_.fetch_add(1, std::memory_order_relaxed);
}

// Destructor: Clean up and decrement the reference count
erpc_send_endpoint::~erpc_send_endpoint() {
    erpc_transport_factory::rpc_use_count_.fetch_sub(1, std::memory_order_relaxed);
}

// Send a message to the remote endpoint without waiting for a response
void erpc_send_endpoint::send(void* message, unsigned long length) {
    if (!send_native_request(endpoint_id_, message, length, remote_rpc_id_)) {
        throw std::runtime_error("native eRPC send failed for endpoint " + endpoint_id_);
    }
}

// Send a message from a buffer to the remote endpoint without waiting for a response
void erpc_send_endpoint::send(buffer& buf, unsigned long length) {
    if (!send_native_request(endpoint_id_, buf.buffer_, length, remote_rpc_id_)) {
        throw std::runtime_error("native eRPC send failed for endpoint " + endpoint_id_);
    }
}

// Send a request and wait for a reply via a separate receive endpoint
// Uses the provided receive endpoint to collect the response after the transport send completes
unsigned long erpc_send_endpoint::request_reply(
    void* send_buf,
    unsigned long send_length,
    buffer& recv_buf,
    recv_endpoint& recv_ep
) {
    if (!send_native_request(endpoint_id_, send_buf, send_length, remote_rpc_id_)) {
        throw std::runtime_error("native eRPC request/reply failed for endpoint " + endpoint_id_);
    }
    // Preserve ZipLog semantics: transport-level completion is followed by
    // application-level response reception.
    return recv_ep.blocking_recv(recv_buf);
}

// Get the endpoint ID (host:port) for this send endpoint
const std::string& erpc_send_endpoint::endpoint_id() const {
    return endpoint_id_;
}

// Constructor: Initialize the eRPC transport factory
// Stores reference to the manager and enables the native eRPC transport backend
erpc_transport_factory::erpc_transport_factory(manager& manager, uint8_t erpc_value, std::string address, uint16_t port)
: manager_(manager)  {
    auto lock = std::lock_guard(init_lock_);
    addr_ = address;
    port_ = port;
    // Native eRPC backend: once this factory is selected, requests and
    // replies use eRPC end-to-end.
    native_transport_enabled_.store(true, std::memory_order_release);
    erpc_value_ = erpc_value;
    local_uri_ = std::format("{}:{}",address, port);
    logger.info("eRPC transport factory created");
}

// Initialize the eRPC transport system (called at startup)
void erpc_transport_factory::initialize() {
    auto lock = std::lock_guard(init_lock_);
    logger.info("Initializing eRPC transport system");

    // Set global running state
    transport_factory::reset_running();

    logger.info("eRPC transport initialized");
}

// Initialize a connection to a remote endpoint
void erpc_transport_factory::initialize_connection(const std::string& remote_uri) {
    if (!transport_factory::is_running()) {
        throw std::runtime_error("eRPC transport is not running");
    }
    logger.debug("Initializing connection to: " + remote_uri);
}

// Finalize the eRPC transport (called at shutdown)
void erpc_transport_factory::finalize() {
    auto lock = std::lock_guard(init_lock_);
    logger.info("Finalizing eRPC transport");

    // Signal all threads to stop
    transport_factory::stop();

    // TODO: Cleanup eRPC resources
    logger.info("eRPC transport finalized");
}

// Run a single iteration of the eRPC event loop
// Processes pending requests, responses, and connection management
void erpc_transport_factory::run_event_loop_once() {
    if (rpc_ != nullptr) {
        rpc_->run_event_loop_once();
    }
}

void erpc_transport_factory::run_event_loop(uint32_t timeout) {
    if (rpc_ != nullptr) {
        rpc_->run_event_loop(timeout);
    }
}

// Get or create an eRPC session to a remote endpoint
// Attempts to use cached sessions first, then tries connecting with the preferred RPC ID
// Scans through candidate RPC IDs if the initial attempt fails
int erpc_transport_factory::get_or_create_session(const std::string& remote_uri, uint8_t remote_rpc_id) {
    if (!ensure_native_runtime()) {
        return -1;
    }
    const auto cache_key = session_cache_key(remote_uri, remote_rpc_id);
    if (auto session_it = session_cache_.find(cache_key); session_it != session_cache_.end()) {
        return session_it->second;
    }

    return try_connect_session(cache_key, remote_uri, remote_rpc_id);
}

// Check if the native eRPC transport backend is enabled
bool erpc_transport_factory::native_transport_enabled() {
    return native_transport_enabled_.load(std::memory_order_acquire);
}

// Dispatch an incoming native message to the registered receive endpoint for a
// given rpc-id. Returns true if dispatched successfully, false if no endpoint
// is registered for that thread.
bool erpc_transport_factory::dispatch_native_message(uint8_t local_rpc_id, const void* data, unsigned long length) {
    auto lock = std::lock_guard(recv_registry_lock_);
    if (auto it = recv_registry_.find(local_rpc_id); it != recv_registry_.end() && it->second != nullptr) {
        it->second->enqueue_native_message(data, length);
        return true;
    }
    return false;
}

// Get a buffer from the manager for message processing
std::unique_ptr<buffer> erpc_transport_factory::get_buffer() {
    return manager_.get_buffer();
}

// Create a receive endpoint listening on the specified port
std::unique_ptr<recv_endpoint> erpc_transport_factory::create_recv_endpoint(uint16_t port) {
    if (!native_transport_enabled()) {
        throw std::runtime_error("native eRPC transport is disabled for endpoint " + addr_);
    }

    if (!ensure_native_runtime()) {
        throw std::runtime_error("failed to bootstrap native eRPC runtime for receive endpoint " + addr_);
    }

    if (port == static_cast<uint16_t>(-1))
    {
        return std::make_unique<erpc_recv_endpoint>(local_uri_);
    }

    return std::make_unique<erpc_recv_endpoint>(addr_ + ":" + std::to_string(static_cast<unsigned>(port)));
}

// Create a send endpoint targeting the specified address and port
std::unique_ptr<send_endpoint> erpc_transport_factory::create_send_endpoint(const std::string& addr, uint16_t port, uint8_t remote_rpc_id) {
    if (!native_transport_enabled()) {
        throw std::runtime_error("native eRPC transport is disabled for endpoint " + addr_);
    }

    if (!ensure_native_runtime()) {
        throw std::runtime_error("failed to bootstrap native eRPC runtime for receive endpoint " + addr_);
    }
    return std::make_unique<erpc_send_endpoint>(
        addr + ":" + std::to_string(static_cast<unsigned>(port)), remote_rpc_id
    );
}

// Create a send endpoint targeting the specified URL (host:port)
std::unique_ptr<send_endpoint> erpc_transport_factory::create_send_endpoint(const std::string& url, uint8_t remote_rpc_id) {
    return std::make_unique<erpc_send_endpoint>(
        url, remote_rpc_id
    );
}

// Create a send endpoint from a net_info structure containing host and port
std::unique_ptr<send_endpoint> erpc_transport_factory::create_send_endpoint(zip::util::net_info net_info, uint8_t remote_rpc_id) {
    return std::make_unique<erpc_send_endpoint>(
        net_info.host + ":" + std::to_string(static_cast<unsigned>(net_info.port)), remote_rpc_id
    );
}

// Create a send endpoint from a msg_net_info structure containing IPv4 address and port
std::unique_ptr<send_endpoint> erpc_transport_factory::create_send_endpoint(zip::api::msg_net_info net_info, uint8_t remote_rpc_id) {
    const auto ip = std::to_string(static_cast<unsigned>(net_info.ipv4[0])) + "."
                  + std::to_string(static_cast<unsigned>(net_info.ipv4[1])) + "."
                  + std::to_string(static_cast<unsigned>(net_info.ipv4[2])) + "."
                  + std::to_string(static_cast<unsigned>(net_info.ipv4[3]));
    return std::make_unique<erpc_send_endpoint>(
        ip + ":" + std::to_string(static_cast<unsigned>(net_info.port)), remote_rpc_id
    );
}

void erpc_transport_factory::set_local_rpc_id_(uint8_t local_rpc_id)
{
    local_rpc_id_ = local_rpc_id;
}

void erpc_transport_factory::set_global_rpc_id_(uint8_t local_rpc_id)
{
    updated_global_rpc_id_ = local_rpc_id;
}
} // namespace zip::network

