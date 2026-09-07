#pragma once
#include "base_node.h"
#include "connection_pool.h"
#include "message_store.h"
#include "proxy_liveness_tracker.h"
#include "subscriber_broadcaster.h"
#include "freeze_handler.h"

using namespace ziplog::api;

namespace ziplog::impl
{

    class Server : public BaseNode<ServerConfig>
    {
    public:
        Server(const ServerConfig &cfg);
        ~Server();
        void shutdown() override;

        size_t num_proxies() const { return config_.proxies.size(); }
        size_t num_servers() const { return config_.other_servers.size(); }
        size_t num_subscribers() const { return config_.subscribers.size(); }

    private:
        // --- shared state ---
        ConnectionPool connection_pool_;
        MessageStore store_;

        // --- components ---
        ProxyLivenessTracker liveness_;
        SubscriberBroadcaster broadcaster_;
        FreezeHandler freeze_handler_;

        void handle_connection(int proxy_socket) override;
        void introduce_proxy(const Message &msg);
        void introduce_subscriber(int socket, const Message &msg);
    };

} // namespace ziplog::impl