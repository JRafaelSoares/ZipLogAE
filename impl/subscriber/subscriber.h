#pragma once
#include "base_node.h"
#include "connection_pool.h"
#include "log_tracker.h"

using namespace ziplog::api;

namespace ziplog::impl
{

    class Subscriber : public BaseNode<SubscriberConfig>
    {
    public:
        Subscriber(const SubscriberConfig &cfg);
        Subscriber(const SubscriberConfig &cfg, bool registered);
        ~Subscriber();

        void shutdown() override;
        void wait_for_log_size(size_t n) { log_.wait_for_size(n); }
        const LogTracker &log() const { return log_; }

    private:
        LogTracker log_;
        ConnectionPool connection_pool_;

        void handle_connection(int server_sock) override;
    };

} // namespace ziplog::impl