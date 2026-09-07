#pragma once
#include "base_node.h"
#include "connection_pool.h"
#include "slot_scheduler.h"
#include "server_replicator.h"
#include "client_buffer_manager.h"
#include "proxy_epoch_timer.h"

using namespace ziplog::api;

namespace ziplog::impl
{

    class Proxy : public BaseNode<ProxyConfig>
    {
    public:
        Proxy(const ProxyConfig &cfg);
        Proxy(const ProxyConfig &cfg, bool registered);
        ~Proxy();

        void attempt_join(bool is_new);
        size_t num_servers() const { return config_.servers.size(); }

    private:
        std::atomic<SequenceNumber> next_seq_{1};
        Timestamp epoch_duration_{0};

        // --- components ---
        ProxyEpochTimer epoch_timer_;
        ServerReplicator replicator_;
        ClientBufferManager client_buffers_;
        SlotScheduler slot_scheduler_;

        // --- zipper connection ---
        ConnectionPool zipper_pool_;
        atomic<bool> registered_{true};

        // --- epoch callbacks (called by epoch_timer_) ---
        void update_slot_estimate();
        void send_out_batch(SequenceNumber seq);

        // --- message handling ---
        void handle_connection(int client_socket) override;
        void handle_zip_response(const Message &msg);
    };
} // namespace ziplog::impl