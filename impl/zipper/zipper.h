#pragma once
#include "base_node.h"
#include "connection_pool.h"
#include "zipper/proxy_state.h"
#include "zipper/slot_allocator.h"
#include "zipper/reconfig_manager.h"
#include "zipper/zipper_epoch_timer.h"
#include <future>
#include <deque>

using namespace ziplog::api;
using std::deque;

namespace ziplog::impl
{

    class Zipper : public BaseNode<ZipperConfig>
    {
    private:
        // --- shared state ---
        ProxyRegistry registry_;
        ConnectionPool connection_pool_;
        mutex mu_;
        deque<pair<string, int>> joining_proxies_;

        // --- components ---
        SlotAllocator slot_allocator_;
        ReconfigManager reconfig_manager_;
        ZipperEpochTimer epoch_timer_;

        // --- message handling ---
        void handle_connection(int proxy_socket) override;
        void update_slot_estimate(const Message &msg);
        void deliver_slot_allocation(NodeId proxy_id, const vector<SequenceNumber> &values);

        // --- proxy/subscriber membership ---
        void add_proxy(const Message &msg, bool is_new);
        void introduce_proxies();
        void introduce_subscriber(const Message &msg);

        // --- epoch ---
        void allocate_slots();

    public:
        Zipper(const ZipperConfig &cfg);
        ~Zipper();
        void shutdown() override;

        size_t num_proxies() const { return config_.proxies.size(); }
        size_t num_servers() const { return config_.servers.size(); }
        size_t num_subscribers() const { return config_.subscribers.size(); }
    };
} // namespace ziplog::impl