#pragma once
#include "types.h"
#include "address.h"
#include "zipper/proxy_state.h"
#include "network_utils.h"
#include <functional>
#include <vector>

using namespace ziplog::api;

namespace ziplog::impl
{

    struct ReconfigCallbacks
    {
        std::function<size_t()> get_quorum;
        std::function<ShardId()> get_shard;
        std::function<std::vector<Address>()> get_servers;
        std::function<int(const Address &)> get_connection;
        std::function<void(const Address &)> close_connection;
    };

    class ReconfigManager
    {
    public:
        ReconfigManager(ProxyRegistry &registry, ReconfigCallbacks callbacks);

        void handle_report(const Message &msg);
        void handle_freeze_response(const Message &msg);

    private:
        void send_freeze(NodeId failed_proxy, bool first_round);
        void send_freeze_complete(NodeId failed_proxy, SequenceNumber last_seq);
        void broadcast_to_servers(const Message &msg);

        ProxyRegistry &registry_; // shared, not owned
        ReconfigCallbacks cb_;
    };

} // namespace ziplog::impl