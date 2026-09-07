#pragma once
#include "../api/types.h"
#include <unordered_map>
#include <deque>
#include <mutex>

using namespace ziplog::api;

namespace ziplog::impl
{

    // Thread-safe store of messages per proxy.
    // Shared between Server, FreezeHandler, and SubscriberBroadcaster.
    class MessageStore
    {
    public:
        void store(NodeId proxy_id, const Message &msg)
        {
            std::lock_guard<std::mutex> lock(mu_);
            messages_[proxy_id].push_back(msg);
            if (msg.type == APPEND)
            {
                Timestamp send_time = now();
                // cout << "[server *] msg " << msg.get_sequence_number() << " stored at " << std::to_string(send_time) << std::endl;
            }
        }

        // returns a snapshot copy of all messages for a proxy
        std::deque<Message> get(NodeId proxy_id) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = messages_.find(proxy_id);
            if (it == messages_.end())
                return {};
            return it->second;
        }

        // returns a full snapshot of all proxy messages
        std::unordered_map<NodeId, std::deque<Message>> snapshot() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return messages_;
        }

        bool has(NodeId proxy_id) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return messages_.count(proxy_id) > 0;
        }

        std::mutex &mutex() { return mu_; }

    private:
        mutable std::mutex mu_;
        std::unordered_map<NodeId, std::deque<Message>> messages_;
    };

} // namespace ziplog::impl