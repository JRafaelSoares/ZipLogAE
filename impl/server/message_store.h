#pragma once
#include "../api/types.h"
#include <unordered_map>
#include <deque>
#include <vector>
#include <mutex>

using namespace ziplog::api;

namespace ziplog::impl
{
    class MessageStore
    {
    public:
        // store raw wire bytes — no Message construction
        void store(NodeId proxy_id, const uint8_t *data, size_t len)
        {
            std::lock_guard<std::mutex> lock(mu_);
            messages_[proxy_id].emplace_back(data, data + len); // one copy
        }

        // returns snapshot of raw wire bytes per proxy
        std::unordered_map<NodeId, std::deque<std::vector<uint8_t>>> snapshot() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return messages_;
        }

        bool has(NodeId proxy_id) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return messages_.count(proxy_id) > 0;
        }

    private:
        mutable std::mutex mu_;
        std::unordered_map<NodeId, std::deque<std::vector<uint8_t>>> messages_;
    };
}