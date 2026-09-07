#pragma once
#include "types.h"
#include "address.h"
#include <unordered_map>
#include <set>
#include <vector>
#include <mutex>

using namespace ziplog::api;

namespace ziplog::impl
{

    enum class ProxyStatus
    {
        ACTIVE,
        RECONFIGURING, // freeze in progress
        BLOCKED        // freeze complete, awaiting rejoin
    };

    struct ProxyState
    {
        // membership
        Address address;

        // slot allocation
        size_t estimate = 0;
        std::vector<SequenceNumber> allocated_sequences = {};

        // reconfiguration
        ProxyStatus status = ProxyStatus::ACTIVE;
        int freeze_round = 0;
        std::set<NodeId> freeze_responders = {};
        std::set<NodeId> reporters = {};
        std::set<SequenceNumber> last_sequences = {};
    };

    // Shared registry - both SlotAllocator and ReconfigManager hold a reference
    class ProxyRegistry
    {
    public:
        void add_proxy(NodeId id, Address addr)
        {
            std::lock_guard<std::mutex> lock(mu_);
            proxies_[id] = ProxyState{.address = addr};
        }

        void remove_proxy(NodeId id)
        {
            std::lock_guard<std::mutex> lock(mu_);
            proxies_.erase(id);
        }

        // reset state on rejoin, keep address
        void rejoin_proxy(NodeId id)
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = proxies_.find(id);
            if (it == proxies_.end())
                return;
            Address addr = it->second.address;
            it->second = ProxyState{.address = addr};
        }

        bool exists(NodeId id) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return proxies_.count(id) > 0;
        }

        // private - no lock (for use when mu_ already held)
        bool exists_unlocked(NodeId id) const { return proxies_.count(id) > 0; }
        ProxyState &get_unlocked(NodeId id) { return proxies_.at(id); }

        // access with lock held by caller
        ProxyState &get(NodeId id) { return proxies_.at(id); }
        const ProxyState &get(NodeId id) const { return proxies_.at(id); }

        std::unordered_map<NodeId, ProxyState> &all() { return proxies_; }
        const std::unordered_map<NodeId, ProxyState> &all() const { return proxies_; }

        std::mutex &mutex() { return mu_; }

    private:
        mutable std::mutex mu_;
        std::unordered_map<NodeId, ProxyState> proxies_;
    };

} // namespace ziplog::impl