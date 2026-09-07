#pragma once
#include "types.h"
#include "zipper/proxy_state.h"
#include <unordered_map>
#include <vector>

using namespace ziplog::api;

namespace ziplog::impl
{

    class SlotAllocator
    {
    public:
        // takes shared registry - source of truth for estimates + allocated seqs
        explicit SlotAllocator(ProxyRegistry &registry, SequenceNumber starting_seq = 1);

        void update_estimate(NodeId proxy_id, size_t num_requests);

        // returns {proxy_id -> [timestamp, seq, timestamp, seq, ...]}
        // skips proxies that are RECONFIGURING or BLOCKED
        std::unordered_map<NodeId, std::vector<SequenceNumber>>
        compute_allocations(Timestamp next_epoch, Timestamp epoch_duration_ms);

    private:
        ProxyRegistry &registry_; // shared, not owned
        SequenceNumber global_seq_num_;

        std::mutex estimates_mu_;
        std::unordered_map<NodeId, size_t> estimates_;
    };

} // namespace ziplog::impl