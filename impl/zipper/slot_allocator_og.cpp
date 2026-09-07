#include "zipper/slot_allocator.h"
#include <algorithm>
#include <iostream>

using namespace ziplog::api;

namespace ziplog::impl
{

    SlotAllocator::SlotAllocator(ProxyRegistry &registry, SequenceNumber starting_seq)
        : registry_(registry), global_seq_num_(starting_seq) {}

    void SlotAllocator::update_estimate(NodeId proxy_id, size_t num_requests)
    {
        ZLOG("[update_estimate] waiting for lock...");
        std::lock_guard<std::mutex> lock(registry_.mutex());
        ZLOG("[update_estimate] got lock");
        if (!registry_.exists_unlocked(proxy_id))
        {
            ZLOG("zipper cant update estimate because proxy not registered");
            return;
        }
        registry_.get_unlocked(proxy_id).estimate = num_requests;
        ZLOG("zipper update estimate exited");
    }

    std::unordered_map<NodeId, std::vector<SequenceNumber>>
    SlotAllocator::compute_allocations(Timestamp next_epoch, Timestamp epoch_duration_ms)
    {
        ZLOG("[slot_allocator] waiting for lock...");
        std::lock_guard<std::mutex> lock(registry_.mutex());
        ZLOG("[slot_allocator] got lock");

        // build sorted timestamp list across all active proxies
        std::vector<std::pair<double, NodeId>> timestamps;

        for (auto &[proxy_id, state] : registry_.all())
        {
            ZLOG("proxy " << proxy_id << " status=" << (int)state.status << " estimate=" << state.estimate);

            if (state.status != ProxyStatus::ACTIVE)
            {
                ZLOG("proxy " << proxy_id << " not active skipping");
                continue;
            }
            if (state.estimate == 0)
            {
                ZLOG("proxy doesnt want slots");
                continue;
            }

            double interval = static_cast<double>(epoch_duration_ms) / state.estimate;
            double time_point = interval / 2.0;

            for (size_t i = 0; i < state.estimate; i++)
            {
                timestamps.push_back({time_point, proxy_id});
                time_point += interval;
            }
        }

        std::sort(timestamps.begin(), timestamps.end());

        // assign sequence numbers
        std::unordered_map<NodeId, std::vector<SequenceNumber>> result;
        for (const auto &[time_point, proxy_id] : timestamps)
        {
            SequenceNumber seq = global_seq_num_++;
            result[proxy_id].push_back(next_epoch + static_cast<Timestamp>(time_point));
            result[proxy_id].push_back(seq);
            registry_.get(proxy_id).allocated_sequences.push_back(seq);
        }

        ZLOG("[zipper] compute_allocations() exited");
        return result;
    }

} // namespace ziplog::impl