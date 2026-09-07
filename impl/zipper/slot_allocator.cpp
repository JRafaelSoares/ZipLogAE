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
        std::lock_guard<std::mutex> lock(estimates_mu_);
        estimates_[proxy_id] = num_requests;
    }

    std::unordered_map<NodeId, std::vector<SequenceNumber>>
    SlotAllocator::compute_allocations(Timestamp next_epoch, Timestamp epoch_duration_ms)
    {
        // snapshot estimates without holding registry lock
        std::unordered_map<NodeId, size_t> snapshot;
        {
            std::lock_guard<std::mutex> lock(estimates_mu_);
            snapshot = estimates_;
        }

        std::vector<std::pair<double, NodeId>> timestamps;

        {
            // cout << "[slot_allocator] waiting for lock..." << endl;
            std::lock_guard<std::mutex> lock(registry_.mutex());
            // cout << "[slot_allocator] got lock" << endl;

            for (auto &[proxy_id, state] : registry_.all())
            {
                if (state.status != ProxyStatus::ACTIVE)
                {
                    // cout << "proxy " << proxy_id << " not active skipping" << endl;
                    continue;
                }

                size_t estimate = snapshot.count(proxy_id) ? snapshot.at(proxy_id) : 0;
                if (estimate == 0)
                {
                    // cout << "proxy doesnt want slots" << endl;
                    continue;
                }

                ZLOG("[zipper] proxy " << proxy_id << " wants " << estimate << " slots");

                double interval = static_cast<double>(epoch_duration_ms) / estimate;
                double time_point = interval / 2.0;

                for (size_t i = 0; i < estimate; i++)
                {
                    timestamps.push_back({time_point, proxy_id});
                    time_point += interval;
                }
            }
        }

        std::sort(timestamps.begin(), timestamps.end());

        std::unordered_map<NodeId, std::vector<SequenceNumber>> result;
        for (const auto &[time_point, proxy_id] : timestamps)
        {
            SequenceNumber seq = global_seq_num_++;
            result[proxy_id].push_back(next_epoch + static_cast<Timestamp>(time_point));
            result[proxy_id].push_back(seq);
            registry_.get(proxy_id).allocated_sequences.push_back(seq);
        }

        // cout << "[zipper] compute_allocations() exited" << endl;
        return result;
    }

} // namespace ziplog::impl