#pragma once
#include "../api/types.h"
#include "network_utils.h"
#include "connection_pool.h"
#include <unordered_map>
#include <deque>
#include <mutex>
#include <thread>
#include <atomic>
#include <functional>
#include <iostream>

using namespace ziplog::api;

namespace ziplog::impl
{

    class ProxyLivenessTracker
    {
    public:
        ProxyLivenessTracker(EpochDurationUnit epoch_duration,
                             EpochDurationUnit lag,
                             size_t num_proxies,
                             const Address &zipper,
                             ConnectionPool &pool)
            : epoch_duration_(epoch_duration.count()), lag_(lag.count()), num_proxies_(num_proxies), zipper_(zipper), pool_(pool) {}

        ~ProxyLivenessTracker() { stop(); }

        void start(NodeId server_id)
        {
            server_id_ = server_id;
            running_ = true;
            thread_ = std::thread(&ProxyLivenessTracker::detect_loop, this);
        }

        void stop()
        {
            running_ = false;
            if (thread_.joinable())
                thread_.join();
        }

        // called on ZIP_RESPONSE — loads [timestamp, seq, ...] pairs
        void update_timeouts(NodeId proxy_id, const std::vector<SequenceNumber> &ordering_values)
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto &tq = proxy_timeouts_[proxy_id];
            tq.insert(tq.end(), ordering_values.begin(), ordering_values.end());
        }

        // called when a message from proxy arrives — pops front timeout, records last seq
        void remove_timeout(NodeId proxy_id, SequenceNumber seq)
        {
            std::lock_guard<std::mutex> lock(mu_);

            if (blocked_for_reconfiguration_.find(proxy_id) == blocked_for_reconfiguration_.end())
            {
                auto &tq = proxy_timeouts_[proxy_id];
                if (tq.size() < 2)
                {
                    ZLOG("warning: proxy_timeouts_ too small for proxy size=" << tq.size());
                    return;
                }
                assert(tq.size() >= 2 && tq.size() % 2 == 0);
                tq.pop_front();
                last_used_sequence_number_[proxy_id] = tq.front();
                tq.pop_front();
            }
            else
            {
                if (last_used_sequence_number_[proxy_id] < seq)
                    last_used_sequence_number_[proxy_id] = seq;
            }
        }

        void block_proxy(NodeId proxy_id)
        {
            std::lock_guard<std::mutex> lock(mu_);
            ZLOG("[liveness] fully blocking proxy " << proxy_id);
            blocked_for_reconfiguration_[proxy_id] = true;
            assert(false);
        }

        // called on INCLUDE_PROXY (rejoin) — clears reconfiguration state
        void unblock_proxy(NodeId proxy_id)
        {
            std::lock_guard<std::mutex> lock(mu_);
            blocked_for_reconfiguration_.erase(proxy_id);
            proxy_timeouts_[proxy_id] = std::deque<Timestamp>();
            last_used_sequence_number_.erase(proxy_id);
        }

        bool is_blocked(NodeId proxy_id) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = blocked_for_reconfiguration_.find(proxy_id);
            return it != blocked_for_reconfiguration_.end() && it->second;
        }

        // marks proxy as reconfiguring (false = in process)
        void set_reconfiguring(NodeId proxy_id)
        {
            std::lock_guard<std::mutex> lock(mu_);
            blocked_for_reconfiguration_[proxy_id] = false;
        }

        bool is_reconfiguring(NodeId proxy_id) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return blocked_for_reconfiguration_.count(proxy_id) > 0;
        }

        SequenceNumber last_seq(NodeId proxy_id) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = last_used_sequence_number_.find(proxy_id);
            return it != last_used_sequence_number_.end() ? it->second : 0;
        }

        std::mutex &mutex() { return mu_; }

    private:
        void detect_loop()
        {
            while (running_)
            {
                Timestamp current = now();
                for (NodeId id = 0; id < static_cast<NodeId>(num_proxies_); id++)
                {
                    if (is_blocked(id))
                        continue;

                    std::unique_lock<std::mutex> lock(mu_);
                    bool should_report =
                        blocked_for_reconfiguration_.find(id) == blocked_for_reconfiguration_.end() && !proxy_timeouts_[id].empty() && current >= proxy_timeouts_[id].front() + lag_;
                    lock.unlock();

                    if (should_report)
                    {
                        ZLOG("[liveness] reporting proxy " << id);
                        ZLOG("[liveness] current=" << current
                                                   << " timeout=" << proxy_timeouts_[id].front()
                                                   << " deadline=" << proxy_timeouts_[id].front() + lag_);
                        report(id);
                    }
                }
                std::this_thread::sleep_for(EpochDurationUnit(epoch_duration_));
            }
        }

        void report(NodeId proxy_id)
        {
            Message req;
            req.type = REPORT;
            req.shard_id = 0; // set by server via callback if needed
            req.sender_id = server_id_;
            req.set_failed_proxy(proxy_id);

            int sock = pool_.get_connection(zipper_);
            if (sock < 0)
                return;
            NetworkUtils::send_message(sock, req);
        }

        Timestamp epoch_duration_;
        Timestamp lag_;
        size_t num_proxies_;
        Address zipper_;
        ConnectionPool &pool_;
        NodeId server_id_{0};

        mutable std::mutex mu_;
        std::unordered_map<NodeId, std::deque<Timestamp>> proxy_timeouts_;
        std::unordered_map<NodeId, SequenceNumber> last_used_sequence_number_;
        std::unordered_map<NodeId, bool> blocked_for_reconfiguration_;

        std::atomic<bool> running_{false};
        std::thread thread_;
    };

} // namespace ziplog::impl