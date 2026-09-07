#pragma once
#include "../api/types.h"
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <iostream>

using namespace ziplog::api;

namespace ziplog::impl
{

    class LogTracker
    {
    public:
        LogTracker()
        {
            log_.push_back(Command()); // index 0 placeholder
        }

        // observe takes raw data pointer instead of Command
        std::vector<std::pair<SequenceNumber, int64_t>> observe(
            NodeId sender, SequenceNumber seq,
            const uint8_t *data, size_t data_len,
            size_t quorum)
        {
            std::vector<std::pair<SequenceNumber, int64_t>> to_print;
            {
                std::lock_guard<std::mutex> lock(mu_);
                if (applied_.count(seq))
                    return to_print;

                pending_quorum_[seq].insert(sender);
                if (pending_quorum_[seq].size() >= quorum)
                {
                    apply_locked(seq, data, data_len, to_print);
                    applied_.insert(seq);
                    pending_quorum_.erase(seq);
                }
            }
            return to_print;
        }

        // wait until log has at least `count` committed entries (blocking)
        void wait_for_size(size_t count)
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [&]()
                     { return log_.size() > count; });
        }

        // wait until next_seq passes a given sequence number (blocking)
        void wait_for_seq(SequenceNumber seq)
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [&]()
                     { return next_seq_ > seq; });
        }

        // returns raw log (index 0 is empty placeholder)
        const vector<vector<uint8_t>> &raw() const { return log_; }

        // expands batches and strips skips
        std::vector<std::vector<Command>> expand() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::vector<std::vector<Command>> result;
            for (const auto &entry : log_)
            {
                vector<Command> batch = CommandBatch::deserialize(entry);
                if (!batch.empty())
                    result.push_back(batch);
            }
            return result;
        }

        // expands log (batched commands to )
        std::vector<Command> expand_unraveled() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::vector<Command> result;
            for (const Command &entry : log_)
            {
                std::vector<Command> batch = CommandBatch::deserialize(entry);
                if (!batch.empty())
                {
                    for (Command &c : batch)
                        result.push_back(c);
                }
            }
            return result;
        }

        size_t size() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return log_.size();
        }

        SequenceNumber next_seq() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return next_seq_;
        }

        void print() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::cout << "Current log..." << std::endl;
            for (size_t i = 1; i < log_.size(); i++)
            {
                std::cout << "Index " << i << ": "
                          << std::string(log_[i].begin(), log_[i].end()) << std::endl;
            }
        }

        void print_expanded() const
        {
            auto expanded = expand();
            std::cout << "-------- expanded log (" << expanded.size() << ") --------" << std::endl;
            for (size_t i = 0; i < expanded.size(); i++)
            {
                std::cout << "index " << i + 1 << ": \n";
                /*
                for (const auto &c : expanded[i])
                    std::cout << command_to_string(c);
                */
            }
        }

        void print_pending() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::cout << "-------- pending (" << pending_quorum_.size() << ") --------" << std::endl;
            for (const auto &[seq, servers] : pending_quorum_)
                std::cout << "seq " << seq << ": " << servers.size() << " servers" << std::endl;
        }

        void print_out_of_order() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::cout << "-------- out of order (" << out_of_order_.size() << ") --------" << std::endl;
            for (const auto &[seq, cmd] : out_of_order_)
                std::cout << "seq " << seq << ": \n";
        }

    private:
        // called with mu_ already held
        void apply_locked(SequenceNumber seq, const uint8_t *data, size_t data_len,
                          std::vector<std::pair<SequenceNumber, int64_t>> &to_print)
        {
            Timestamp received = now();

            // one copy
            out_of_order_[seq] = (data && data_len > 0)
                                     ? std::vector<uint8_t>(data, data + data_len)
                                     : std::vector<uint8_t>{};

            while (out_of_order_.count(next_seq_))
            {
                auto &raw = out_of_order_[next_seq_];
                vector<Command> commands = CommandBatch::deserialize(raw);

                for (auto &command : commands)
                {
                    auto it = std::find(command.begin(), command.end(), '|');
                    if (it != command.end())
                    {
                        string ts_str(command.begin(), it);
                        if (!ts_str.empty() && std::all_of(ts_str.begin(), ts_str.end(),
                                                           [](unsigned char c)
                                                           { return std::isdigit(c); }))
                        {
                            Timestamp sent = std::stoull(ts_str);
                            int64_t latency = static_cast<int64_t>(received) - static_cast<int64_t>(sent);
                            to_print.emplace_back(next_seq_, latency);
                        }
                    }
                }

                log_.push_back(std::move(out_of_order_[next_seq_]));
                out_of_order_.erase(next_seq_);
                next_seq_++;
            }
            cv_.notify_all();
        }

        mutable std::mutex mu_;
        std::condition_variable cv_;

        vector<vector<uint8_t>> log_;
        std::map<SequenceNumber, vector<uint8_t>> out_of_order_;
        SequenceNumber next_seq_{1};

        // quorum tracking
        std::unordered_map<SequenceNumber, std::set<NodeId>> pending_quorum_;
        std::unordered_set<SequenceNumber> applied_;
    };

} // namespace ziplog::impl