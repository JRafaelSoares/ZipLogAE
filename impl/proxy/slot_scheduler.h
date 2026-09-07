#pragma once
#include "../api/types.h"
#include <deque>
#include <mutex>
#include <cmath>
#include <cassert>

using namespace ziplog::api;

namespace ziplog::impl
{

    // Manages the proxy's allocated sequence numbers, send timeouts,
    // and the rolling estimate sent to the zipper each epoch.
    class SlotScheduler
    {
    public:
        explicit SlotScheduler(size_t max_epoch_history)
            : max_epoch_history_(max_epoch_history) {}

        // called when a ZIP_RESPONSE arrives - loads new slots
        void load_slots(const std::vector<SequenceNumber> &ordering_values)
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (size_t i = 0; i < ordering_values.size(); i++)
            {
                if (i % 2 == 0)
                    timeouts_.push_back(ordering_values[i]);
                else
                    sequences_.push_back(ordering_values[i]);
            }
            if (!timeouts_.empty())
                next_send_ = timeouts_.front();
            assert(timeouts_.size() == sequences_.size()); // include for safety
        }

        // called per incoming client request
        void record_request()
        {
            std::lock_guard<std::mutex> lock(mu_);
            request_count_++;
        }

        // called at epoch boundary - rolls history, returns new estimate, resets count
        SequenceNumber compute_estimate()
        {
            std::lock_guard<std::mutex> lock(mu_);
            estimate_history_.push_back(request_count_);
            if (estimate_history_.size() > max_epoch_history_)
                estimate_history_.pop_front();

            request_count_ = 0;

            if (estimate_history_.empty())
                return 0;

            SequenceNumber avg = 0;
            for (auto e : estimate_history_)
                avg += e;
            return static_cast<SequenceNumber>(
                std::ceil(static_cast<double>(avg) / estimate_history_.size()));
        }

        // returns {seq, send_time} and advances the deques. returns false if no slots.
        bool pop_next_slot(Timestamp &send_time_out, SequenceNumber &seq_out)
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (next_send_ == 0 || sequences_.empty())
                return false;

            seq_out = sequences_.front();
            send_time_out = timeouts_.front();

            sequences_.pop_front();
            timeouts_.pop_front();
            next_send_ = timeouts_.empty() ? 0 : timeouts_.front();
            return true;
        }

        Timestamp next_send() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return next_send_;
        }

        bool has_slots() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return !sequences_.empty();
        }

        void push_front(Timestamp ts, SequenceNumber seq)
        {
            std::lock_guard<std::mutex> lock(mu_);
            timeouts_.push_front(ts);
            sequences_.push_front(seq);
            next_send_ = timeouts_.front();
        }

    private:
        mutable std::mutex mu_;

        size_t max_epoch_history_;
        SequenceNumber request_count_{0};
        std::deque<SequenceNumber> estimate_history_;

        std::deque<SequenceNumber> sequences_;
        std::deque<Timestamp> timeouts_;
        Timestamp next_send_{0};
    };

} // namespace ziplog::impl