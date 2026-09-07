#pragma once
#include "../api/types.h"
#include <mutex>
#include <set>
#include <arpa/inet.h>

using namespace ziplog::api;

namespace ziplog::impl
{
    class ClientBufferManager
    {
    public:
        static constexpr size_t CAPACITY = 1 << 20; // 1MB pre-allocated

        struct DrainResult
        {
            const uint8_t *data; // pointer into buf_
            size_t len;          // total bytes
            std::set<int> participating;
        };

        // called from handle_connection — writes length-prefixed command
        // directly into ring buffer. one copy, zero alloc.
        bool push_raw(int client_socket, const uint8_t *data, size_t len)
        {
            std::lock_guard<std::mutex> lock(mu_);

            size_t needed = 4 + len; // CommandBatch wire format: uint32 len + data
            if (used_ + needed > CAPACITY)
                return false; // full — backpressure

            uint32_t net_len = htonl(static_cast<uint32_t>(len));
            memcpy(buf_ + write_pos_, &net_len, 4);
            memcpy(buf_ + write_pos_ + 4, data, len);
            write_pos_ += needed;
            used_ += needed;
            participating_.insert(client_socket);
            return true;
        }

        void remove(int client_socket)
        {
            std::lock_guard<std::mutex> lock(mu_);
            participating_.erase(client_socket);
        }

        // called from send_out_batch — returns pointer + len into buffer
        // zero copy. caller must call release() after send.
        DrainResult drain()
        {
            std::lock_guard<std::mutex> lock(mu_);
            DrainResult r;
            r.data = buf_ + read_pos_;
            r.len = used_;
            r.participating = std::move(participating_);
            participating_.clear();
            return r;
        }

        void release(size_t consumed)
        {
            std::lock_guard<std::mutex> lock(mu_);
            read_pos_ += consumed;
            used_ -= consumed;
            if (used_ == 0)
                read_pos_ = write_pos_ = 0; // reset to front
        }

        bool empty() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return used_ == 0;
        }

        size_t buffer_size(int) const { return used_; } // approx

    private:
        mutable std::mutex mu_;
        uint8_t buf_[CAPACITY]{};
        size_t write_pos_{0};
        size_t read_pos_{0};
        size_t used_{0};
        std::set<int> participating_;
    };
}