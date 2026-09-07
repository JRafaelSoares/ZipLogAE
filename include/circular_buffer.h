#pragma once
#include "common.h"
#include <condition_variable>

using std::condition_variable;

namespace ziplog
{
    namespace impl
    {
        /*
            @brief: thread-safe blocking push and non-blockign pop
            @param T Type of items to store
        */
        template <typename T>
        class CircularBuffer
        {
        private:
            vector<T> buffer_;
            size_t head_ = 0;
            size_t tail_ = 0;
            size_t size_ = 0;
            size_t capacity_;
            mutable mutex mu_;
            condition_variable cv_;
            static constexpr size_t BUFFER_SIZE = 16; // constant we can make configurable later

        public:
            CircularBuffer(size_t capacity = BUFFER_SIZE) : capacity_(capacity)
            {
                buffer_.resize(capacity);
            }

            // blocking push
            bool push(const T &item)
            {
                std::unique_lock<mutex> lock(mu_);

                // wait until there is space in buffer
                cv_.wait(lock, [this]()
                         { return size_ < capacity_; });

                buffer_[tail_] = item;
                tail_ = (tail_ + 1) % capacity_;
                size_++;
                return true;
            }

            // non-blocking push
            bool try_push(const T &item)
            {
                std::lock_guard<std::mutex> lock(mu_);
                if (size_ == capacity_)
                    return false;

                buffer_[tail_] = item;
                tail_ = (tail_ + 1) % capacity_;
                size_++;

                return true;
            }

            bool pop(T &item)
            {
                lock_guard<mutex> lock(mu_);
                if (size_ == 0)
                    return false;

                item = buffer_[head_];
                head_ = (head_ + 1) % capacity_;
                size_--;

                // notify waiting pushers that slot is available
                cv_.notify_one();

                return true;
            }

            bool peek(T &item)
            {
                lock_guard<mutex> lock(mu_);
                if (size_ == 0)
                    return false;

                item = buffer_[head_];
                return true;
            }

            vector<T> drain_all()
            {
                lock_guard<mutex> lock(mu_);
                vector<T> result;
                while (size_ > 0)
                {
                    result.push_back(buffer_[head_]);
                    head_ = (head_ + 1) % capacity_;
                    size_--;
                }
                return result;
            }

            size_t size() const
            {
                lock_guard<mutex> lock(mu_);
                return size_;
            }

            bool empty()
            {
                lock_guard<mutex> lock(mu_);
                return size_ == 0;
            }

            bool full()
            {
                lock_guard<mutex> lock(mu_);
                return size_ == capacity_;
            }
        };

        struct PendingRequest
        {
            Command cmd;
            int client_socket;

            PendingRequest() : cmd(), client_socket(-1) {}
            PendingRequest(const Command &c, int sock) : cmd(c), client_socket(sock) {}
        };
    }
}