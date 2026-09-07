#pragma once
#include "../api/types.h"
#include "network_utils.h"
#include "connection_pool.h"
#include <unordered_map>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <iostream>

using namespace ziplog::api;

namespace ziplog::impl
{

    // One persistent thread per subscriber. Fire-and-forget broadcast
    // (server does not wait for acks from subscribers synchronously).
    class SubscriberBroadcaster
    {
    public:
        explicit SubscriberBroadcaster(ConnectionPool &pool) : pool_(pool) {}

        ~SubscriberBroadcaster() { shutdown(); }

        void start(const std::vector<Address> &subscribers)
        {
            for (size_t i = 0; i < subscribers.size(); i++)
            {
                add_subscriber(i, subscribers[i]);
            }
        }

        void add_subscriber(size_t idx, const Address &addr)
        {
            auto worker = std::make_unique<SubscriberWorker>();
            worker->address = addr;
            {
                std::lock_guard<std::mutex> lock(mu_);
                workers_[idx] = std::move(worker);
            }
            workers_[idx]->thread = std::thread(&SubscriberBroadcaster::worker_loop, this, idx);
        }

        void broadcast(const uint8_t *data, size_t len) // raw bytes
        {
            std::vector<uint8_t> wire(data, data + len); // one copy into queue
            std::lock_guard<std::mutex> lock(mu_);
            for (auto &[idx, worker] : workers_)
            {
                {
                    std::lock_guard<std::mutex> qlock(worker->queue_mu);
                    worker->queue.push(wire); // copy per worker
                }
                worker->cv.notify_one();
            }
        }

        void shutdown()
        {
            cout << "bcast shutdown called\n";
            std::lock_guard<std::mutex> lock(mu_);
            for (auto &[idx, worker] : workers_)
            {
                {
                    std::lock_guard<std::mutex> qlock(worker->queue_mu);
                    worker->shutdown = true;
                }
                worker->cv.notify_one();
            }
            cout << "bcast workers notified\n";
            for (auto &[idx, worker] : workers_)
            {
                if (worker->thread.joinable())
                    worker->thread.join();
            }
            cout << "bcast shutdown complete\n";
        }

    private:
        struct SubscriberWorker
        {
            Address address;
            std::thread thread;
            std::queue<vector<uint8_t>> queue;
            std::mutex queue_mu;
            std::condition_variable cv;
            std::atomic<bool> shutdown{false};
        };

        void worker_loop(size_t idx)
        {
            SubscriberWorker &worker = *workers_[idx];

            NetworkUtils::ReadBuffer rb;
            int current_socket = -1;

            while (!worker.shutdown)
            {
                vector<uint8_t> wire;
                {
                    std::unique_lock<std::mutex> lock(worker.queue_mu);
                    worker.cv.wait(lock, [&]()
                                   { return !worker.queue.empty() || worker.shutdown.load(); });
                    if (worker.shutdown)
                        break;
                    wire = worker.queue.front();
                    worker.queue.pop();
                }

                int sock = pool_.get_connection(worker.address);
                if (sock < 0)
                {
                    worker.shutdown = true;
                    break;
                }

                if (sock != current_socket)
                {
                    cout << "changing\n";
                    current_socket = sock;
                    rb.reset(); // sock change, flush old data
                }

                auto t0 = high_resolution_clock::now();
                auto t1 = high_resolution_clock::now();
                if (!NetworkUtils::send_bytes_raw(sock, wire.data(), wire.size()))
                {
                    worker.shutdown = true;
                    break;
                }

                t1 = high_resolution_clock::now();
                // drain ACK — just need to consume bytes, don't care about content
                size_t ack_len;
                const uint8_t *ack = NetworkUtils::recv_raw_buffered(sock, rb, ack_len);
                if (!ack)
                {
                    worker.shutdown = true;
                    break;
                }
                rb.consume(2 + ack_len);

                auto t2 = high_resolution_clock::now();

                auto dur1 = duration_cast<microseconds>(t1 - t0);
                auto dur2 = duration_cast<microseconds>(t2 - t1);
                // cout << "Server broadcast send latency: " << dur1.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
                // cout << "Server broadcast recv latency: " << dur2.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
            }
            cout << "bcast worker shutdown\n";
        }

        ConnectionPool &pool_;
        std::mutex mu_;
        std::unordered_map<size_t, std::unique_ptr<SubscriberWorker>> workers_;
    };

} // namespace ziplog::impl