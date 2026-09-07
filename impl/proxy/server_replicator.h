#pragma once
#include "../api/address.h"
#include "../api/types.h"
#include "../api/network_utils.h"
#include <unordered_map>
#include <queue>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <memory>
#include <atomic>

using namespace ziplog::api;

namespace ziplog::impl
{

    class ServerReplicator
    {
    public:
        ServerReplicator(const std::vector<Address> &servers, size_t quorum)
            : servers_(servers), quorum_(quorum) {}

        ~ServerReplicator() { shutdown(); }

        void start()
        {
            for (size_t i = 0; i < servers_.size(); i++)
            {
                auto worker = std::make_unique<ServerWorker>();
                server_workers_[i] = std::move(worker);
                live_workers_++;
                server_workers_[i]->thread = std::thread(&ServerReplicator::worker_loop, this, i);
            }
        }

        void shutdown()
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (auto &[idx, worker] : server_workers_)
            {
                {
                    std::lock_guard<std::mutex> qlock(worker->queue_mu);
                    worker->shutdown = true;
                }
                worker->cv.notify_one();
            }
            for (auto &[idx, worker] : server_workers_)
            {
                if (worker->thread.joinable())
                    worker->thread.join();
            }
        }

        // blocks until f+1 acks received
        bool replicate_bytes(std::vector<uint8_t> wire_bytes, SequenceNumber seq)
        {
            auto t0 = high_resolution_clock::now();
            auto state = std::make_shared<ReplicationState>();
            state->seq = seq;

            {
                std::lock_guard<std::mutex> lock(mu_);
                for (auto &[idx, worker] : server_workers_)
                {
                    std::lock_guard<std::mutex> qlock(worker->queue_mu);
                    worker->pending_queue.push({wire_bytes, state}); // one copy per server
                    worker->cv.notify_one();
                }
            }

            while (state->ack_count.load(std::memory_order_acquire) < static_cast<int>(quorum_))
            {
                if (live_workers_.load(std::memory_order_relaxed) == 0)
                    return false;
            }
            auto t1 = high_resolution_clock::now();
            auto total = duration_cast<microseconds>(t1 - t0).count();
            // cout << "replicate took" << total << "µs\n";

            return true;
        }

    private:
        struct ReplicationState
        {
            std::atomic<int> ack_count{0};
            SequenceNumber seq;
        };

        struct WorkItem
        {
            std::vector<uint8_t> wire_bytes; // header + data + footer, assembled by caller (just need to send)
            std::shared_ptr<ReplicationState> state;
        };

        struct ServerWorker
        {
            std::thread thread;
            int socket{-1};
            std::queue<WorkItem> pending_queue;
            std::mutex queue_mu;
            std::condition_variable cv;
            std::atomic<bool> shutdown{false};
        };

        void worker_loop(int server_idx)
        {
            ServerWorker &worker = *server_workers_[server_idx];
            const Address &server = servers_[server_idx];

            // keep retrying until connected or shutdown
            while (worker.socket < 0 && !worker.shutdown)
            {
                worker.socket = NetworkUtils::connect_to_address_persistent(server.ip, server.port);

                if (worker.socket < 0)
                {
                    // std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
            }

            NetworkUtils::ReadBuffer rb;
            while (!worker.shutdown)
            {
                WorkItem item;
                auto t0 = high_resolution_clock::now();

                {
                    std::unique_lock<std::mutex> lock(worker.queue_mu);
                    worker.cv.wait(lock, [&]()
                                   { return !worker.pending_queue.empty() || worker.shutdown.load(); });
                    if (worker.shutdown)
                        break;

                    item = worker.pending_queue.front();
                    worker.pending_queue.pop();
                }

                auto t1 = high_resolution_clock::now(); // deque

                bool ok = NetworkUtils::send_bytes_raw(worker.socket, item.wire_bytes.data(), item.wire_bytes.size());
                if (ok)
                {
                    Message ack;
                    ok = NetworkUtils::recv_message_buffered(worker.socket, rb, ack);
                }

                if (!ok)
                {
                    worker.shutdown = true;
                    break;
                }

                // signal quorum waiter
                if (item.state)
                {
                    item.state->ack_count.fetch_add(1, std::memory_order_release); // https://en.cppreference.com/cpp/atomic/atomic/fetch_add
                }
            }
            live_workers_.fetch_sub(1, std::memory_order_release);
            close(worker.socket);
            cout << "worker thread exitted" << endl;
        }

        std::vector<Address> servers_;
        size_t quorum_;
        std::mutex mu_;
        std::unordered_map<int, std::unique_ptr<ServerWorker>> server_workers_;
        std::atomic<int> live_workers_{0};
    };

} // namespace ziplog::impl