#pragma once
#include "../api/types.h"
#include "../include/circular_buffer.h"
#include <unordered_map>
#include <mutex>
#include <set>

using namespace ziplog::api;

namespace ziplog::impl
{

    class ClientBufferManager
    {
    public:
        static constexpr size_t MAX_BATCH_BYTES = 60000;

        void push(int client_socket, const Command &cmd)
        {
            std::lock_guard<std::mutex> lock(mu_);
            client_buffers_[client_socket].push(PendingRequest(cmd, client_socket));
        }

        void push_raw(int client_socket, const uint8_t *data, size_t len)
        {
            std::lock_guard<std::mutex> lock(mu_);
            Command cmd(data, data + len); // one copy, directly into PendingRequest
            client_buffers_[client_socket].push(PendingRequest(std::move(cmd), client_socket));
        }

        void remove(int client_socket)
        {
            std::lock_guard<std::mutex> lock(mu_);
            client_buffers_.erase(client_socket);
        }

        // round-robin drain across all client buffers up to MAX_BATCH_BYTES
        // returns serialized batch and the set of participating client sockets
        std::pair<CommandBatch, std::set<int>> drain_batch()
        {
            std::lock_guard<std::mutex> lock(mu_);

            CommandBatch batch;
            std::set<int> participating;
            size_t total = 0;
            bool pulled = true;

            while (pulled && total < MAX_BATCH_BYTES)
            {
                pulled = false;
                for (auto &[socket, buffer] : client_buffers_)
                {
                    if (total >= MAX_BATCH_BYTES)
                        break;
                    PendingRequest pr;
                    if (buffer.peek(pr) && total + pr.cmd.size() <= MAX_BATCH_BYTES)
                    {
                        buffer.pop(pr);
                        batch.add_command(pr.cmd);
                        participating.insert(pr.client_socket);
                        total += pr.cmd.size();
                        pulled = true;
                    }
                }
            }

            return {batch, participating};
        }

        size_t buffer_size(int client_socket) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = client_buffers_.find(client_socket);
            return it != client_buffers_.end() ? it->second.size() : 0;
        }

    private:
        mutable std::mutex mu_;
        std::unordered_map<int, CircularBuffer<PendingRequest>> client_buffers_;
    };

} // namespace ziplog::impl