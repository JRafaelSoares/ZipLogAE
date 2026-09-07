#pragma once
#include "../api/types.h"
#include "network_utils.h"
#include "connection_pool.h"
#include "message_store.h"
#include "proxy_liveness_tracker.h"
#include "subscriber_broadcaster.h"
#include <unordered_map>
#include <mutex>
#include <functional>
#include <iostream>

using namespace ziplog::api;

namespace ziplog::impl
{

    class FreezeHandler
    {
    public:
        FreezeHandler(NodeId server_id,
                      const Address &zipper,
                      const std::vector<Address> &other_servers,
                      ConnectionPool &pool,
                      MessageStore &store,
                      ProxyLivenessTracker &liveness,
                      SubscriberBroadcaster &broadcaster,
                      std::function<ShardId()> get_shard)
            : server_id_(server_id), zipper_(zipper), other_servers_(other_servers), pool_(pool), store_(store), liveness_(liveness), broadcaster_(broadcaster), get_shard_(std::move(get_shard)) {}

        void handle_freeze(const Message &msg, bool from_zipper)
        {
            ZLOG("[freeze] server " << server_id_ << " got freeze");
            NodeId failed_proxy = msg.get_failed_proxy();

            {
                std::lock_guard<std::mutex> lock(mu_);
                if (from_zipper && rounds_.count(failed_proxy) && rounds_[failed_proxy] >= msg.get_round())
                {
                    ZLOG("[freeze] outdated, ignoring");
                    return;
                }
                rounds_[failed_proxy] = msg.get_round();
            }

            liveness_.set_reconfiguring(failed_proxy);

            // build transfer request to collect missing messages from peers
            Message transfer;
            transfer.type = TRANSFER_REQUEST;
            transfer.shard_id = get_shard_();
            transfer.sender_id = server_id_;
            transfer.set_failed_proxy(failed_proxy);
            transfer.set_round(msg.get_round());
            transfer.set_sequence_number(liveness_.last_seq(failed_proxy));

            for (const Address &server : other_servers_)
            {
                int sock = pool_.get_connection(server);
                if (sock < 0)
                    continue;
                if (!NetworkUtils::send_message(sock, transfer))
                    continue;

                while (true)
                {
                    Message resp;
                    if (!NetworkUtils::recv_message(sock, resp))
                        break;
                    if (resp.type == ACK)
                        break;

                    ZLOG("[freeze] got stored msg seq " << resp.get_sequence_number());
                    auto wire = resp.serialize();
                    store_.store(failed_proxy, wire.data(), wire.size());
                    broadcaster_.broadcast(wire.data(), wire.size());
                }
            }

            // respond to zipper with last known sequence
            Message freeze_resp;
            freeze_resp.type = FREEZE_RESPONSE;
            freeze_resp.shard_id = get_shard_();
            freeze_resp.sender_id = server_id_;
            freeze_resp.set_failed_proxy(failed_proxy);
            freeze_resp.set_round(msg.get_round());
            freeze_resp.set_sequence_number(liveness_.last_seq(failed_proxy));

            ZLOG("[freeze] last seq = " << liveness_.last_seq(failed_proxy));

            int zip_sock = pool_.get_connection(zipper_);
            if (zip_sock >= 0)
                NetworkUtils::send_message(zip_sock, freeze_resp);
        }

        void handle_transfer_request(int socket, const Message &msg)
        {
            if (msg.shard_id != get_shard_())
                return;

            NodeId failed_proxy = msg.get_failed_proxy();

            {
                std::lock_guard<std::mutex> lock(mu_);
                if (rounds_.count(failed_proxy) && rounds_[failed_proxy] > msg.get_round())
                {
                    // outdated - just ack
                    Message ack;
                    ack.type = ACK;
                    ack.shard_id = get_shard_();
                    ack.sender_id = server_id_;
                    NetworkUtils::send_message(socket, ack);
                    return;
                }
            }

            bool new_round = false;
            {
                std::lock_guard<std::mutex> lock(mu_);
                if (!rounds_.count(failed_proxy) || rounds_[failed_proxy] < msg.get_round())
                {
                    rounds_[failed_proxy] = msg.get_round();
                    new_round = true;
                }
            }

            if (!store_.has(failed_proxy))
            {
                Message ack;
                ack.type = ACK;
                ack.shard_id = get_shard_();
                ack.sender_id = server_id_;
                ack.set_sequence_number(msg.get_sequence_number());
                NetworkUtils::send_message(socket, ack);
                if (new_round)
                    handle_freeze(msg, false);
                return;
            }

            SequenceNumber req_last_seq = msg.get_sequence_number();
            auto snap = store_.snapshot();
            auto it = snap.find(failed_proxy);
            if (it != snap.end())
            {
                for (const auto &wire : it->second)
                {
                    // send wire bytes directly
                    NetworkUtils::send_bytes_raw(socket, wire.data(), wire.size());
                }
            }

            Message ack;
            ack.type = ACK;
            ack.shard_id = get_shard_();
            ack.sender_id = server_id_;
            NetworkUtils::send_message(socket, ack);

            if (new_round)
                handle_freeze(msg, false);
        }

    private:
        NodeId server_id_;
        Address zipper_;
        std::vector<Address> other_servers_;
        ConnectionPool &pool_;
        MessageStore &store_;
        ProxyLivenessTracker &liveness_;
        SubscriberBroadcaster &broadcaster_;
        std::function<ShardId()> get_shard_;

        std::mutex mu_;
        std::unordered_map<NodeId, int> rounds_;
    };

} // namespace ziplog::impl