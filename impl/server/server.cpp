#include "server/server.h"

using namespace ziplog::api;

namespace ziplog::impl
{

    Server::Server(const ServerConfig &cfg)
        : BaseNode<ServerConfig>(cfg), liveness_(EpochDurationUnit(cfg.epoch_duration),
                                                 EpochDurationUnit(cfg.epoch_duration * 5), // lag = 5 epochs rn bc microseconds
                                                 cfg.proxies.size(),
                                                 cfg.zipper,
                                                 connection_pool_),
          broadcaster_(connection_pool_), freeze_handler_(id(), cfg.zipper, cfg.other_servers,
                                                          connection_pool_, store_, liveness_, broadcaster_,
                                                          [this]()
                                                          { return shard(); })
    {
        broadcaster_.start(cfg.subscribers);
        start_listening();
        // liveness_.start(id());
    }

    Server::~Server()
    {
        ZLOG("Server " << id() << " shutdown() called");
        cout << "Server " << id() << " shutdown() called" << endl;
        broadcaster_.shutdown();
        cout << "Server " << id() << " bcaster terminated" << endl;
        // liveness_.stop();
        BaseNode::shutdown();
        cout << "Server " << id() << " basenode terminated" << endl;
        connection_pool_.close_all();
        ZLOG("Server " << id() << " shutdown() complete");
        cout << "Server " << id() << " shutdown() complete" << endl;
    }

    void Server::shutdown()
    {
        BaseNode::shutdown();
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Main Loop
    ----------------------------------------------------------------------------------------------------------------------- */
    void Server::handle_connection(int proxy_socket)
    {
        NetworkUtils::ReadBuffer rb;

        while (running())
        {
            size_t msg_len;
            const uint8_t *buf = NetworkUtils::recv_raw_buffered(proxy_socket, rb, msg_len);
            if (!buf)
                break;

            auto header = MessageHeader::peek(buf, msg_len);
            if (!header)
            {
                rb.consume(2 + msg_len);
                break;
            }

            switch (header->type)
            {
            case APPEND:
            {
                // cout << "[server " << id() << "] got batch at " << std::to_string(now()) << "\n";
                [[fallthrough]];
            }
            case SKIP:
            {
                // ACK immediately — zero alloc, zero copy
                auto ack_wire = NetworkUtils::build_wire_bytes(
                    ACK, shard(), id(), header->seq_or_count, nullptr, 0);
                NetworkUtils::send_bytes_raw(proxy_socket, ack_wire.data(), ack_wire.size());

                if (!liveness_.is_blocked(header->sender_id))
                {
                    // store raw bytes — one copy
                    store_.store(header->sender_id, buf - 2, msg_len + 2);
                    liveness_.remove_timeout(header->sender_id, header->seq_or_count);

                    // update sender id to be server's id
                    uint8_t *wire_start = const_cast<uint8_t *>(buf - 2);
                    uint32_t net_sender = htonl(static_cast<uint32_t>(id()));
                    memcpy(wire_start + 10, &net_sender, 4);

                    // broadcast raw bytes — one copy per subscriber worker
                    broadcaster_.broadcast(buf - 2, msg_len + 2);

                    // restore proxy id
                    uint32_t net_orig = htonl(static_cast<uint32_t>(header->sender_id));
                    memcpy(wire_start + 10, &net_orig, 4);
                }
                break;
            }
            case ZIP_RESPONSE:
            {
                auto opt_msg = Message::deserialize(buf, msg_len);
                if (opt_msg)
                {
                    liveness_.update_timeouts(opt_msg->sender_id, opt_msg->ordering_values);
                    auto ack_wire = NetworkUtils::build_wire_bytes(
                        ACK, shard(), id(), 0, nullptr, 0);
                    NetworkUtils::send_bytes_raw(proxy_socket, ack_wire.data(), ack_wire.size());
                }
                break;
            }
            case FREEZE:
            {
                // freeze_handler_.handle_freeze(msg, true);
                break;
            }
            case TRANSFER_REQUEST:
            {
                // freeze_handler_.handle_transfer_request(proxy_socket, *opt_msg);
                break;
            }
            case FREEZE_COMPLETE:
            {
                // liveness_.block_proxy(opt_msg->get_failed_proxy());
                break;
            }
            case INCLUDE_PROXY:
            {
                // introduce_proxy(*opt_msg);
                break;
            }
            case INCLUDE_SUBSCRIBER:
            {
                // introduce_subscriber(proxy_socket, *opt_msg);
                break;
            }
            }

            rb.consume(2 + msg_len);
        }
        close(proxy_socket);
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Reconfiguration
    ----------------------------------------------------------------------------------------------------------------------- */
    void Server::introduce_proxy(const Message &msg)
    {
        string addr_info(msg.data.begin(), msg.data.end());
        size_t colon = addr_info.find(':');
        string ip = addr_info.substr(0, colon);
        int port = stoi(addr_info.substr(colon + 1));

        NodeId proxy_id = msg.sender_id;
        if (!config_.isValidProxy(proxy_id))
        {
            config_.proxies.push_back({ip, port});
        }
        else
        {
            liveness_.unblock_proxy(proxy_id);
        }

        Message ack;
        ack.type = ACK;
        ack.shard_id = shard();
        ack.sender_id = id();
        NetworkUtils::send_message(/* caller holds socket */ 0, ack); // ack sent by caller
    }

    void Server::introduce_subscriber(int socket, const Message &msg)
    {
        string addr_info(msg.data.begin(), msg.data.end());
        size_t colon = addr_info.find(':');
        string ip = addr_info.substr(0, colon);
        int port = stoi(addr_info.substr(colon + 1));
        Address subscriber{ip, port};

        config_.subscribers.push_back(subscriber);
        auto snap = store_.snapshot();

        Message ack;
        ack.type = ACK;
        ack.shard_id = shard();
        ack.sender_id = id();
        NetworkUtils::send_message(socket, ack);

        // replay stored messages to new subscriber
        int sock = connection_pool_.get_connection(subscriber);
        if (sock < 0)
            return;

        for (const auto &[proxy_id, messages] : snap)
        {
            if (liveness_.is_blocked(proxy_id))
                continue;
            for (const auto &wire : messages)
            {
                NetworkUtils::send_bytes_raw(sock, wire.data(), wire.size());
                Message resp;
                // NetworkUtils::recv_message_buffered(sock, rb, resp);
            }
        }

        broadcaster_.add_subscriber(config_.subscribers.size() - 1, subscriber);
    }

} // namespace ziplog::impl