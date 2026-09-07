#include "proxy.h"
#include <math.h>

using namespace ziplog::api;

namespace ziplog::impl
{

    /* -----------------------------------------------------------------------------------------------------------------------
        Construction/Deconstruction
    ----------------------------------------------------------------------------------------------------------------------- */

    Proxy::Proxy(const ProxyConfig &cfg)
        : BaseNode<ProxyConfig>(cfg), slot_scheduler_(cfg.max_epoch_history), replicator_(cfg.servers, quorum()), epoch_timer_(
                                                                                                                      EpochDurationUnit(cfg.epoch_duration),
                                                                                                                      [this]()
                                                                                                                      { update_slot_estimate(); },
                                                                                                                      [this](Timestamp &ts, SequenceNumber &seq)
                                                                                                                      { return slot_scheduler_.pop_next_slot(ts, seq); },
                                                                                                                      [this](SequenceNumber seq)
                                                                                                                      { send_out_batch(seq); },
                                                                                                                      [this](Timestamp ts, SequenceNumber seq)
                                                                                                                      { slot_scheduler_.push_front(ts, seq); })
    {
        epoch_duration_ = cfg.epoch_duration;
        replicator_.start();
        start_listening();
        epoch_timer_.start();
    }

    Proxy::Proxy(const ProxyConfig &cfg, bool registered)
        : BaseNode<ProxyConfig>(cfg), slot_scheduler_(cfg.max_epoch_history), replicator_(cfg.servers, quorum()), epoch_timer_(
                                                                                                                      EpochDurationUnit(cfg.epoch_duration),
                                                                                                                      [this]()
                                                                                                                      { update_slot_estimate(); },
                                                                                                                      [this](Timestamp &ts, SequenceNumber &seq)
                                                                                                                      { return slot_scheduler_.pop_next_slot(ts, seq); },
                                                                                                                      [this](SequenceNumber seq)
                                                                                                                      { send_out_batch(seq); },
                                                                                                                      [this](Timestamp ts, SequenceNumber seq)
                                                                                                                      { slot_scheduler_.push_front(ts, seq); })
    {
        epoch_duration_ = cfg.epoch_duration;
        registered_ = registered;
        replicator_.start();
        attempt_join(true);
        start_listening();
        epoch_timer_.start();
    }

    Proxy::~Proxy()
    {
        ZLOG("Proxy " << id() << " shutdown() called");
        cout << "Proxy " << id() << " shutdown() called" << endl;
        epoch_timer_.stop();
        BaseNode::shutdown();
        replicator_.shutdown();
        zipper_pool_.close_all();
        ZLOG("Proxy " << id() << " shutdown() complete");
        cout << "Proxy " << id() << " shutdown() complete" << endl;
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Main Loop
    ----------------------------------------------------------------------------------------------------------------------- */
    void Proxy::handle_connection(int client_socket)
    {
        NetworkUtils::ReadBuffer rb; // lives on stack for this connection

        while (running())
        {
            size_t msg_len;
            const uint8_t *buf = NetworkUtils::recv_raw_buffered(client_socket, rb, msg_len);
            if (!buf)
                break;

            // peek header — no full deserialize
            auto header = MessageHeader::peek(buf, msg_len);
            if (!header)
            {
                rb.consume(2 + msg_len); // TODOD: return here!!
                break;
            }

            switch (header->type)
            {
            case APPEND:
            {
                auto start = high_resolution_clock::now();
                if (!registered_)
                {
                    Message resp;
                    resp.type = FAILURE;
                    NetworkUtils::send_message(client_socket, resp);
                    break;
                }

                // data starts at offset 24 (type(4) + shard(4) + sender(4) + seq(8) + data_len(4) = 24)
                if (msg_len < 24)
                    break;

                ZLOG("[proxy " << id() << "] recv client req on socket " << client_socket);

                uint32_t data_len;
                memcpy(&data_len, buf + 20, 4);
                data_len = ntohl(data_len);

                client_buffers_.push_raw(client_socket, buf + 24, data_len);
                auto client_buffers_end = high_resolution_clock::now();

                slot_scheduler_.record_request();
                auto slot_sched_end = high_resolution_clock::now();

                ZLOG("[proxy " << id() << "] buffer size: " << client_buffers_.buffer_size(client_socket));

                auto dur1 = duration_cast<EpochDurationUnit>(client_buffers_end - start);
                auto dur2 = duration_cast<EpochDurationUnit>(slot_sched_end - start);

                // cout << "Proxy client buffer - push: " << dur1.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
                // cout << "Proxy slot scheduler - record request: " << dur2.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
                break;
            }
            case ZIP_RESPONSE:
            {
                // handle_zip_response(buf, msg_len);   // TODO: needs vector version
                break;
            }
            case INCLUDE_PROXY:
            {
                if (registered_)
                    break;
                registered_ = true;
                epoch_timer_.start();
                ZLOG("[proxy " << id() << "] joined the system");
                break;
            }
            case FREEZE:
            {
                registered_ = false;
                epoch_timer_.pause();
                attempt_join(false);
                break;
            }
            }

            rb.consume(2 + msg_len); // end
        }

        close(client_socket);
        client_buffers_.remove(client_socket);
        ZLOG("[proxy " << id() << "] closed socket " << client_socket);
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Epoch Callbacks
    ----------------------------------------------------------------------------------------------------------------------- */
    void Proxy::update_slot_estimate()
    {
        SequenceNumber estimate = 1;

        // self-assign slots, bypass zipper entirely
        std::vector<SequenceNumber> ordering_values;
        Timestamp ts = now();
        Timestamp interval = static_cast<Timestamp>(epoch_duration_) / estimate;

        for (SequenceNumber i = 0; i < estimate; i++)
        {
            Timestamp send_ts = ts + (interval * (i + 1));     // evenly spaced
            ordering_values.push_back(send_ts);                // timeout
            ordering_values.push_back(next_seq_.fetch_add(1)); // seq
        }
        slot_scheduler_.load_slots(ordering_values);
    }

    void Proxy::send_out_batch(SequenceNumber seq)
    {
        auto start = high_resolution_clock::now();
        ZLOG("[proxy " << id() << "] send_out_batch() called");

        auto drain = client_buffers_.drain();

        std::vector<uint8_t> wire;

        if (drain.participating.empty())
        {
            // SKIP — no data
            wire = NetworkUtils::build_wire_bytes(SKIP, shard(), id(), seq, nullptr, 0);
            client_buffers_.release(0);
        }
        else
        {
            // APPEND — data already serialized in drain.data
            wire = NetworkUtils::build_wire_bytes(APPEND, shard(), id(), seq, drain.data, drain.len);
            client_buffers_.release(drain.len);
        }

        bool success = replicator_.replicate_bytes(std::move(wire), seq);

        if (!drain.participating.empty())
        {
            // build ACK/FAILURE wire bytes once, send to all clients
            std::vector<uint8_t> resp_wire = NetworkUtils::build_wire_bytes(success ? SUCCESS : FAILURE, shard(), id(), 0, nullptr, 0);

            for (int client : drain.participating)
                NetworkUtils::send_bytes_raw(client, resp_wire.data(), resp_wire.size());
        }

        ZLOG("[proxy " << id() << "] replication "
                       << (success ? "succeeded" : "failed")
                       << " for seq " << seq);

        auto end = high_resolution_clock::now();
        auto dur = duration_cast<EpochDurationUnit>(end - start);
        // cout << "Proxy send out batch(): " << dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Reconfiguration (not currently in use)
    ----------------------------------------------------------------------------------------------------------------------- */
    void Proxy::handle_zip_response(const Message &msg)
    {
        if (msg.shard_id != shard())
            return;

        ZLOG("[proxy " << id() << "] got " << msg.get_num_requests() << " slots from zipper");
        cout << "proxy got slots from zipper at " << now() << "\n";
        auto start = high_resolution_clock::now();

        // cout << "[proxy " << id() << "] got slots from zipper at " << std::to_string(now()) << endl;
        slot_scheduler_.load_slots(msg.ordering_values);

        ZLOG("[proxy " << id() << "] load slots returned");
        auto end = high_resolution_clock::now();

        auto dur = duration_cast<EpochDurationUnit>(end - start);
        cout << "Proxy load zipper slots: " << dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
    }

    void Proxy::attempt_join(bool is_new)
    {
        Message req;
        req.type = is_new ? REGISTER_PROXY : REJOIN_PROXY;
        req.shard_id = shard();
        if (!is_new)
            req.sender_id = id();

        string addr = address().ip + ":" + std::to_string(address().port);
        req.data = Command(addr.begin(), addr.end());

        int sock = zipper_pool_.get_connection(config_.zipper);
        if (sock < 0)
            return;
        NetworkUtils::send_message(sock, req);
    }
} // namespace ziplog::impl
