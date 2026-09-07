#include "subscriber.h"

using namespace ziplog::api;

namespace ziplog::impl
{

    Subscriber::Subscriber(const SubscriberConfig &cfg)
        : BaseNode<SubscriberConfig>(cfg)
    {
        start_listening();
    }

    Subscriber::Subscriber(const SubscriberConfig &cfg, bool registered)
        : BaseNode<SubscriberConfig>(cfg)
    {
        start_listening();

        Message req;
        req.type = REGISTER_SUBSCRIBER;
        req.shard_id = shard();
        req.sender_id = id();
        string addr = address().ip + ":" + std::to_string(address().port);
        req.data = Command(addr.begin(), addr.end());

        int sock = connection_pool_.get_connection(config_.zipper);
        if (sock >= 0)
            NetworkUtils::send_message(sock, req);
    }

    Subscriber::~Subscriber()
    {
        ZLOG("Subscriber " << id() << " shutdown() called");
        cout << "Subscriber " << id() << " shutdown() called" << endl;
        BaseNode::shutdown();
        connection_pool_.close_all();
        log_.print_expanded();
        log_.print_pending();
        log_.print_out_of_order();
        ZLOG("Subscriber " << id() << " shutdown() complete");
        cout << "Subscriber " << id() << " shutdown() complete" << endl;
    }

    void Subscriber::shutdown()
    {
        BaseNode::shutdown();
    }

    /* -----------------------------------------------------------------------------------------------------------------------
        Main Loop
    ----------------------------------------------------------------------------------------------------------------------- */
    void Subscriber::handle_connection(int server_sock)
    {
        NetworkUtils::ReadBuffer rb;

        while (running())
        {
            size_t msg_len;
            const uint8_t *buf = NetworkUtils::recv_raw_buffered(server_sock, rb, msg_len);
            if (!buf)
                break;

            // cout << "subscriber recv message\n";
            auto header = MessageHeader::peek(buf, msg_len);
            if (!header)
            {
                cout << "peek failed, msg_len=" << msg_len << "\n";
                rb.consume(2 + msg_len);
                break;
            }

            // cout << "subscriber processing message\n";

            auto t0 = high_resolution_clock::now();

            std::vector<std::pair<SequenceNumber, int64_t>> latencies;

            if (header->type == APPEND || header->type == SKIP)
            {
                size_t data_len = 0;
                const uint8_t *data = (header->type == APPEND)
                                          ? NetworkUtils::get_data_ptr(buf, msg_len, data_len)
                                          : nullptr;

                latencies = log_.observe(header->sender_id, header->seq_or_count, data, data_len, quorum());
            }

            auto t1 = high_resolution_clock::now();

            // send ACK — no Message construction
            auto ack_wire = NetworkUtils::build_wire_bytes(ACK, shard(), id(), header->seq_or_count, nullptr, 0);
            if (!NetworkUtils::send_bytes_raw(server_sock, ack_wire.data(), ack_wire.size()))
                break;

            rb.consume(2 + msg_len);

            auto end = high_resolution_clock::now();
            auto observe_dur = duration_cast<EpochDurationUnit>(t1 - t0);
            auto ack_dur = duration_cast<EpochDurationUnit>(end - t1);

            // print after ack — completely off critical path
            for (auto &[seq, lat] : latencies)
            {
                cout << "[latency] seq=" << seq << " latency=" << lat
                     << " " << EPOCH_DURATION_UNIT_STR << "\n";
            }
        }
        close(server_sock);
    }

} // namespace ziplog::impl