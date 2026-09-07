#include "client.h"
#include <arpa/inet.h>

using namespace ziplog::api;

namespace ziplog::impl
{

    Client::Client(Address proxy)
    {
        proxy_ = proxy;
    }

    Client::~Client()
    {
        connection_pool_.close_all();
    }

    static const Command BENCHMARK_PADDING = []()
    {
        constexpr size_t PAD_SIZE = 4096 - 28 - 21; // 21 = max uint64 digits + '|' and 28 = metadata size
        Command pad(PAD_SIZE);
        std::generate(pad.begin(), pad.end(), []()
                      { return static_cast<uint8_t>(rand() % 256); });
        return pad;
    }();

    inline Command make_benchmark_payload(Timestamp send_time)
    {
        string ts_prefix = std::to_string(send_time) + "|";
        Command payload(ts_prefix.begin(), ts_prefix.end());
        payload.insert(payload.end(), BENCHMARK_PADDING.begin(), BENCHMARK_PADDING.end());
        return payload;
    }

    bool Client::append(const Command &payload)
    {
        int sock = connection_pool_.get_connection(proxy_);
        if (sock < 0)
        {
            cout << "client::append() failed to connect to proxy" << endl;
            return false;
        }

        Timestamp send_time = now();
        Command data = make_benchmark_payload(send_time);

        // build wire bytes directly — no Message struct
        auto wire = NetworkUtils::build_wire_bytes(
            APPEND, 0, 0, 0,
            data.data(), data.size());

        if (!NetworkUtils::send_bytes_raw(sock, wire.data(), wire.size()))
        {
            connection_pool_.close_connection(proxy_);
            return false;
        }

        // recv ACK — raw buffered
        size_t msg_len;
        const uint8_t *buf = NetworkUtils::recv_raw_buffered(sock, rb_, msg_len);
        if (!buf)
        {
            connection_pool_.close_connection(proxy_);
            return false;
        }

        auto header = MessageHeader::peek(buf, msg_len);
        rb_.consume(2 + msg_len);

        return header && header->type == SUCCESS;
    }

    /*
    bool Client::append(const Command &payload)
    {
        int sock = connection_pool_.get_connection(proxy_);
        if (sock >= 0)
        {
            auto start = high_resolution_clock::now();

            Timestamp send_time = now();

            Message req;
            req.type = APPEND;
            req.data = make_benchmark_payload(send_time);

            auto send_start = high_resolution_clock::now();
            if (!NetworkUtils::send_message(sock, req))
            {
                connection_pool_.close_connection(proxy_);
                cout << "client::append() failed to send message request to proxy" << endl;
                return false;
            }
            auto send_end = high_resolution_clock::now();

            Message resp;
            if (!NetworkUtils::recv_message(sock, resp))
            {
                connection_pool_.close_connection(proxy_);
                cout << "client::append() failed to recv proxy response" << endl;
                return false;
            }
            auto end = high_resolution_clock::now();

            auto send_dur = duration_cast<EpochDurationUnit>(send_end - send_start);
            auto recv_dur = duration_cast<EpochDurationUnit>(end - send_end);
            auto dur = duration_cast<EpochDurationUnit>(end - start);

            // cout << "[client] sending request at " << std::to_string(send_time) << "\n";
            // cout << "Client send latency: " << send_dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
            // cout << "Client recv latency: " << recv_dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";
            // cout << "Client total latency: " << dur.count() << " " << EPOCH_DURATION_UNIT_STR << "\n";

            return resp.type == SUCCESS;
        }
        cout << "client::append() failed to connect to proxy" << endl;
        return false;
    }
    */

    bool Client::bulk_append(const vector<Command> &commands)
    {
        size_t success = 0;
        for (const auto &command : commands)
        {
            if (append(command))
                success++;
        }
        cout << "Sent " << success << "/" << commands.size() << " commands" << endl;
        return success == commands.size();
    }

    void Client::update_proxy(Address new_proxy)
    {
        connection_pool_.close_all();
        proxy_ = new_proxy;
    }
}