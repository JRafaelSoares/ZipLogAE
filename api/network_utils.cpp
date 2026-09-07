#include "network_utils.h"
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <sys/uio.h>

namespace ziplog::api
{

    /*
    -------------------------------------------------------------------------------------------
    Request and Response Pattern
    -------------------------------------------------------------------------------------------
    */

    // sends a message on a connect and wait for a response
    bool NetworkUtils::send_message_and_wait_for_ack(int connector_socket, const Message &msg)
    {
        if (!send_message(connector_socket, msg))
        {
            return false;
        }

        Message resp;
        if (!recv_message(connector_socket, resp))
        {
            return false;
        }

        return resp.type == ACK && msg.get_sequence_number() == resp.get_sequence_number();
    }

    // creates connector socket and attempt to send a message ('max_retries'x) and stores response
    bool NetworkUtils::send_message_to_address(const string &ip, int port, const Message &msg, Message &resp, int max_retries)
    {
        for (int attempt = 0; attempt < max_retries; attempt++)
        {
            // create connector socket (outgoing connection)
            int sockfd = create_connector_socket();
            if (sockfd < 0)
                continue;

            // attempt to connect to address of recipient
            if (connect_to_address(sockfd, ip, port))
            {
                if (!send_message(sockfd, msg))
                {
                    close(sockfd);
                    return false;
                }
                if (!recv_message(sockfd, resp))
                {
                    close(sockfd);
                    return false;
                }
                close(sockfd);
                return true;
            }
            else
            {
                close(sockfd);
            }
        }
        return false;
    }

    /*
    -------------------------------------------------------------------------------------------
    Sending/Reading Bytes over a Connection
    -------------------------------------------------------------------------------------------
    */

    // serializes messages, send size header and bytes of struct over a socket
    bool NetworkUtils::send_message(int socket, const Message &msg)
    {
        auto serialized = msg.serialize();
        if (serialized.empty())
        {
            ZLOG("Rejecting send of oversized message");
            return false;
        }

        uint16_t msg_len = htons(static_cast<uint16_t>(serialized.size()));

        struct iovec iov[2];
        iov[0].iov_base = &msg_len;
        iov[0].iov_len = 2;
        iov[1].iov_base = const_cast<uint8_t *>(serialized.data());
        iov[1].iov_len = serialized.size();

        struct msghdr hdr{};
        hdr.msg_iov = iov;
        hdr.msg_iovlen = 2;

        ssize_t expected = 2 + static_cast<ssize_t>(serialized.size());
        return sendmsg(socket, &hdr, MSG_NOSIGNAL) == expected;
    }

    bool NetworkUtils::send_message_og(int socket, const Message &msg)
    {
        // cout << "send_message() called" << endl;
        //  attempt to serialize message
        auto serialized = msg.serialize();
        if (serialized.empty())
        {
            ZLOG("Rejecting send of oversized message");
            return false;
        }

        Timestamp start = now();

        // send size of serialized message
        uint16_t msg_len = htons(static_cast<uint16_t>(serialized.size()));
        if (!send_bytes(socket, &msg_len, 2))
        {
            ZLOG("couldnt send size on connection");
            return false;
        }
        // cout << "send_message() sending " << msg_len << " bytes on socket " << socket << endl;
        // cout << "send_bytes() exitting" << endl;
        //  send message
        bool result = send_bytes(socket, serialized.data(), serialized.size());
        Timestamp end = now();

        if (end - start > 10)
        {
            // cout << "[slow send] duration ---------------- " << end - start << endl;
        }
        return result;
    }

    // sending specified number of bytes from a pointer to a socket
    bool NetworkUtils::send_bytes(int socket, const void *data, size_t len)
    {
        // cout << "send_bytes() called" << endl;
        const uint8_t *ptr = static_cast<const uint8_t *>(data);
        size_t sent = 0;

        while (sent < len)
        {
            ssize_t result = send(socket, ptr + sent, len - sent, MSG_NOSIGNAL);
            if (result <= 0)
            {
                ZLOG("sending failed on socket. return value = " << result);
                return false;
            }
            sent += result;
        }
        // cout << "send_bytes() returning true" << endl;
        return true;
    }

    // reads size header and struct from a connection and deserializes into a message struct
    bool NetworkUtils::recv_message(int socket, Message &msg)
    {
        Timestamp start = now();
        // read 2-byte length prefix
        uint16_t msg_len;
        if (!recv_bytes(socket, &msg_len, 2))
        {
            ZLOG("recv_message() no header sent" << msg_len);
            return false;
        }
        // cout << "recv_message() reading in " << msg_len << " bytes on socket " << socket << endl;

        msg_len = ntohs(msg_len);
        if (msg_len > MAX_MESSAGE_SIZE)
        {
            ZLOG("recv_message() rejecting recv of oversized message");
            return false;
        }

        // read message data
        vector<uint8_t> buffer(msg_len);
        if (!recv_bytes(socket, buffer.data(), msg_len))
        {
            ZLOG("recv_message() revc_bytes() failed");
            return false;
        }

        // attempt to deserialize message
        auto opt_msg = Message::deserialize(buffer);
        if (!opt_msg)
        {
            ZLOG("Failed to deserialize message 1");
            return false;
        }

        Timestamp end = now();
        if (end - start > 10)
        {
            // cout << "[slow recv] duration ---------------- " << end - start << endl;
        }
        msg = *opt_msg;
        return true;
    }

    // reads specified number of bytes from a socket and writes to a pointer
    bool NetworkUtils::recv_bytes(int socket, void *data, size_t len)
    {
        uint8_t *ptr = static_cast<uint8_t *>(data);
        size_t received = 0;

        while (received < len)
        {
            ssize_t result = recv(socket, ptr + received, len - received, 0);
            if (result <= 0)
            {
                return false;
            }
            received += result;
        }
        return true;
    }

    // non-blocking recv_bytes — spins until data available
    bool NetworkUtils::recv_bytes_spin(int socket, void *data, size_t len)
    {
        uint8_t *ptr = static_cast<uint8_t *>(data);
        size_t received = 0;

        while (received < len)
        {
            ssize_t result = recv(socket, ptr + received, len - received, MSG_DONTWAIT);
            if (result > 0)
            {
                received += result;
            }
            else if (result < 0 && errno == EAGAIN)
            {
                continue; // spin
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    bool NetworkUtils::recv_message_spin(int socket, Message &msg)
    {
        uint16_t msg_len;
        if (!recv_bytes_spin(socket, &msg_len, 2))
            return false;

        msg_len = ntohs(msg_len);
        if (msg_len > MAX_MESSAGE_SIZE)
            return false;

        vector<uint8_t> buffer(msg_len);
        if (!recv_bytes_spin(socket, buffer.data(), msg_len))
            return false;

        auto opt_msg = Message::deserialize(buffer);
        if (!opt_msg)
            return false;

        msg = *opt_msg;
        return true;
    }

    bool NetworkUtils::recv_message_buffered(int socket, ReadBuffer &rb, Message &msg)
    {
        // ensure we have at least the 2-byte length prefix
        while (rb.available() < 2)
            if (!rb.refill_spin(socket))
                return false;

        uint16_t msg_len;
        memcpy(&msg_len, rb.data(), 2);
        msg_len = ntohs(msg_len);

        if (msg_len > MAX_MESSAGE_SIZE)
            return false;

        size_t total_needed = 2 + msg_len;

        // ensure we have the full message
        while (rb.available() < total_needed)
            if (!rb.refill_spin(socket))
                return false;

        // deserialize directly from buffer — zero extra allocation
        auto opt_msg = Message::deserialize(rb.data() + 2, msg_len);
        rb.consume(total_needed);

        if (!opt_msg)
            return false;
        msg = *opt_msg;
        return true;
    }

    bool NetworkUtils::send_bytes_raw(int socket, const uint8_t *data, size_t len)
    {
        struct msghdr hdr{};
        struct iovec iov;
        iov.iov_base = const_cast<uint8_t *>(data);
        iov.iov_len = len;
        hdr.msg_iov = &iov;
        hdr.msg_iovlen = 1;
        return sendmsg(socket, &hdr, MSG_NOSIGNAL) == static_cast<ssize_t>(len);
    }

    vector<uint8_t> NetworkUtils::build_wire_bytes(
        MessageType type, ShardId shard_id, NodeId sender_id, SequenceNumber seq,
        const uint8_t *data, size_t data_len)
    {
        size_t payload = 4 + 4 + 4 + 8 + 4 + data_len + 4; // all fields
        std::vector<uint8_t> wire;
        wire.resize(2 + payload);
        size_t off = 0;

        uint16_t wire_len = htons(static_cast<uint16_t>(payload));
        memcpy(wire.data() + off, &wire_len, 2);
        off += 2;
        uint32_t net_type = htonl(type);
        memcpy(wire.data() + off, &net_type, 4);
        off += 4;
        uint32_t net_shard = htonl(shard_id);
        memcpy(wire.data() + off, &net_shard, 4);
        off += 4;
        uint32_t net_sender = htonl(sender_id);
        memcpy(wire.data() + off, &net_sender, 4);
        off += 4;
        uint64_t net_seq = htonll(seq);
        memcpy(wire.data() + off, &net_seq, 8);
        off += 8;
        uint32_t net_data_len = htonl(static_cast<uint32_t>(data_len));
        memcpy(wire.data() + off, &net_data_len, 4);
        off += 4;
        if (data && data_len > 0)
        {
            memcpy(wire.data() + off, data, data_len);
            off += data_len;
        }
        uint32_t net_ord = htonl(0);
        memcpy(wire.data() + off, &net_ord, 4);

        return wire;
    }

    // returns pointer into buffer at start of message body, sets msg_len
    // returns nullptr on failure
    // caller MUST call rb.consume(2 + msg_len) when done with the data
    const uint8_t *NetworkUtils::recv_raw_buffered(int socket, ReadBuffer &rb, size_t &msg_len)
    {
        while (rb.available() < 2)
            if (!rb.refill_spin(socket))
                return nullptr;

        uint16_t len;
        memcpy(&len, rb.data(), 2);
        len = ntohs(len);

        if (len > MAX_MESSAGE_SIZE)
            return nullptr;

        msg_len = len;
        size_t total_needed = 2 + len;

        while (rb.available() < total_needed)
            if (!rb.refill_spin(socket))
                return nullptr;

        return rb.data() + 2; // pointer into buffer, past length prefix
    }

    /*
    -------------------------------------------------------------------------------------------
    Socket Creation and Configuration
    -------------------------------------------------------------------------------------------
    */

    // useful for clients in ziplog architecture
    int NetworkUtils::create_connector_socket()
    {
        // bind to socket
        int sockfd = socket(AF_INET, SOCK_STREAM, 0); // creates socket
        if (sockfd < 0)
            return -1;

        int flag = 1;
        setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        return sockfd;
    }

    // for outgoing connections to an ip address
    bool NetworkUtils::connect_to_address(int connector_socket, const string &ip, int port)
    {
        // set up recipient server addr
        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr);

        // return success (connect < 0 signifies failure)
        return connect(connector_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) >= 0;
    }

    int NetworkUtils::create_listening_socket(const string &ip, int port, bool reuse_addr)
    {
        // create socket server
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0)
            return -1;

        int flag = 1;
        setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        if (reuse_addr)
        {
            // set sock option to allow re-use (https://pubs.opengroup.org/onlinepubs/009695099/functions/setsockopt.html)
            int enabled = 1;
            if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0)
            {
                close(sockfd);
                return -1;
            }
        }

        // bind socket to address
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

        if (bind(sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        {
            close(sockfd);
            return -1;
        }

        // listen for connections (https://man7.org/linux/man-pages/man2/listen.2.html)
        if (listen(sockfd, 256) < 0)
        {
            close(sockfd);
            return -1;
        }

        return sockfd;
    }

    int NetworkUtils::connect_to_address_persistent(const string &ip, int port)
    {
        int sock = create_connector_socket();
        if (sock < 0)
        {
            return -1;
        }

        if (!connect_to_address(sock, ip, port))
        {
            cerr << "connect failed: " << strerror(errno) << endl;
            close(sock);
            return -1;
        }

        return sock;
    }

    std::optional<MessageHeader> NetworkUtils::peek_header(const ReadBuffer &rb)
    {
        // need 2-byte length prefix + at least WIRE_SIZE bytes
        if (rb.available() < 2 + MessageHeader::SIZE)
            return std::nullopt;

        // rb.data()[0..1] is the length prefix — skip it, peek into payload
        return MessageHeader::peek(rb.data() + 2, rb.available() - 2);
    }
}