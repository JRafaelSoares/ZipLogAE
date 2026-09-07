#pragma once
#include "message.h"

namespace ziplog::api
{
    class NetworkUtils
    {
    public:
        /*
        -------------------------------------------------------------------------------------------
        Request and Response Pattern
        -------------------------------------------------------------------------------------------
        */

        /*
            @brief: Creates connector socket and calls send_message_to_address() at most max_retries times.
            @param ip: ipv4 address of recipient
            @param port: port of recipient
            @param msg: message being sent to recipient
            @param max_retries: max number of retries
            @return: true on communication success (and fills resp), false otherwise
        */
        static bool send_message_to_address(const string &ip, int port, const Message &msg, Message &resp, int max_retries);

        /*
            @brief: Send/receive once. Checks that the message/acknowledgment sequence number match
            @param socket: 'connector socket' file descriptor
            @param msg: message being sent on socket
            @return: true on success in sending/receiving to/from recipient, false otherwise
        */
        static bool send_message_and_wait_for_ack(int socket, const Message &msg);

        /*
        -------------------------------------------------------------------------------------------
        Sending/Reading Bytes over a Connection
        -------------------------------------------------------------------------------------------
        */

        /*
            @brief: Serializes message and writes it to a socket
            @param socket: file descriptor being written to
            @param msg: message to serialize and send
            @return: true on success in writing size header and serialized struct to file descriptor
        */
        static bool send_message(int socket, const Message &msg);
        static bool send_message_og(int socket, const Message &msg);

        /*
             @param: file descriptor to write to, bytes of serialized data, the size of serialized data
             @return: boolean of success of writing all bytes to file descriptor
        */
        static bool send_bytes(int socket, const void *data, size_t len);

        /*
             @param: file descriptor to read from, message struct that has been deserialized
             @return: boolean of success of reading size header and deserializing message struct from file descriptor
        */
        static bool recv_message(int socket, Message &msg);
        static bool recv_message_spin(int socket, Message &msg);

        /*
             @param: file descriptor to read from, buffer to write the read bytes to, the number of bytes to read in
             @return: boolean of success of reading all bytes from file descriptor
        */
        static bool recv_bytes(int socket, void *data, size_t len);
        static bool recv_bytes_spin(int socket, void *data, size_t len);

        /*
        -------------------------------------------------------------------------------------------
        Socket Creation and Configuration
        -------------------------------------------------------------------------------------------
        */

        /*
            @param: number of milliseconds to wait to receive on a connection before timing out
            @return: file descriptor for socket for outgoing connections
        */
        static int create_connector_socket();

        /*
            @param: ipv4 address, port and a flag marking if the socket should be re-usable
            @return: file descriptor for socket for incoming connections
        */
        static int create_listening_socket(const string &ip, int port, bool reuse_addr = true);

        /*
            @param: 'connector socket' file descriptor, ipv4 address and port of outgoing connection
            @return: boolean of success in connecting to recipient
        */
        static bool connect_to_address(int socket, const string &ip, int port);

        static int connect_to_address_persistent(const string &ip, int port);

        struct ReadBuffer
        {
            static constexpr size_t CAPACITY = MAX_MESSAGE_SIZE * 2;
            uint8_t buf[CAPACITY];
            size_t head = 0; // start of unconsumed data
            size_t tail = 0; // end of received data

            size_t available() const { return tail - head; }
            void reset() { head = tail = 0; }

            // contiguous bytes available from head
            // if message wraps, we handle it separately
            const uint8_t *data() const { return buf + head; }

            void consume(size_t n)
            {
                head += n;
                if (head == tail)
                    reset();
            }

            bool refill_spin(int socket)
            {
                // shift if running low on space
                if (head > CAPACITY / 2)
                {
                    size_t avail = available();
                    memmove(buf, buf + head, avail); // only when necessary
                    tail = avail;
                    head = 0;
                }

                while (true)
                {
                    ssize_t n = recv(socket, buf + tail, CAPACITY - tail, MSG_DONTWAIT);
                    if (n > 0)
                    {
                        tail += n;
                        return true;
                    }
                    if (n < 0 && errno == EAGAIN)
                        continue;
                    return false; // connection closed
                }
            }
        };

        static bool recv_message_buffered(int socket, ReadBuffer &rb, Message &msg);

        static const uint8_t *recv_raw_buffered(int socket, ReadBuffer &rb, size_t &msg_len);

        static std::optional<MessageHeader> peek_header(const ReadBuffer &rb);

        static bool send_bytes_raw(int socket, const uint8_t *data, size_t len);

        static vector<uint8_t> build_wire_bytes(
            MessageType type, ShardId shard_id, NodeId sender_id, SequenceNumber seq,
            const uint8_t *data, size_t data_len);

        static const uint8_t *get_data_ptr(const uint8_t *buf, size_t msg_len, size_t &data_len_out)
        {
            if (msg_len < 24)
            {
                data_len_out = 0;
                return nullptr;
            }
            uint32_t data_len;
            memcpy(&data_len, buf + 20, 4);
            data_len_out = ntohl(data_len);
            return buf + 24;
        }
    };
}