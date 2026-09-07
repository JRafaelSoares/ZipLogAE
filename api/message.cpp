#include "message.h"
#include <arpa/inet.h>
#include <cstring>

namespace ziplog
{
    namespace api
    {

        // https://linux.die.net/man/3/htonl
        // https://cplusplus.com/reference/vector/vector/insert/#google_vignette

        vector<uint8_t> Message::serialize() const
        {

            // validate message struct does not exceed desired size
            uint32_t struct_size = 4 + 4 + 4 + 8 +
                                   4 + static_cast<uint32_t>(data.size()) +                 // data length + content
                                   4 + (static_cast<uint32_t>(ordering_values.size()) * 8); // vector length + content

            if (struct_size > MAX_MESSAGE_SIZE)
                return {};

            vector<uint8_t> buffer;
            buffer.reserve(struct_size); // pre-allocate

            // add type (4 byte)
            uint32_t net_type = htonl(static_cast<uint32_t>(type));
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(&net_type),
                          reinterpret_cast<const uint8_t *>(&net_type) + 4);

            // add shard id (4 bytes)
            uint32_t net_shard_id = htonl(shard_id);
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(&net_shard_id),
                          reinterpret_cast<const uint8_t *>(&net_shard_id) + 4);

            // add sender_id (4 bytes)
            uint32_t net_sender_id = htonl(sender_id);
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(&net_sender_id),
                          reinterpret_cast<const uint8_t *>(&net_sender_id) + 4);

            // add sequence_number/count (8 bytes)
            uint64_t net_seq = htonll(seq_or_count);
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(&net_seq),
                          reinterpret_cast<const uint8_t *>(&net_seq) + 8);

            // add data length (4 bytes)
            uint32_t data_len = htonl(static_cast<uint32_t>(data.size()));
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(&data_len),
                          reinterpret_cast<const uint8_t *>(&data_len) + 4);

            // add data
            buffer.insert(buffer.end(), data.begin(), data.end());

            // add size of ordering_values vector (4 bytes)
            uint32_t vec_size = htonl(static_cast<uint32_t>(ordering_values.size()));
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(&vec_size),
                          reinterpret_cast<const uint8_t *>(&vec_size) + 4);

            // add all elements from vector
            for (uint64_t val : ordering_values)
            {
                uint64_t net_val = htonll(val); // 64-bit conversion
                buffer.insert(buffer.end(),
                              reinterpret_cast<const uint8_t *>(&net_val),
                              reinterpret_cast<const uint8_t *>(&net_val) + 8);
            }

            return buffer;
        }

        std::optional<Message> Message::deserialize(const vector<uint8_t> &buffer)
        {
            if (buffer.size() < 28)
            { // minimum size: 5 fields × 4 bytes each + 8 bytes
                ZLOG("billy size = " << buffer.size());
                return nullopt;
            }

            Message msg;
            size_t offset = 0;

            // read type
            uint32_t net_type;
            memcpy(&net_type, buffer.data() + offset, 4);
            msg.type = static_cast<MessageType>(ntohl(net_type));
            offset += 4;

            // read shard_id
            uint32_t net_shard_id;
            memcpy(&net_shard_id, buffer.data() + offset, 4);
            msg.shard_id = ntohl(net_shard_id);
            offset += 4;

            // read sender_id
            uint32_t net_sender_id;
            memcpy(&net_sender_id, buffer.data() + offset, 4);
            msg.sender_id = ntohl(net_sender_id);
            offset += 4;

            // read sequence_number/count
            uint64_t net_seq_or_count;
            memcpy(&net_seq_or_count, buffer.data() + offset, 8);
            msg.seq_or_count = ntohll(net_seq_or_count);
            offset += 8;

            // read data length
            uint32_t data_len;
            memcpy(&data_len, buffer.data() + offset, 4);
            data_len = ntohl(data_len);
            offset += 4;

            // validate data length
            if (offset + data_len > buffer.size())
            {
                ZLOG("mandy");
                return nullopt; // buffer too small for claimed data length
            }

            // read data
            msg.data = Command(buffer.begin() + offset, buffer.begin() + offset + data_len);
            offset += data_len;

            // read ordering values length
            if (offset + 4 > buffer.size())
            {
                ZLOG("grim");
                return nullopt;
            }
            uint32_t ordering_len;
            memcpy(&ordering_len, buffer.data() + offset, 4);
            ordering_len = ntohl(ordering_len);
            offset += 4;

            // validate vector size
            if (offset + (ordering_len * 8) > buffer.size())
            {
                ZLOG("paul");
                return nullopt;
            }

            // read each number of sequence array
            msg.ordering_values.reserve(ordering_len);
            for (uint32_t i = 0; i < ordering_len; i++)
            {
                uint64_t net_val;
                memcpy(&net_val, buffer.data() + offset, 8);
                msg.ordering_values.push_back(ntohll(net_val));
                offset += 8;
            }

            return msg;
        }

        std::optional<Message> Message::deserialize(const uint8_t *buffer, size_t size)
        {
            if (size < 28)
                return nullopt;

            Message msg;
            size_t offset = 0;

            uint32_t net_type;
            memcpy(&net_type, buffer + offset, 4);
            msg.type = static_cast<MessageType>(ntohl(net_type));
            offset += 4;

            uint32_t net_shard_id;
            memcpy(&net_shard_id, buffer + offset, 4);
            msg.shard_id = ntohl(net_shard_id);
            offset += 4;

            uint32_t net_sender_id;
            memcpy(&net_sender_id, buffer + offset, 4);
            msg.sender_id = ntohl(net_sender_id);
            offset += 4;

            uint64_t net_seq;
            memcpy(&net_seq, buffer + offset, 8);
            msg.seq_or_count = ntohll(net_seq);
            offset += 8;

            uint32_t data_len;
            memcpy(&data_len, buffer + offset, 4);
            data_len = ntohl(data_len);
            offset += 4;

            if (offset + data_len > size)
                return nullopt;

            msg.data = Command(buffer + offset, buffer + offset + data_len);
            offset += data_len;

            if (offset + 4 > size)
                return nullopt;

            uint32_t ordering_len;
            memcpy(&ordering_len, buffer + offset, 4);
            ordering_len = ntohl(ordering_len);
            offset += 4;

            if (offset + (ordering_len * 8) > size)
                return nullopt;

            msg.ordering_values.reserve(ordering_len);
            for (uint32_t i = 0; i < ordering_len; i++)
            {
                uint64_t net_val;
                memcpy(&net_val, buffer + offset, 8);
                msg.ordering_values.push_back(ntohll(net_val));
                offset += 8;
            }

            return msg;
        }
    }
}