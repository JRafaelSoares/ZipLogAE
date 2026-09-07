#pragma once
#include "types.h"
#include <cassert>

namespace ziplog
{
    namespace api
    {

        // Message type for all servers
        enum MessageTypes : uint32_t
        {
            APPEND, // client to proxy, proxy to server OR server to subscriber
            SKIP,
            SUCCESS,
            FAILURE,
            ACK,              // subscriber to server OR server to proxy
            ZIP_REQUEST,      // proxy to zipper
            ZIP_RESPONSE,     // zipper to proxy
            REPORT,           // report failure
            FREEZE,           // sent out in rounds (zipper to server)
            FREEZE_RESPONSE,  // repsonse to freeze saying how many messages received after freeze round
            TRANSFER_REQUEST, // sent between servers to share message after freeze
            FREEZE_COMPLETE,
            REGISTER_PROXY,      // new proxy trying to join system (send to zipper)
            REJOIN_PROXY,        // proxy attempting to reintegrate in system (send to zipper)
            INCLUDE_PROXY,       // zipper to all other
            REGISTER_SUBSCRIBER, // new proxy trying to join system (send to zipper)
            INCLUDE_SUBSCRIBER,  // zipper to other servers to include subscriber
        };

        struct Message
        {
            MessageType type;
            ShardId shard_id;
            NodeId sender_id;                       // index of address in config
            SequenceNumber seq_or_count;            // log index (APPEND) or num of slots (ZIP_REQUEST)
            Command data;                           // data being appended in log (APPEND). may be a batch
            vector<SequenceNumber> ordering_values; // timestamps (ZIP_REQUEST) or sequence numbers (ZIP_RESPONSE)

            // Serialization methods
            vector<uint8_t> serialize() const;                                        // returns empty vector on failure (message is too large)
            static std::optional<Message> deserialize(const vector<uint8_t> &buffer); // return std::nullopt on failure

            // overload for fixed buffer (used by proxy)
            static std::optional<Message> deserialize(const uint8_t *buf, size_t len);

            // Accessors to make intent clear (non-defensive assuming benign failures)
            SequenceNumber get_sequence_number() const
            {
                assert(type == APPEND || type == SKIP || type == ACK || type == TRANSFER_REQUEST || type == FREEZE_RESPONSE || type == FREEZE_COMPLETE);
                if (type == TRANSFER_REQUEST || type == FREEZE_RESPONSE)
                {
                    assert(ordering_values.size() > 1);
                    return ordering_values[1];
                }
                return seq_or_count;
            }

            void set_sequence_number(SequenceNumber seq)
            {
                assert(type == APPEND || type == SKIP || type == ACK || type == TRANSFER_REQUEST || type == FREEZE_RESPONSE || type == FREEZE_COMPLETE);
                if (type == TRANSFER_REQUEST || type == FREEZE_RESPONSE)
                {
                    if (ordering_values.size() < 2)
                    {
                        ordering_values.resize(2);
                    }
                    ordering_values[1] = seq;
                    return;
                }
                seq_or_count = seq;
            }

            NodeId get_failed_proxy() const
            {
                if (type == REPORT)
                {
                    return static_cast<NodeId>(seq_or_count);
                }
                else if (type == FREEZE || type == FREEZE_RESPONSE || type == TRANSFER_REQUEST || type == FREEZE_COMPLETE)
                {
                    return static_cast<NodeId>(ordering_values.front());
                }
                assert(false);
            }

            void set_failed_proxy(NodeId id)
            {
                if (type == REPORT)
                {
                    seq_or_count = static_cast<SequenceNumber>(id);
                }
                else if (type == FREEZE || type == FREEZE_RESPONSE || type == TRANSFER_REQUEST || type == FREEZE_COMPLETE)
                {
                    ordering_values = {static_cast<SequenceNumber>(id)};
                }
                else
                {
                    assert(false);
                }
            }

            void set_round(SequenceNumber round)
            {
                assert(type == FREEZE || type == FREEZE_RESPONSE || type == TRANSFER_REQUEST);
                seq_or_count = round;
            }

            int get_round() const
            {
                assert(type == FREEZE || type == FREEZE_RESPONSE || type == TRANSFER_REQUEST);
                return seq_or_count;
            }

            SequenceNumber get_num_requests() const
            {
                assert(type == ZIP_REQUEST || type == ZIP_RESPONSE);
                return seq_or_count;
            }

            void set_num_requests(SequenceNumber count)
            {
                assert(type == ZIP_REQUEST || type == ZIP_RESPONSE);
                seq_or_count = count;
            }

            const vector<SequenceNumber> &get_assigned_sequences() const
            {
                assert(type == ZIP_RESPONSE);
                return ordering_values;
            }

            void set_assigned_sequences(const vector<SequenceNumber> &sequences)
            {
                assert(type == ZIP_RESPONSE);
                ordering_values = sequences;
            }
        };

        struct MessageHeader
        {
            MessageType type;
            ShardId shard_id;
            NodeId sender_id;
            SequenceNumber seq_or_count;

            static constexpr size_t SIZE = 20;

            static std::optional<MessageHeader> peek(const uint8_t *buf, size_t available)
            {
                if (available < SIZE)
                    return std::nullopt;

                MessageHeader h;
                size_t offset = 0;

                uint32_t net_type;
                memcpy(&net_type, buf + offset, 4);
                h.type = static_cast<MessageType>(ntohl(net_type));
                offset += 4;

                uint32_t net_shard;
                memcpy(&net_shard, buf + offset, 4);
                h.shard_id = ntohl(net_shard);
                offset += 4;

                uint32_t net_sender;
                memcpy(&net_sender, buf + offset, 4);
                h.sender_id = ntohl(net_sender);
                offset += 4;

                uint64_t net_seq;
                memcpy(&net_seq, buf + offset, 8);
                h.seq_or_count = ntohll(net_seq);
                offset += 8;

                return h;
            }
        };

        inline Message
        build_message(MessageType type = 0, ShardId shard_id = 0, NodeId sender_id = 0, SequenceNumber number = 0, Command data = {}, vector<SequenceNumber> values = {})
        {
            Message message;
            message.type = type;
            message.shard_id = shard_id;
            message.sender_id = sender_id;
            message.seq_or_count = number;
            message.data = data;
            message.ordering_values = values;
            return message;
        }
    }
}