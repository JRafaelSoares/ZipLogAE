#pragma once
#include "common.h"

namespace ziplog
{
    namespace api
    {

        // type aliases
        using MessageType = uint32_t;
        using ShardId = uint32_t;
        using NodeId = uint32_t;
        using SequenceNumber = uint64_t;
        using Command = vector<uint8_t>;
        using Timestamp = uint64_t;
        using EpochDurationUnit = microseconds; // updates the epoch duration unit in components
        static constexpr const char *EPOCH_DURATION_UNIT_STR = "microsec";

        inline Command string_to_command(const string &s)
        {
            return Command(s.begin(), s.end());
        }

        inline string command_to_string(const Command &cmd)
        {
            return string(cmd.begin(), cmd.end());
        }

        class CommandBatch
        {
        private:
            Command buffer_;

        public:
            void add_command(const Command &cmd)
            {
                uint32_t len = htonl(cmd.size());
                buffer_.insert(buffer_.end(), reinterpret_cast<const uint8_t *>(&len), reinterpret_cast<const uint8_t *>(&len) + 4);
                buffer_.insert(buffer_.end(), cmd.begin(), cmd.end());
            }

            Command serialize() const
            {
                return buffer_;
            }

            static vector<Command> deserialize(const Command &data)
            {
                vector<Command> commands;
                size_t offset = 0;

                while (offset < data.size())
                {
                    // read length
                    if (offset + 4 > data.size())
                        break;

                    uint32_t net_len;
                    memcpy(&net_len, data.data() + offset, 4);
                    uint32_t len = ntohl(net_len);
                    offset += 4;

                    if (offset + len > data.size())
                        break;

                    // read command and append command to vector
                    commands.emplace_back(data.begin() + offset, data.begin() + offset + len);
                    offset += len;
                }

                return commands;
            }

            inline Command string_to_command(const string &s)
            {
                return Command(s.begin(), s.end());
            }
        };

        /*
            @return: Timepoint representing current time.
        */
        inline system_clock::time_point now_tp()
        {
            return system_clock::now();
        }

        /*
            @return: Timestamp representing current time.
        */
        inline Timestamp now()
        {
            return duration_cast<EpochDurationUnit>(now_tp().time_since_epoch()).count();
        }

        /*
            @param tp: Time point to be converted to timestamp.
            @return: Timestamp representing current time.
        */
        inline Timestamp tp_to_timestamp(const system_clock::time_point &tp)
        {
            return duration_cast<EpochDurationUnit>(tp.time_since_epoch()).count();
        }

        /*
            @param tp: Timestamp to be converted to timepointstamp.
            @return: Timepoint representing current time.
        */
        inline system_clock::time_point timestamp_to_tp(const Timestamp &ts)
        {
            return system_clock::time_point(EpochDurationUnit(ts));
        }
    }
}