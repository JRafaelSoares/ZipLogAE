#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#include <zip/util/util.h>

/** Define some types for this API. */
using gsn_t        = uint64_t;
using client_t     = uint16_t;
using subscriber_t = uint16_t;
using shard_t      = uint8_t;
using replica_t    = uint8_t;

namespace zip::api {

/**
 * Message types.
 */
enum message_types {

    /*=== begin messages ===*/
    ORDER_INTRO = 1,
    CLIENT_INTRO,
    STORAGE_INTRO,
    SUBSCRIBER_INTRO,

    /*=== end messages ===*/
    ORDER_FINISHED,
    CLIENT_FINISHED,
    STORAGE_FINISHED,
    SUBSCRIBER_FINISHED,

    /*=== messages received by the client ===*/
    CLIENT_NEW_EPOCH,
    CLIENT_APPEND_ACK,

    /*=== messages received by the ordering server ===*/
    ORDER_CLIENT_RATE,

    /*=== messages received by the storage server === */
    STORAGE_APPEND,
    STORAGE_NEW_EPOCH,
    STORAGE_FINALIZE_CLIENT,

    /*=== messages received by the subscriber ===*/
    SUBSCRIBER_LOG_ENTRIES,

    /*=== total number of message types ===*/
    MAX_MESSAGE_TYPE

};

struct msg_net_info {
    uint8_t ipv4[4];
    uint16_t port;
} __attribute__((packed));

msg_net_info net_info_to_msg(util::net_info net_inf);

/**
 * A struct for a basic message type.
 *
 * @tparam T the concrete message type
 */
template <typename T>
struct message {

    /** This is the current type. */
    using type = T;

    /** type tag for this message */
    static constexpr uint32_t tag = std::numeric_limits<uint8_t>::max();

    /** Default constructor */
    message() = default;

    /** Delete the copy and move constructors. */
    message(const message&) = delete;
    message& operator=(const message&) = delete;
    message(message&&) = delete;
    message& operator=(message&&) = delete;

    /** Returns the length of the current message. */
    inline uint32_t length() {
        return sizeof(T);
    }

    uint32_t message_type; /** type of this message */

} __attribute__((packed));

/**
 * A struct for order intro.
 */
struct order_intro: message<order_intro> {

    /** type tag for this message */
    static constexpr uint32_t tag = ORDER_INTRO;

} __attribute__((packed));

/**
 * A struct for client intro.
 */
struct client_intro: message<client_intro> {

    /** type tag for this message */
    static constexpr uint32_t tag = CLIENT_INTRO;

    client_t client_id; /** ID of the client */
    shard_t  shard_id;  /** ID of the storage server shard */
    msg_net_info address; /** ipv4 and port for this client */

} __attribute__((packed));

/**
 * A struct for storage intro.
 */
struct storage_intro: message<storage_intro> {

    /** type tag for this message */
    static constexpr uint32_t tag = STORAGE_INTRO;

    shard_t   shard_id;   /** ID of the shard for this storage server */
    replica_t replica_id; /** ID of the replica for this storage server */
    uint32_t client_threads; /** Number of client threads at the server */

    msg_net_info address; /** ipv4 and port for this storage server */

} __attribute__((packed));

/**
 * A struct for subscriber intro.
 */
struct subscriber_intro: message<subscriber_intro> {

    /** type tag for this message */
    static constexpr uint32_t tag = SUBSCRIBER_INTRO;

    subscriber_t subscriber_id; /** ID of the subscriber */
    msg_net_info address; /** ipv4 and port for this storage server */
    uint32_t erpc_index; /** eRPC index for server to connect to */

} __attribute__((packed));

/**
 * A struct for order finished.
 */
struct order_finished: message<order_finished> {

    /** type tag for this message */
    static constexpr uint32_t tag = ORDER_FINISHED;

} __attribute__((packed));

/**
 * A struct for client finished.
 */
struct client_finished: message<client_finished> {

    /** type tag for this message */
    static constexpr uint32_t tag = CLIENT_FINISHED;

    client_t client_id; /** ID of the client */

} __attribute__((packed));

/**
 * A struct for storage finished.
 */
struct storage_finished: message<storage_finished> {

    /** type tag for this message */
    static constexpr uint32_t tag = STORAGE_FINISHED;

    shard_t   shard_id;   /** ID of the shard for this storage server */
    replica_t replica_id; /** ID of the replica for this storage server */

} __attribute__((packed));

/**
 * A struct for subscriber finished.
 */
struct subscriber_finished: message<subscriber_finished> {

    /** type tag for this message */
    static constexpr uint32_t tag = SUBSCRIBER_FINISHED;

    subscriber_t subscriber_id; /** ID of the subscriber */

} __attribute__((packed));

/**
 * A struct for storage append.
 */
struct storage_append: message<storage_append> {

    /** type tag for this message */
    static constexpr uint32_t tag = STORAGE_APPEND;

    /** Returns the length of the current message. */
    inline uint32_t length() {
        return sizeof(*this) + data_length;
    }

    uint32_t  num_slots;   /** minimum number of slots to close */
    uint32_t  data_length; /** size of the data */
    client_t  client_id;   /** ID of the client */
    std::byte data[0];     /** beginning of the data --- NOTE: this must always be the last member */

} __attribute__((packed));

/**
 * A struct for client append ack.
 */
struct client_append_ack: message<client_append_ack> {

    /** type tag for this message */
    static constexpr uint32_t tag = CLIENT_APPEND_ACK;

    gsn_t     gsn;        /** GSN of the message */
    replica_t replica_id; /** ID of the storage server replica */

} __attribute__((packed));

/**
 * A struct for order client rate.
 */
struct order_client_rate: message<order_client_rate> {

    /** type tag for this message */
    static constexpr uint32_t tag = ORDER_CLIENT_RATE;

    uint32_t num_slots; /** number of slots per epoch to assign */
    client_t client_id; /** ID of the client */

} __attribute__((packed));

/**
 * A struct for storage new epoch.
 */
struct storage_new_epoch: message<storage_new_epoch> {

    /** type tag for this message */
    static constexpr uint32_t tag = STORAGE_NEW_EPOCH;

    /**
     * A struct for the GSNs of a single client.
     */
    struct assignment_t {

        uint32_t num_slots;  /** number of GSNs in this epoch */
        client_t client_id;  /** ID of the client */

    } __attribute__((packed));

    /** Returns the length of the current message. */
    inline uint32_t length() {
        return sizeof(*this) + num_clients * sizeof(assignment_t);
    }

    gsn_t        start_gsn;      /** start of the GSNs */
    uint64_t     begin;          /** beginning of the epoch in nanoseconds since UNIX epoch */
    uint32_t     num_clients;    /** number of clients */
    assignment_t assignments[0]; /** beginning of the assignments --- NOTE: this must always be the last member */

} __attribute__((packed));

/**
 * A struct for storage finalize client.
 */
struct storage_finalize_client: message<storage_finalize_client> {

    /** type tag for this message */
    static constexpr uint32_t tag = STORAGE_FINALIZE_CLIENT;

    client_t client_id; /** ID of the client */

} __attribute__((packed));

/**
 * A struct for client new epoch.
 */
struct client_new_epoch: message<client_new_epoch> {

    /** type tag for this message */
    static constexpr uint32_t tag = CLIENT_NEW_EPOCH;

    uint32_t num_slots; /** number of slots assigned this epoch */
    uint64_t begin;     /** beginning of the epoch in nanoseconds since UNIX epoch */

} __attribute__((packed));

/**
 * A struct for subscriber log entries.
 */
struct subscriber_log_entries: message<subscriber_log_entries> {

    /** type tag for this message */
    static constexpr uint64_t tag = SUBSCRIBER_LOG_ENTRIES;

    /**
     * A struct for a single log entry.
     */
    struct entry_t {

        /** Returns the length of this entry. */
        inline uint32_t length() {
            return sizeof(*this) + data_length;
        }

        client_t client_id;   /** ID of the client */
        gsn_t    gsn;         /** GSN of this log entry */
        uint32_t data_length; /** size of the data */
        std::byte data[0];    /** beginning of the data -- NOTE: must be the last item in the struct */

    } __attribute__((packed));

    /** Returns the length of the current message. */
    inline uint32_t length() {
        return sizeof(*this) + data_length;
    }

    shard_t    shard_id;    /** ID of the shard for this storage server */
    replica_t  replica_id;  /** ID of the replica for this storage server */
    uint32_t   num_entries; /** number of entries in the message */
    uint32_t   data_length; /** length of the entire message combined */
    std::byte  entries[0];  /** beginning of the entries -- NOTE: must be last item in the struct */

} __attribute__((packed));

} // namespace zip::api
