#pragma once

#include <chrono>
#include <limits>

#include <netinet/ip.h>

namespace zip::api {

/**
 * Message types.
 */
enum message_types {

    /*=== begin messages ===*/
    ORDER_INTRO               = 1 << 1,
    CLIENT_INTRO              = 1 << 2,
    STORAGE_INTRO             = 1 << 3,
    SUBSCRIBER_INTRO          = 1 << 4,

    /*=== end messages ===*/
    ORDER_FINISHED            = 1 << 5,
    CLIENT_FINISHED           = 1 << 6,
    STORAGE_FINISHED          = 1 << 7,
    SUBSCRIBER_FINISHED       = 1 << 8,

    /*=== messages received by the client ===*/
    CLIENT_STORAGE_INFO       = 1 << 9,
    CLIENT_NEW_EPOCH          = 1 << 10,
    CLIENT_INSERT_ACK         = 1 << 11,
    CLIENT_HEARTBEAT         = 1 << 30,

    /*=== messages received by the ordering server ===*/
    ORDER_CLIENT_RATE         = 1 << 12,
    ORDER_CLIENT_FREEZE       = 1 << 13,
    ORDER_CLIENT_CUT          = 1 << 14,
    ORDER_CLIENT_DATA         = 1 << 15,

    /*=== messages received by the storage server === */
    STORAGE_INSERT            = 1 << 16,
    STORAGE_SLOTS             = 1 << 17,
    STORAGE_CLIENT_FREEZE     = 1 << 18,
    STORAGE_CLIENT_CUT        = 1 << 19,
    STORAGE_CLIENT_PATCH      = 1 << 20,
    STORAGE_CLIENT_FINALIZE   = 1 << 21,

    /*=== messages received by the subscriber ===*/
    SUBSCRIBER_LOG_ENTRY      = 1 << 22,
    SUBSCRIBER_STORAGE_INFO   = 1 << 23

};

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
    static constexpr uint64_t tag = std::numeric_limits<uint64_t>::max();

    /** Default constructor */
    message() = default;

    /** Delete the copy and move constructors. */
    message(const message&) = delete;
    message& operator=(const message&) = delete;
    message(message&&) = delete;
    message& operator=(message&&) = delete;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(T);
    }

    uint64_t message_type; /** type of this message */

} __attribute__((packed));

/**
 * A struct for order intro.
 */
struct order_intro: message<order_intro> {

    /** type tag for this message */
    static constexpr uint64_t tag = ORDER_INTRO;

} __attribute__((packed));

/**
 * A struct for client intro.
 */
struct client_intro: message<client_intro> {

    /** type tag for this message */
    static constexpr uint64_t tag = CLIENT_INTRO;

    uint64_t client_id; /** ID of the client */
    uint64_t shard_id;  /** ID of the shard */
    uint64_t num_slots; /** number of slots per epoch to assign */

} __attribute__((packed));

/**
 * A struct for storage intro.
 */
struct storage_intro: message<storage_intro> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_INTRO;

    uint64_t shard_id;      /** ID of the shard */
    uint64_t replica_id;    /** ID of the shard's replica */
    uint64_t client_queues; /** number of client queues on this server */
    uint16_t port;          /** port number for the server */

} __attribute__((packed));

/**
 * A struct for subscriber intro.
 */
struct subscriber_intro: message<subscriber_intro> {

    /** type tag for this message */
    static constexpr uint64_t tag = SUBSCRIBER_INTRO;

    uint64_t subscriber_id; /** ID of the subscriber */
    uint64_t num_queues;    /** number of receive queues at this server */

} __attribute__((packed));

/**
 * A struct for order finished.
 */
struct order_finished: message<order_finished> {

    /** type tag for this message */
    static constexpr uint64_t tag = ORDER_FINISHED;

} __attribute__((packed));

/**
 * A struct for client finished.
 */
struct client_finished: message<client_finished> {

    /** type tag for this message */
    static constexpr uint64_t tag = CLIENT_FINISHED;

    uint64_t client_id; /** ID of the client */

} __attribute__((packed));

/**
 * A struct for storage finished.
 */
struct storage_finished: message<storage_finished> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_FINISHED;

    uint64_t shard_id;      /** ID of the shard */
    uint64_t replica_id;    /** ID of the shard's replica */

} __attribute__((packed));

/**
 * A struct for subscriber finished.
 */
struct subscriber_finished: message<subscriber_finished> {

    /** type tag for this message */
    static constexpr uint64_t tag = SUBSCRIBER_FINISHED;

    uint64_t subscriber_id; /** ID of the subscriber */

} __attribute__((packed));

/**
 * A struct for storage insert.
 */
struct storage_insert: message<storage_insert> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_INSERT;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + data_length;
    }

    uint64_t client_id;   /** ID of the client */
    uint64_t num_slots;   /** minimum number of slots to close */
    uint64_t data_length; /** size of the data buffer to store */
    uint8_t  data[0];     /** beginning of the data --- NOTE: this must always be the last member */

} __attribute__((packed));

/**
 * A struct for client insert ack.
 */
struct client_insert_ack: message<client_insert_ack> {

    /** type tag for this message */
    static constexpr uint64_t tag = CLIENT_INSERT_ACK;

    uint64_t replica_id; /** ID of the replica within a shard */
    uint64_t gsn;        /** GSN of the request, -1 if error occurred */

} __attribute__((packed));

/**
 * A struct for order client rate.
 */
struct order_client_rate: message<order_client_rate> {

    /** type tag for this message */
    static constexpr uint64_t tag = ORDER_CLIENT_RATE;

    uint64_t client_id; /** ID of the client */
    uint64_t num_slots; /** number of slots per epoch to assign */

} __attribute__((packed));

/**
 * A struct for order client cut.
 */
struct order_client_cut: message<order_client_cut> {

    /** type tag for this message */
    static constexpr uint64_t tag = ORDER_CLIENT_CUT;

    uint64_t client_id;  /** ID of the client */
    uint64_t shard_id;   /** ID of the shard */
    uint64_t replica_id; /** ID of the replica */
    uint64_t latest_gsn; /** GSN of the last entry received from this client */

} __attribute__((packed));

/**
 * A struct for order client data.
 */
struct order_client_data: message<order_client_data> {

    /** type tag for this message */
    static constexpr uint64_t tag = ORDER_CLIENT_DATA;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + data_length;
    }

    uint64_t client_id;   /** ID of the client */
    uint64_t shard_id;    /** ID of the shard */
    uint64_t replica_id;  /** ID of the replica */
    uint64_t gsn;         /** global sequence number, -1 if finished */
    uint64_t data_length; /** length of the data */
    uint8_t  data[0];     /** beginning of the data -- NOTE: must be last item in the struct */

} __attribute__((packed));

/**
 * A struct for order client freeze.
 */
struct order_client_freeze: message<order_client_freeze> {

    /** type tag for this message */
    static constexpr uint64_t tag = ORDER_CLIENT_FREEZE;

    uint64_t client_id;  /** ID of the client */

} __attribute__((packed));

/**
 * A struct for storage slots.
 */
struct storage_slots: message<storage_slots> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_SLOTS;

    /**
     * A struct for the GSNs of a single client.
     */
    struct assignment {

        uint64_t client_id;  /** ID of the client, -1 if skipped */
        uint64_t num_slots;  /** number of GSNs in this epoch */

    } __attribute__((packed));

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + num_clients * sizeof(assignment);
    }

    uint64_t   start_gsn;      /** start of the GSNs */
    uint64_t   num_clients;    /** number of clients */
    assignment assignments[0]; /** beginning of the assignments --- NOTE: this must always be the last member */

} __attribute__((packed));

/**
 * A struct for storage client finalize.
 */
struct storage_client_finalize: message<storage_client_finalize> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_CLIENT_FINALIZE;

    uint64_t client_id; /** ID of the client */

} __attribute__((packed));

/**
 * A struct for storage client freeze.
 */
struct storage_client_freeze: message<storage_client_freeze> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_CLIENT_FREEZE;

    uint64_t client_id; /** ID of the client */

} __attribute__((packed));

/**
 * A struct for storage client cut.
 */
struct storage_client_cut: message<storage_client_cut> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_CLIENT_CUT;

    uint64_t client_id;  /** ID of the client */
    uint64_t begin_gsn;  /** GSN starting which to request entries */

} __attribute__((packed));

/**
 * A struct for storage client patch.
 */
struct storage_client_patch: message<storage_client_patch> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_CLIENT_PATCH;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + data_length;
    }

    uint64_t client_id;   /** ID of the client */
    uint64_t gsn;         /** global sequence number */
    uint64_t data_length; /** length of the data */
    uint8_t  data[0];     /** beginning of the data -- NOTE: must be last item in the struct */

} __attribute__((packed));

/**
 * A struct for subscriber log entry.
 */
struct subscriber_log_entry: message<subscriber_log_entry> {

    /** type tag for this message */
    static constexpr uint64_t tag = SUBSCRIBER_LOG_ENTRY;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + data_length;
    }

    /**
     * The subscriber doesn't care about replica ID
     * so we can use the same slot to store the
     * number of ACKs needed.
     */
    union {
    uint64_t replica_id;  /** ID of this shard's replica */
    uint64_t num_acks;    /** number of ACKs needed to deliver */
    };

    uint64_t shard_id;    /** ID of this shard */
    uint64_t thread_id;   /** ID of the thread on this storage server */
    uint64_t client_id;   /** ID of the client */
    uint64_t gsn;         /** global sequence number */
    uint64_t data_length; /** length of the data */
    uint8_t  data[0];     /** beginning of the data -- NOTE: must be last item in the struct */

} __attribute__((packed));

/**
 * A struct for client storage info.
 */
struct client_storage_info: message<client_storage_info> {

    /** type tag for this message */
    static constexpr uint64_t tag = CLIENT_STORAGE_INFO;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + sizeof(sockaddr_in) * num_servers;
    }

    uint64_t    num_servers;  /** number of servers in the message */
    sockaddr_in addresses[0]; /** beginning of the server addresses -- NOTE: must be last item in the struct */

} __attribute__((packed));

/**
 * A struct for subscriber storage info.
 */
struct subscriber_storage_info: message<subscriber_storage_info> {

    /** type tag for this message */
    static constexpr uint64_t tag = SUBSCRIBER_STORAGE_INFO;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + sizeof(sockaddr_in) * num_servers;
    }

    uint64_t    num_servers;  /** number of servers in the message */
    sockaddr_in addresses[0]; /** beginning of the server addresses -- NOTE: must be last item in the struct */

} __attribute__((packed));

/**
 * A struct for client new epoch.
 */
struct client_new_epoch: message<client_new_epoch> {

    /** type tag for this message */
    static constexpr uint64_t tag = CLIENT_NEW_EPOCH;

    std::chrono::high_resolution_clock::time_point begin;     /** beginning of the epoch */
    uint64_t                                       num_slots; /** number of slots assigned this epoch */

} __attribute__((packed));

/**
 * A struct for client hearbeat.
 */
struct client_heartbeat: message<client_heartbeat> {
    /** type tag for this message */
    static constexpr uint64_t tag = CLIENT_HEARTBEAT;
    uint64_t id;
} __attribute__((packed));

} // namespace zip::api
