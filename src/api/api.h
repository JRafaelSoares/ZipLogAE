#pragma once

#include <chrono>

#include <netinet/ip.h>

namespace zip {
namespace api {

/**
 * Message types.
 */

enum message_type {

    /*=== begin messages ===*/
    ORDER_INTRO = 1,
    CLIENT_INTRO = 2,
    STORAGE_INTRO = 3,
    SUBSCRIBER_INTRO = 4,

    /*=== end messages ===*/
    CLIENT_FINISHED = 5,
    SUBSCRIBER_FINISHED = 6,
    STORAGE_FINISHED = 7,
    ORDER_FINISHED = 8,

    /*=== messages received by the client ===*/
    CLIENT_STORAGE_INFO = 9,
    CLIENT_NEW_EPOCH = 10,
    CLIENT_INSERT_ACK = 11,

    /*=== messages received by the ordering server ===*/
    ORDER_CLIENT_REQUEST = 12,

    /*=== messages received by the storage server === */
    STORAGE_ORDER_SLOTS = 13,
    STORAGE_INSERT_AFTER = 14,
    STORAGE_CLIENT_FINALISE = 15,
    STORAGE_CLIENT_INITIALISE = 16,

    /*=== messages received by the subscriber ===*/
    SUBSCRIBER_LOG_ENTRY = 17,
    SUBSCRIBER_STORAGE_INFO = 18,

    /*=== co-located app message ===*/
    ZIPKAT_CLIENT_INTRO = 101,
    ZIPKAT_GET = 102,
    ZIPKAT_GET_RESPONSE = 103,
    ZIPKAT_COMMIT = 104,
    ZIPKAT_COMMIT_RESPONSE = 105,

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
    static constexpr uint64_t tag = 0;

    /** Default constructor */
    message() = default;

    /** Delete the copy constructor. */
    message(const message&) = delete;
    message& operator=(const message&) = delete;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(T);
    }

    uint64_t message_type; /** type of this message */

} __attribute__((packed));

/**
 * A struct for storage intro.
 */
struct storage_intro: message<storage_intro> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_INTRO;

    uint64_t shard_id;          /** ID of the shard */
    uint64_t replica_id;        /** ID of the shard's replica */
    uint32_t client_queues;     /** number of client queues on this server */
#ifdef COLOCATED_ZIPKAT
    uint32_t subscriber_queues; /** number of subscriber queues on this server */
    uint32_t get_queues;        /** number of zipkat get queues on this server */
#endif
    uint16_t port;              /** port number for the server */

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
 * A struct for storage insert after.
 */
struct storage_insert_after: message<storage_insert_after> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_INSERT_AFTER;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + data_length;
    }

    uint64_t client_id;   /** ID of the client */
#ifdef COLOCATED_ZIPKAT
    uint64_t global_client_id;
#endif
    uint64_t gsn_after;   /** GSN to insert after */
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

#if ZIP_CLIENT_SEQ
    uint64_t client_seq; /** client sequence number of the request */
#endif
#if ZIP_SHARD_SEQ
    uint64_t shard_seq;  /** shard sequence number of the request */
#endif
    uint64_t gsn;        /** GSN of the request, -1 if error occurred */
    uint64_t closed;     /** number of slots closed during this request */
#ifdef COLOCATED_ZIPKAT
    uint64_t app_return;  /** co-located application-specific ack value */
    uint64_t global_client_id;
#endif

} __attribute__((packed));

/**
 * A struct for client intro.
 */
struct client_intro: message<client_intro> {

    /** type tag for this message */
    static constexpr uint64_t tag = CLIENT_INTRO;

    uint64_t client_id; /** ID of the client */
    uint64_t shard_id;  /** ID of the shard */
    uint64_t num_slots; /** number of slots to assign per epoch */

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
 * A struct for subscriber intro.
 */
struct subscriber_intro: message<subscriber_intro> {

    /** type tag for this message */
    static constexpr uint64_t tag = SUBSCRIBER_INTRO;

    uint64_t subscriber_id;     /** ID of the subscriber */
    uint64_t start_gsn;         /** staring GSN to stream requests, -1 is current */

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
 * A struct for order intro.
 */
struct order_intro: message<order_intro> {

    /** type tag for this message */
    static constexpr uint64_t tag = ORDER_INTRO;

} __attribute__((packed));

/**
 * A struct for order intro.
 */
struct order_finished: message<order_finished> {

    /** type tag for this message */
    static constexpr uint64_t tag = ORDER_FINISHED;

} __attribute__((packed));

/**
 * A struct for storage order slots.
 */
struct storage_order_slots: message<storage_order_slots> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_ORDER_SLOTS;

    /**
     * A struct for the GSNs of a single client.
     */
    struct assignment {

        uint64_t client_id;  /** ID of the client, -1 if skipped */
#if ZIP_CLIENT_SEQ
        uint64_t client_seq; /** starting client sequence for this slot */
#endif
        uint64_t num_slots;  /** number of GSNs in this epoch */

    } __attribute__((packed));

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + num_clients * sizeof(assignment);
    }

    uint64_t   start_gsn;      /** start of the GSNs */
#if ZIP_SHARD_SEQ
    uint64_t   shard_seq;      /** start of the shard sequence numbers */
#endif
    uint64_t   num_clients;    /** number of clients */
    assignment assignments[0]; /** beginning of the assignments --- NOTE: this must always be the last member */

} __attribute__((packed));

/**
 * A struct for storage client initialize.
 */
struct storage_client_initialize: message<storage_client_initialize> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_CLIENT_INITIALISE;

    uint64_t client_id; /** ID of the client */

} __attribute__((packed));

/**
 * A struct for storage client finalize.
 */
struct storage_client_finalize: message<storage_client_finalize> {

    /** type tag for this message */
    static constexpr uint64_t tag = STORAGE_CLIENT_FINALISE;

    uint64_t client_id; /** ID of the client */

} __attribute__((packed));

/**
 * A struct for subscriber log entry.
 */
struct subscriber_log_entry: message<subscriber_log_entry> {

    /** type tag for this message */
    static constexpr uint64_t tag = SUBSCRIBER_LOG_ENTRY;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + sizeof(uint8_t) * data_length;
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
    uint64_t client_id;   /** ID of the client */
#if ZIP_CLIENT_SEQ
    uint64_t client_seq;  /** client sequence number */
#endif
#if ZIP_SHARD_SEQ
    uint64_t shard_seq;   /** shard sequence number */
#endif
    uint64_t gsn;         /** global sequence number */
    uint64_t data_length; /** length of the data */
#ifdef COLOCATED_ZIPKAT
    uint64_t closed;      /** number of slots closed during this request */
#endif
#ifdef ZIP_MEASURE
    std::chrono::high_resolution_clock::time_point start;
#endif
    uint8_t  data[0];     /** beginning of the data -- NOTE: must be last item in the struct */

} __attribute__((packed));

/**
 * A struct for client shard info.
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
 * A struct for subscriber shard info.
 */
struct subscriber_storage_info: client_storage_info {

    /** type tag for this message */
    static constexpr uint64_t tag = SUBSCRIBER_STORAGE_INFO;

} __attribute__((packed));

/**
 * A struct for client new epoch.
 */
struct client_new_epoch: message <client_new_epoch> {

    /** type tag for this message */
    static constexpr uint64_t tag = CLIENT_NEW_EPOCH;

    std::chrono::high_resolution_clock::time_point begin;       /** beginning of the epoch */
    uint64_t                                       num_slots; /** number of slots assigned this epoch */

} __attribute__((packed));

// HACK: this is just a dummy msg to let the zipkat client get one send queue for GET reqeust
/**
 * Co-located zipkat client intro.
 */
struct zipkat_client_intro: message<zipkat_client_intro> {

    /** type tag for this message */
    static constexpr uint64_t tag = ZIPKAT_CLIENT_INTRO;

} __attribute__((packed));

/**
 * Co-located zipkat GET request.
 */
struct zipkat_get: message <zipkat_get> {

    /** type tag for this message */
    static constexpr uint64_t tag = ZIPKAT_GET;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this);
        //return sizeof(*this) + data_length * sizeof(uint8_t);
    }

    uint64_t client_id;   /** ID of the client */
    uint64_t mid;         /** message id for identifying the same request */
    uint64_t gsn;         /** GSN as the requested timestamp of the key */
    uint64_t data_length; /** size of the key */
    char  key[64];      /** beginning of the key --- NOTE: this must always be the last member */
    //uint8_t  key[0];      /** beginning of the key --- NOTE: this must always be the last member */
};

/**
 * Co-located zipkat GET response.
 */
struct zipkat_get_response: message <zipkat_get_response> {

    /** type tag for this message */
    static constexpr uint64_t tag = ZIPKAT_GET_RESPONSE;
    static constexpr uint64_t kKeyNotFound = -2;

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this);
        //return sizeof(*this) + data_length * sizeof(uint8_t);
    }

    uint64_t replica_id;  /** ID of the replica within a shard */
    uint64_t client_id;   /** ID of the client */
    uint64_t mid;         /** message id for identifying the same request */
    uint64_t gsn;         /** GSN as the timestamp of the value */
    uint64_t data_length; /** size of the value */
    char     value[64];    /** beginning of the value --- NOTE: this must always be the last member */
    //uint8_t  value[0];    /** beginning of the value --- NOTE: this must always be the last member */
};

// HACK: this should not be in zip::api.
/**
 * Co-located commit request message.
 */
struct zipkat_commit_request {

    /** Returns the length of the current message. */
    inline unsigned long length() {
        return sizeof(*this) + data_length * sizeof(uint8_t);
    }

    uint64_t data_length; /** size of the value */
    uint64_t nr_reads;    /** number of reads */
    uint64_t nr_writes;   /** number of writes */
    uint8_t  data[0];    /** beginning of the txn data --- NOTE: this must always be the last member */
};

} // namespace api
} // namespace zip
