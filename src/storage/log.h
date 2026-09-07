#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <list>

#include "store/common/transaction.h"
#include "util/consts.h"
#include "util/util.h"
#include <hdr/hdr_histogram.h>

namespace zip {

/** Forward declaration for some messages. */
namespace api { struct client_insert_ack;  struct storage_insert_after; struct storage_order_slots; }

namespace storage {

/**
 * This class represents the log for
 * this storage server replica.
 */
class log {

public:

    /**
     * This is the type of a single entry
     * in the log.
     */
    struct entry {

        /** client ID the entry has been assigned to */
        uint32_t client_id;

#if ZIP_CLIENT_SEQ
        /** client sequence number for this entry */
        uint64_t client_seq;
#endif

#if ZIP_SHARD_SEQ
        /** shard sequence number of the entry */
        uint64_t shard_seq;
#endif

        /** GSN of the entry */
        uint64_t gsn;

        /** length of the data in this entry */
        uint32_t data_length;

#ifdef COLOCATED_ZIPKAT
        uint32_t closed;
        std::atomic<bool> locked;  /* whether the zipkat already done locking this entry */
        std::unique_ptr<Transaction> txn;
#ifndef SUBSCRIBER_SEND_IA_ACK
        static constexpr int64_t kUnhandled = -1;
        std::atomic<int64_t> status; /* the returned COMMIT status */
#endif
        uint64_t global_client_id;
#else
        /** pointer to the data */
        uint8_t* data;
#endif
#if defined(ZIP_MEASURE) || defined(MEASURE_LOG_ITERATE)
//        std::chrono::high_resolution_clock::time_point start;
#endif

        /** whether this entry has been set */
        std::atomic<bool> set;

        /** offset of where the next slot for this client is */
        unsigned long offset;

    };

    /**
     * Type of a single block which includes
     * the GSN and log entries of this block.
     */
    using block = std::array<entry, zip::consts::NUM_ENTRIES_PER_BLOCK>;

    /**
     * Type of the linked list of blocks.
     */
    using blocks = std::list<block*>;

    /**
     * Initialise the log.
     */
    log();

    /**
     * Deinit the log.
     */
    ~log();

    /**
     * Add a new entries to this log.
     *
     * @param slots the details of the GSNs to add
     */
    void add_entries(zip::api::storage_order_slots& slots);

private:
    /**
     * Recursively generate a densely packed interleaving
     * of given sequence numbers.
     *
     * This goes over the whole assignment and generates the
     * best interleaving possible. For instance, if slots are
     * a: 2, b: 128, c: 128, d: 512, it generates:
     * a, b, c, d, d, d, d, b, c, d, d, d, d, b, c, ...
     */
    void recurse_generate(zip::api::storage_order_slots& slots, unsigned long index);

    /** Make position and iterator friends of this class */
    friend class position;
    friend class iterator;

    /** vector of blocks */
    blocks blocks_;

    /** number of entries in the log */
    unsigned long current_entries_ = 0;

    /** location where the other threads read the size of the log  */
    std::atomic<unsigned long> num_entries_ = 0;

    /** iterator to the last used entry in the log */
    block::iterator entry_;

    /** iterator to the last block */
    blocks::iterator block_;

};

/**
 * This class represents a single client's
 * position inside the log.
 */
class position {

public:

    /**
     * Initialise a position in the log.
     *
     * @param client_id ID of the client
     * @param log       reference to the global log
     */
    position(uint64_t client_id, log& log);

    /**
     * Append the given request to the log.
     *
     * @param req the append after request
     * @param ack the ack to generate for the client
     */
#ifdef SUBSCRIBER_SEND_IA_ACK
    void append_after(zip::api::storage_insert_after& req, zip::api::client_insert_ack& ack);
#else
    log::entry& append_after(zip::api::storage_insert_after& req, zip::api::client_insert_ack& ack);
#endif

    /**
     * Open this client's slots in the log.
     */
    void open();

    /**
     * Close this client's slots in the log.
     */
    void close();

private:

    /** ID of the client */
    uint64_t client_id_;

    /** whether this log is initialised */
    std::atomic<bool> open_ = false;

    /** index into the log */
    unsigned long entry_index_ = 0;

    /** latest value of number of entries read */
    unsigned long current_entries_ = 0;

    /** number of entries in the global log */
    std::atomic<unsigned long>& num_entries_;

    /** iterator to the current block */
    log::blocks::iterator iterator_;

    /** index of the block in the log */
    unsigned long block_index_ = 0;

    // TODO: remove
/*
    hdr_histogram* hist;
    int hist_count;
*/
};

/**
 * This class represents the position of the subscriber
 * in the log.
 */
class iterator {

public:

    /**
     * Initialise the iterator.
     *
     * @param log       reference to the global log
     * @param start_gsn the starting GSN to iterator from
     */
    iterator(log& log, uint64_t start_gsn = 0);

    /**
     * Try to get the next entry from the log.
     *
     * @param entry location to put the entry in
     *
     * @return whether an entry was obtained
     */
    bool next_entry(log::entry*& entry);

private:

    /** index into the log */
    unsigned long entry_index_ = 0;

    /** number of entries in the global log */
    std::atomic<unsigned long>& num_entries_;

    /** latest value of number of entries read */
    unsigned long current_entries_;

    /** iterator to the current block */
    log::blocks::iterator iterator_;

    /** index of the block in the log */
    unsigned long block_index_ = 0;

    /** pointer to the current entry */
    log::entry* entry_ = nullptr;

};

} // namespace storage
} // namespace zip
