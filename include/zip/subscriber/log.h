#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <list>
#include <optional>

#include <zip/api/api.h>
#include <zip/network/buffer.h>
#include <zip/util/consts.h>

namespace zip::subscriber {

/**
 * This is the API-level type of a log entry.
 */
struct entry_t {

    /** GSN of the entry */
    gsn_t gsn;

    /** data for the log entry */
    uintptr_t data;

    /** size of the data for this entry */
    uint32_t data_length;

    /** ID of the client */
    client_t client_id;

};

/**
 * This is the underlying type of a log entry.
 */
struct block_entry: entry_t {

    /** whether this entry is set */
    std::atomic<bool> set;
    
};

/**
 * Type of a single block which includes
 * the log entries of this block.
 */
using block_t = std::array<block_entry, zip::consts::NUM_ENTRIES_PER_BLOCK>;

/**
 * Type of the linked list of blocks.
 */
using blocks_t = std::list<block_t*>;

/**
 * Type of the linked list of counters.
 */
using counters_t = std::list<std::atomic<uint32_t>>;

/**
 * This class represents the log for this storage server.
 */
class log {

public:

    /**
     * Initialize the log.
     *
     * @param num_iterators number of iterators for this log
     */
    log(uint32_t num_iterators);

    /**
     * Deinitialize the log.
     */
    ~log();

private:

    /** Make position and iterator friends of this class */
    friend class position;
    friend class iterator;

    /** lock to protect the state */
    std::mutex lock_;

    /** blocks of this log */
    blocks_t blocks_;

    /** counters for the blocks of this log */
    counters_t counters_;

    /** number of blocks in this log  */
    std::atomic<uint32_t> num_blocks_ = 0;

    /** iterator to the first to recycle block for this log */
    blocks_t::iterator block_;

    /** iterator to the counter for this log */
    counters_t::iterator counter_;

    /** number of iterators for this log */
    const uint32_t num_iterators_;

};

/**
 * This class represents a single thread's position inside the log.
 */
class position {

public:

    /**
     * Initialize a position in the log.
     *
     * @param log reference to the global log
     */
    position(log& log);

    /**
     * Insert the given entry into the log.
     *
     * @param gsn         GSN of the given entry
     * @param client_id   ID of the client for this entry
     * @param data        pointer to the data for this entry
     * @param data_length size of the entry
     */
    void insert(gsn_t gsn, client_t client_id, uintptr_t data, uint32_t data_length);

private:

    /** reference to the log for this iterator */
    log& log_;

    /** iterator to the current block */
    blocks_t::iterator block_;

    /** index of the block in the log */
    uint32_t block_index_ = 0;

    /** latest value of the number of blocks read */
    uint32_t current_blocks_ = 0;

};

/**
 * This class is for iterating the log sequentially.
 */
class iterator {

public:

    /**
     * Initialize the iterator.
     *
     * @param log reference to the global log
     */
    iterator(log& log);

    /**
     * Try to get the next entry from the log.
     *
     * @param optionally, an entry from the log
     */
    std::optional<entry_t> next_entry();

private:

    /** reference to the log for this iterator */
    log& log_;

    /** index into the log */
    uint64_t entry_index_ = 0;

    /** iterator to the current block */
    blocks_t::iterator block_;

    /** iterator to the current counter */
    counters_t::iterator counter_;

    /** index of the block in the log */
    uint32_t block_index_ = 0;

    /** latest value of the number of blocks read */
    uint32_t current_blocks_ = 0;

    /** pointer to the current entry */
    block_t::iterator current_ = nullptr;

};

} // namespace zip::subscriber
