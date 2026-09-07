#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <list>

#include <zip/api/api.h>
#include <zip/network/buffer.h>
#include <zip/util/consts.h>

namespace zip::storage {

/**
 * This is the underlying of a single entry in the log.
 */
struct entry_t {

    /** GSN of the entry */
    gsn_t gsn;

    /** data for the log entry */
    uintptr_t data;

    /** pointer to the next entry for this client */
    std::atomic<entry_t*> next;

    /** size of the data for this entry */
    uint32_t data_length;

    /** ID of the client */
    client_t client_id;

    /** whether this entry has been received */
    std::atomic<bool> set;

};

/**
 * Type of a single block which includes
 * the log entries of this block.
 */
using block_t = std::array<entry_t, zip::consts::NUM_ENTRIES_PER_BLOCK>;

/**
 * Type of the linked list of blocks.
 */
using blocks_t = std::list<block_t*>;

/**
 * This class represents the log for this storage server.
 */
class log {

public:

    /**
     * Initialize the log.
     */
    log();

    /**
     * Deinitialize the log.
     */
    ~log();

    /**
     * Add entries to this log for the next epoch.
     *
     * @param epoch the details of the new epoch
     */
    void add_entries(zip::api::storage_new_epoch& epoch);

private:

    /** Make position and iterator friends of this class */
    friend class position;
    friend class iterator;

    /** blocks of this log */
    blocks_t blocks_;

    /** number of entries in the log */
    uint64_t current_entries_ = 0;

    /** location where the other threads read the size of the log  */
    std::atomic<uint64_t> num_entries_ = 0;

    /** map from client IDs to the first and last iterators*/
    std::array<std::pair<std::atomic<block_t::iterator>, block_t::iterator>, zip::consts::MAX_CLIENTS> first_last_ {};

    /** iterator to the last used entry in the log */
    block_t::iterator entry_;

    /** iterator to the current block to which entries are being added */
    blocks_t::iterator current_;

};

/**
 * This class represents a single client's position inside the log.
 */
class position {

public:

    /**
     * Initialize a position in the log.
     *
     * @param log       reference to the global log
     * @param client_id ID of the client for this position
     */
    position(log& log, client_t client_id);

    /**
     * Insert the given entry into the log.
     *
     * @param data        pointer to the data for this entry
     * @param data_length size of the entry
     * @param close       number of slots to close
     * 
     * @return GSN assigned to this entry
     */
    gsn_t insert(void* data, uint32_t data_length, uint32_t close, int fd);

    /**
     * Fill the log with given number of no-ops.
     *
     * @param close the number of slots to close
     */
    void fill(uint32_t close);

    /**
     * Close this client's slots in the log.
     */
    void close();

private:

    /** reference to the log for this iterator */
    log& log_;

    /** ID of the client for this position */
    client_t client_id_;

    /** pointer to the current entry in the log */
    block_t::iterator current_ = nullptr;

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
     * @param pointet to an entry from the log
     */
    entry_t* next_entry();

private:

    /** reference to the log for this iterator */
    log& log_;

    /** index into the log */
    uint64_t entry_index_ = 0;

    /** latest value of number of entries read */
    uint64_t current_entries_ = 0;

    /** iterator to the current block */
    blocks_t::iterator block_;

    /** index of the block in the log */
    uint32_t block_index_ = 0;

    /** iterator to the current entry */
    block_t::iterator current_ = nullptr;

};

} // namespace zip::storage
