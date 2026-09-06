#pragma once

#include <array>
#include <atomic>
#include <list>

#include "util/consts.h"

namespace zip {

/** Forward declaration for some messages. */
namespace api { struct subscriber_log_entry; }

namespace subscriber {

/**
 * This class represents the log for a subscriber.
 */
class log {

public:

    /**
     * This is the type of a single entry
     * in the log.
     */
    using entry = std::atomic<zip::api::subscriber_log_entry*>;

    /**
     * Type of a single block which includes
     * the log entries of this block.
     */
    using block = std::array<entry, zip::consts::NUM_ENTRIES_PER_BLOCK>;
    static_assert(sizeof(block) % zip::consts::HUGE_PAGE_SIZE == 0);

    /**
     * Type of the linked list of blocks.
     */
    using blocks = std::list<block*>;

    /**
     * Type of the linked list of counters.
     */
    using counters = std::list<std::atomic<unsigned long>>;

    /**
     * Initialize the log.
     *
     * @param num_iterators the number of iterators for this log
     */
    log(unsigned long num_iterators);

    /**
     * Deinitialize the log.
     */
    ~log();

    /**
     * Recycle consumed blocks for this log.
     */
    void recycle_blocks();

private:

    /** Make position and iterator friends of this class */
    friend class position;
    friend class iterator;

    /** blocks of this log */
    blocks blocks_;

    /** counters for the blocks of this log */
    counters counters_;

    /** iterator to the current block */
    log::blocks::iterator block_;

    /** iterator to the current counter */
    log::counters::iterator counter_;

    /** number of blocks in the log  */
    std::atomic<unsigned long> num_blocks_ = 0;

    /** number of iterators for this log */
    unsigned long num_iterators_;

};

/**
 * This class represents a monotonically increasing
 * position in the log.
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
     * Insert the given log entry into the log.
     *
     * @param entry the entry to insert
     */
    void insert(zip::api::subscriber_log_entry* entry);

private:

    /** latest value of number of blocks read */
    unsigned long current_blocks_ = 0;

    /** number of blocks in the global log */
    std::atomic<unsigned long>& num_blocks_;

    /** iterator to the current block */
    log::blocks::iterator iterator_;

    /** index of the block in the log */
    unsigned long block_index_ = 0;

};

/**
 * This class represents the position of an unordered
 * subscriber in the log.
 */
class iterator {

public:

    /**
     * Initialize the iterator.
     *
     * @param log    reference to the global log
     * @param index  index of this iterator in the stride
     * @param stride stride with which to iterate the log
     */
    iterator(log& log, unsigned long index, unsigned long stride);

    /**
     * Try to get the next entry from the log.
     *
     * @return pointer to the entry, if one was
     *         obtained, nullptr otherwise
     */
    zip::api::subscriber_log_entry* next_entry();

protected:

    /** index into the log */
    unsigned long entry_index_;

    /** stride with which to iterate the log */
    unsigned long stride_;

    /** number of blocks in the global log */
    std::atomic<unsigned long>& num_blocks_;

    /** latest value of number of blocks read */
    unsigned long current_blocks_;

    /** iterator to the current block */
    log::blocks::iterator block_;

    /** iterator to the current counter */
    log::counters::iterator counter_;

    /** index of the block in the log */
    unsigned long block_index_ = 0;

    /** iterator to the current entry */
    log::entry* entry_ = nullptr;

};

/**
 * This class represents the position of a sequential
 * subscriber in the log.
 */
class sequential_iterator: public iterator {

public:

    /**
     * Initialize the sequential iterator.
     *
     * @param log    reference to the global log
     * @param index  index of this iterator in the stride
     * @param stride stride with which to iterate the log
     */
    sequential_iterator(log& log, unsigned long index, unsigned long stride);

    /**
     * Try to get the next entry from the log sequentially.
     *
     * @return pointer to the entry, if one was
     *         obtained, nullptr otherwise
     */
    zip::api::subscriber_log_entry* next_entry();

private:

    /** index of this iterator within the stride */
    unsigned long index_;

};

} // namespace subscriber

} // namespace zip
