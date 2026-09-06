#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <list>
#include <utility>
#include <vector>

#include "util/consts.h"

namespace zip {

/** Forward declaration for some messages. */
namespace api { struct storage_slots; }

namespace storage {

/**
 * This class represents the log for
 * this storage server.
 */
class log {

public:

    /**
     * This is the type of a single entry
     * in the log.
     */
    struct entry {

        /** client ID the entry has been assigned to */
        uint64_t client_id;

        /** GSN of the entry */
        uint64_t gsn;

        /** length of the data in this entry */
        unsigned long data_length;

        /**
         * pointer to the data
         *
         * NOTE: if the data fits in a `uint64_t`, it is just put
         *       into this field
         * */
        uint64_t data;

        /** whether this entry has been set */
        std::atomic<bool> set;

        /** pointer to the next entry for this client */
        std::atomic<entry*> next;

        /** pointer to the previous entry for this client */
        entry* prev;

    };

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
     * Initialize the log.
     */
    log();

    /**
     * Deinitialize the log.
     */
    ~log();

    /**
     * Add a new entries to this log.
     *
     * @param slots the details of the GSNs to add
     */
    void add_entries(zip::api::storage_slots& slots);

private:

    /** Make position and iterator friends of this class */
    friend class position;
    friend class iterator;

    /** blocks of this log */
    blocks blocks_;

    /** number of entries in the log */
    unsigned long current_entries_ = 0;

    /** location where the other threads read the size of the log  */
    std::atomic<unsigned long> num_entries_ = 0;

    /** store the location of each client's first and last entry */
    std::array<std::pair<std::atomic<entry*>, entry*>, zip::consts::MAX_CLIENTS> client_entries_ {};

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
     * Initialize a position in the log.
     *
     * @param client_id ID of the client
     * @param log       reference to the global log
     */
    position(uint64_t client_id, log& log);

    /**
     * Insert the given request into the log.
     *
     * @param data   the client data
     * @param length the length of the data
     * @param close  the number of slots to close
     *
     * @return GSN of the log entry
     */
    uint64_t insert(void* data, unsigned long length, unsigned long close);

    /**
     * Patch the given entry into the log.
     *
     * @param data   the client data
     * @param length the length of the data
     * @param gsn    the GSN of the entry
     */
    void patch(void* data, unsigned long length, uint64_t gsn);

    /**
     * Return a suffix of this client's entries beggining
     * at the given GSN.
     *
     * @param begin_gsn the starting GSN
     *
     * @return a vector with all the entries
     */
    std::vector<log::entry*> suffix(uint64_t begin_gsn);

    /**
     * Close this client's slots in the log.
     */
    void close();

private:

    /** pointer to the current entry in the log */
    log::entry* current_entry_ = nullptr;

    /** location of the pointer to the first entry */
    std::atomic<log::entry*>& first_entry_;

};

/**
 * This class represents the position of the subscriber
 * in the log.
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
    log::entry* next_entry();

private:

    /** index into the log */
    unsigned long entry_index_;

    /** stride with which to iterate the log */
    unsigned long stride_;

    /** number of entries in the global log */
    std::atomic<unsigned long>& num_entries_;

    /** latest value of number of entries read */
    unsigned long current_entries_ = 0;

    /** iterator to the current block */
    log::blocks::iterator iterator_;

    /** index of the block in the log */
    unsigned long block_index_ = 0;

    /** iterator to the current entry */
    log::entry* entry_ = nullptr;

};

} // namespace storage

} // namespace zip
