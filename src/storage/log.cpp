#include "storage/log.h"

#include <chrono>
#include <compare>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <vector>

#include "api/api.h"
#include "util/log.h"
#include "util/util.h"

namespace zip::storage {

/** create a logger for this file */
static zip::util::logger logger("log");

log::log() {
    // initialize the log with two blocks
    blocks_.emplace_back(static_cast<block*>(zip::util::allocate_huge_page(sizeof(block))));

    // initialize the iterators
    block_ = blocks_.begin();
    entry_ = (*block_)->begin();
}

log::~log() {
    // iterate over each block and each entry
    // and deallocate memory for the buffers
    for (auto& block: blocks_) {
        for (auto& entry: *block) {
            if (entry.data_length > sizeof(uint64_t)) {
                std::free(reinterpret_cast<void*>(entry.data));
            }
        }

        // deallocate the whole block
        std::free(block);
    }
}

void log::add_entries(zip::api::storage_slots& slots) {
    // we need to ensure that we don't exceed the time for this operation
    auto start = std::chrono::high_resolution_clock::now();

    // this lambda yields a slot for the
    // client at the given index
    auto yield = [&] (unsigned long i) {
        if (slots.assignments[i].client_id != std::numeric_limits<uint64_t>::max()) {
            // if we have reached the end of the block then
            // allocate a new block and advance the iterators
            if (entry_ == (*block_)->end()) {
                blocks_.emplace_back(static_cast<block*>(zip::util::allocate_huge_page(sizeof(block))));
                block_++; entry_ = (*block_)->begin();
            }

            // set the entry's parameters
            auto& entry = *entry_++;
            auto client_id = slots.assignments[i].client_id;
            entry.gsn = slots.start_gsn;
            entry.client_id = client_id;

            // update the first and last entries for this client
            auto& [first, last] = client_entries_[client_id];
            if (last != nullptr) [[likely]] {
                entry.prev = last;
                last->next.store(&entry, std::memory_order_release);
            } else {
                first.store(&entry, std::memory_order_release);
            }
            last = &entry;
            current_entries_++;
        }
        slots.start_gsn++;
    };

    // create the errors for the iteration
    auto error = slots.assignments[0].num_slots / slots.num_clients;
    std::vector<long long> errors(slots.num_clients - 1, error);

    // generate the slots
    for (unsigned long i = 0; i < slots.assignments[0].num_slots; i++) {
        // yield a slot for the primary client
        yield(0);

        // update the errors for secondary client and yield if needed
        for (unsigned long j = 1; j < slots.num_clients; j++) {
            if ((errors[j - 1] -= slots.assignments[j].num_slots) < 0)  {
                errors[j - 1] += slots.assignments[0].num_slots;
                yield(j);
            }
        }
    }

    // update the total number of entries
    num_entries_.store(current_entries_, std::memory_order_release);

    // send a warning if we did not finish in the deadline
    if ((std::chrono::high_resolution_clock::now() - start) > zip::consts::EPOCH_DURATION) {
        logger.error("Missed the deadline for allocating slots for this epoch");
    }

    logger.debug("Done adding slots, current_entries_=", current_entries_, ", num_client=", slots.num_clients);
}

position::position(uint64_t client_id, log& log):
first_entry_(log.client_entries_[client_id].first) { }

namespace detail {

/**
 * Return a copy of the data or the data itself
 * if it fits in a `uint64_t`.
 *
 * @param data   the data to copy
 * @param length the length of the data
 *
 * @returns a pointer or the data
 */
uint64_t alloc_copy(void* data, unsigned long length) {
    if (length > sizeof(uint64_t)) {
        auto ptr = std::malloc(length);
        std::memcpy(ptr, data, length);
        return reinterpret_cast<uint64_t>(ptr);
    } else if (length > 0) {
        return *reinterpret_cast<uint64_t*>(data);
    } else {
        return 0;
    }
}

} // namespace detail

uint64_t position::insert(void* data, unsigned long length, unsigned long close) {
    // if the current entry is null, then
    // we must seek to the first entry of this client
    auto prev_gsn = current_entry_ ? current_entry_->gsn : -1;
    int times = 0;
    if (current_entry_ == nullptr) {
        current_entry_ = first_entry_.load(std::memory_order_acquire);
    } else {
#if 1
        current_entry_ = current_entry_->next.load(std::memory_order_acquire);
#else
        log::entry* tmp = nullptr;
        do {
            tmp = current_entry_->next.load(std::memory_order_acquire);
            if (tmp) {
                prev_gsn = tmp->gsn;
                current_entry_ = tmp;
            }
        } while (!tmp);
#endif
    }
    ZIP_ASSERT_NOT_NULL(current_entry_, "could not find an entry belonging to this client, prev_gsn=", prev_gsn);
    logger.debug("In insert, current_entry_->gsn=", current_entry_->gsn, ", close=", close);

    // make sure we close the required number of
    // slots for this request
    while (--close) {
        current_entry_->set.store(true, std::memory_order_relaxed);
#if 1
        current_entry_ = current_entry_->next.load(std::memory_order_acquire);
        ZIP_ASSERT_NOT_NULL(current_entry_, "could not find an entry belonging to this client");
#else
        log::entry* tmp = nullptr;
        do {
            tmp = current_entry_->next.load(std::memory_order_acquire);
            if (tmp) {
                prev_gsn = tmp->gsn;
                current_entry_ = tmp;
            }
        } while (!tmp);
#endif
    }

    // set the attributes of the entry
    current_entry_->data = detail::alloc_copy(data, length);
    current_entry_->data_length = length;
    current_entry_->set.store(true, std::memory_order_release);
    logger.debug("In insert, return gsn=", current_entry_->gsn);
    return current_entry_->gsn;
}

void position::patch(void* data, unsigned long length, uint64_t gsn) {
    // if the current entry is null, then
    // we must seek to the first entry of this client
    if (current_entry_ == nullptr) {
        current_entry_ = first_entry_.load(std::memory_order_acquire);
    } else {
        current_entry_ = current_entry_->next.load(std::memory_order_acquire);
    }
    ZIP_ASSERT_NOT_NULL(current_entry_, "could not find an entry belonging to this client");

    // find the right entry for this GSN, closing
    // any slots that were not matched
    while (current_entry_->gsn < gsn) {
        current_entry_->set.store(true, std::memory_order_relaxed);
        current_entry_ = current_entry_->next.load(std::memory_order_acquire);
        ZIP_ASSERT_NOT_NULL(current_entry_, "could not find an entry belonging to this client");
    }

    // set the attributes of the entry
    ZIP_ASSERT_EQ(current_entry_->gsn, gsn, "could not find the entry with matching GSN");
    current_entry_->data = detail::alloc_copy(data, length);
    current_entry_->data_length = length;
    current_entry_->set.store(true, std::memory_order_release);
}

std::vector<log::entry*> position::suffix(uint64_t begin_gsn) {
    // if the current entry is null, then we must
    // have no requests for this client
    if (current_entry_ == nullptr) return {};

    // create a resulting vector
    std::vector<log::entry*> entries;

    // iterate backwards until we find all entries
    // with a greater GSN than we have been given
    for (auto entry = current_entry_;
         entry != nullptr && (begin_gsn == std::numeric_limits<uint64_t>::max() || entry->gsn > begin_gsn);
         entry = entry->prev
    ) {
        entries.emplace_back(entry);
    }

    // return the result
    return entries;
}

void position::close() {
    // if the current entry is null, then
    // we must seek to the first entry of this client
    auto first = first_entry_.load(std::memory_order_acquire);
    if (current_entry_ == nullptr && first != nullptr) {
        current_entry_ = first;
        current_entry_->set.store(true, std::memory_order_relaxed);
    }

    // if this client has any entries, then close them all
    if (current_entry_ != nullptr) {
        while (auto next = current_entry_->next.load(std::memory_order_acquire)) {
            next->set.store(true, std::memory_order_relaxed);
            current_entry_ = next;
        }
    }
}

iterator::iterator(log& log, unsigned long index, unsigned long stride):
entry_index_(index), stride_(stride), num_entries_(log.num_entries_),
iterator_(log.blocks_.begin()) {}

log::entry* iterator::next_entry() {
    // if we don't have an entry under consideration
    // obtain the next one from the log
    if (entry_ == nullptr) {
        // if there are no entries available then return
        if (entry_index_ >= current_entries_ && entry_index_ >=
                (current_entries_ = num_entries_.load(std::memory_order_acquire))) {
            return nullptr;
        }

        // advance the block iterator, if needed
        auto block = entry_index_ / zip::consts::NUM_ENTRIES_PER_BLOCK;
        while (block > block_index_) { block_index_++; iterator_++; }

        // obtain the current entry
        auto index = entry_index_ % zip::consts::NUM_ENTRIES_PER_BLOCK;
        entry_ = &(*iterator_)->operator[](index);
        entry_index_ += stride_;
    }

    // check the status of the entry and return it
    if (entry_->set.load(std::memory_order_acquire)) {
        auto entry = entry_;
        entry_ = nullptr;
        return entry;
    }

    // otherwise return empty
    return nullptr;
}

} // namespace zip::storage
