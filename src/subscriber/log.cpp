#include "subscriber/log.h"

#include <cstdlib>
#include <cstring>
#include <vector>

#include "api/api.h"
#include "util/log.h"
#include "util/util.h"

namespace zip::subscriber {

/** create a logger for this file */
static zip::util::logger logger("log");

log::log(unsigned long num_iterators): num_iterators_(num_iterators) {
    // initialize the log with two blocks
    blocks_.emplace_back(static_cast<block*>(zip::util::allocate_huge_page(sizeof(block))));
    blocks_.emplace_back(static_cast<block*>(zip::util::allocate_huge_page(sizeof(block))));

    // initialize the counters
    counters_.emplace_back(0);
    counters_.emplace_back(0);

    // initialise the iterators
    block_ = blocks_.begin();
    counter_ = counters_.begin();

    // set the number of blocks
    num_blocks_.store(2, std::memory_order_release);
}

log::~log() {
    // free the memory for the blocks
    for (auto& block: blocks_) {
        if (block != nullptr) std::free(block);
    }
}

void log::recycle_blocks() {
    // if all iterators have gone over first block,
    // then recycle it to the back of the log
    if (counter_->load(std::memory_order_relaxed) == num_iterators_) {
        // zero-out the block and put it to the back
        std::memset(*block_, 0, sizeof(block));
        blocks_.emplace_back(*block_);
        counters_.emplace_back(0);
        *block_ = nullptr;

        // update the state
        num_blocks_.fetch_add(1, std::memory_order_release);
        counter_++;
        block_++;
    }
}

position::position(log& log):
num_blocks_(log.num_blocks_), iterator_(log.blocks_.begin()) {}

void position::insert(zip::api::subscriber_log_entry* entry) {
    // get the block index for this entry
    auto block = entry->gsn / zip::consts::NUM_ENTRIES_PER_BLOCK;

    // check for if we have reached the end of blocks
    if (block >= current_blocks_) {
        while (block >= (current_blocks_ = num_blocks_.load(std::memory_order_acquire))) zip::util::relax();
    }

    // advance the block iterator, if needed
    while (block > block_index_) { block_index_++; iterator_++; }

    // get the location where we need to store this entry
    auto index = entry->gsn % zip::consts::NUM_ENTRIES_PER_BLOCK;
    auto& location = (*iterator_)->operator[](index);
    location.store(entry, std::memory_order_release);
}

iterator::iterator(log& log, unsigned long index, unsigned long stride):
entry_index_(index), stride_(stride), num_blocks_(log.num_blocks_),
block_(log.blocks_.begin()), counter_(log.counters_.begin())  {}

zip::api::subscriber_log_entry* iterator::next_entry() {
    // if we don't have an entry under consideration
    // obtain the next one from the log
    if (entry_ == nullptr) {
        // get the block index for this entry
        auto block = entry_index_ / zip::consts::NUM_ENTRIES_PER_BLOCK;

        // if there are no blocks available then return
        if (block >= current_blocks_ && block >=
                (current_blocks_ = num_blocks_.load(std::memory_order_acquire))) {
            return nullptr;
        }

        // advance the block iterator, if needed
        while (block > block_index_) {
            block_index_++; block_++; auto counter = counter_++;
            counter->fetch_add(1, std::memory_order_release);
        }

        // obtain the current entry, and advance to the next
        auto index = entry_index_ % zip::consts::NUM_ENTRIES_PER_BLOCK;
        entry_ = &(*block_)->operator[](index);
        entry_index_ += stride_;
    }

    // check the status of the entry and return it
    auto ptr = entry_->load(std::memory_order_acquire);
    if (ptr != nullptr) {
        entry_ = nullptr;
        return ptr;
    }

    // otherwise return empty
    return nullptr;
}

sequential_iterator::sequential_iterator(log& log, unsigned long index, unsigned long stride):
iterator(log, index, stride), index_(index) { entry_index_ = 0; }

zip::api::subscriber_log_entry* sequential_iterator::next_entry() {
    // if we don't have an entry under consideration
    // obtain the next one from the log
    if (entry_ == nullptr) {
        // get the block index for this entry
        auto block = entry_index_ / zip::consts::NUM_ENTRIES_PER_BLOCK;

        // if there are no blocks available then return
        if (block >= current_blocks_ && block >=
                (current_blocks_ = num_blocks_.load(std::memory_order_acquire))) {
            return nullptr;
        }

        // advance the block iterator, if needed
        while (block > block_index_) {
            block_index_++; block_++; auto counter = counter_++;
            counter->fetch_add(1, std::memory_order_release);
        }

        // obtain the current entry, and advance to the next
        auto index = entry_index_++ % zip::consts::NUM_ENTRIES_PER_BLOCK;
        entry_ = &(*block_)->operator[](index);
    }

    // check the status of the entry and return it,
    // only if we are allowed to process it
    auto ptr = entry_->load(std::memory_order_acquire);
    if (ptr != nullptr) {
        auto result = ((entry_index_ - 1) % stride_ == index_) ? ptr : nullptr;
        entry_ = nullptr;
        return result;
    }

    // otherwise return empty
    return nullptr;
}

} // namespace zip::subscriber
