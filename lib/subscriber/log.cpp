#include <cstdlib>
#include <ranges>
#include <zip/subscriber/log.h>

#include <atomic>
#include <cstring>
#include <mutex>

#include <sys/mman.h>

#include <zip/api/api.h>
#include <zip/storage/storage.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/util.h>

namespace zip::subscriber {

/** create a logger for this file */
static zip::util::logger logger("log");

log::log(uint32_t num_iterators): num_iterators_(num_iterators) {
    // initialize the log with some empty blocks
    for (uint32_t i = 0; i < zip::consts::NUM_SUBSCRIBER_BLOCKS; i++) {
        auto ptr = static_cast<block_t*>(std::aligned_alloc(alignof(block_t), sizeof(block_t)));
        std::memset(ptr, 0, sizeof(block_t));
        blocks_.emplace_back(ptr);
        counters_.emplace_back(0);
    }

    // initialize the iterators
    block_ = blocks_.begin();
    counter_ = counters_.begin();

    // initialize the number of blocks
    num_blocks_.store(blocks_.size(), std::memory_order_release);
}

log::~log() {
    // deallocate memory for the blocks
    for (auto block: blocks_ | std::ranges::views::take(zip::consts::NUM_SUBSCRIBER_BLOCKS)) {
        for (auto& entry: *block) {
            // deallocate the entry if it was allocated
            if (entry.data_length > sizeof(uintptr_t)) {
                std::free(reinterpret_cast<void*>(entry.data));
            }
        }
        std::free(block);
    }
}

position::position(log& log): log_(log), block_(log_.blocks_.begin()) {}

void position::insert(gsn_t gsn, client_t client_id, uintptr_t data, uint32_t data_length) {
    // wait until there are enough blocks for this entry
    auto block = gsn / zip::consts::NUM_ENTRIES_PER_BLOCK;
    if (block >= current_blocks_) {
        while (block >= (current_blocks_ = log_.num_blocks_.load(std::memory_order_acquire))) zip::util::relax();
    }

    // advance the block iterator, if needed
    while (block > block_index_) { block_index_++; block_++; }

    // obtain the entry under consideration
    auto index = gsn % zip::consts::NUM_ENTRIES_PER_BLOCK;
    auto& entry = (*block_)->operator[](index);

    // set the attributes of the entry
    entry.gsn = gsn;
    entry.data = data;
    entry.client_id = client_id;
    entry.data_length = data_length;
    entry.set.store(true, std::memory_order_release);
}

iterator::iterator(log& log): log_(log), block_(log_.blocks_.begin()), counter_(log_.counters_.begin()) {}

std::optional<entry_t> iterator::next_entry() {
    // if we don't have an entry under consideration
    // obtain the next one from the log
    if (current_ == nullptr) {
        // if there are not enough blocks available then return
        auto block = entry_index_ / zip::consts::NUM_ENTRIES_PER_BLOCK;
        if (block >= current_blocks_ && block >=
                (current_blocks_ = log_.num_blocks_.load(std::memory_order_acquire))) {
            return {};
        }

        // advance the block iterator, if needed
        while (block > block_index_) { 
            // advance the iterators
            auto& counter = *(counter_++);
            auto block = *(block_++);
            block_index_++;

            // increment the counter for the block and recycle the block
            auto result = counter.fetch_add(1, std::memory_order_relaxed) + 1;
            if (result == log_.num_iterators_) {
                // acquire the lock and clear the contents of the block
                auto lock = std::unique_lock(log_.lock_);
                std::memset(block, 0, sizeof(block_t));               

                // add the block to the back of the list
                // and update the blocks counter
                log_.blocks_.emplace_back(block);
                log_.counters_.emplace_back(0);
                log_.num_blocks_.store(log_.blocks_.size(), std::memory_order_release);
            }
        }

        // obtain the current entry
        auto index = entry_index_++ % zip::consts::NUM_ENTRIES_PER_BLOCK;
        current_ = &(*block_)->operator[](index);
    }

    // if the entry hasn't been received yet then return
    if (!current_->set.load(std::memory_order_acquire)) return {};

    // save the current entry and reset the pointer
    auto result = entry_t(current_->gsn, current_->data, current_->data_length, current_->client_id);
    current_->data_length = 0;
    current_ = nullptr;
    return result;
}

} // namespace zip::subscriber
