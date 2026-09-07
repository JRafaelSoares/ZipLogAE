#include <cstdlib>
#include <zip/storage/log.h>

#include <chrono>
#include <cstring>
#include <limits>
#include <vector>

#include <sys/mman.h>

#include <zip/api/api.h>
#include <zip/storage/storage.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/util.h>

namespace zip::storage {

/** create a logger for this file */
static zip::util::logger logger("log");

log::log() {
    // initialize the log with an empty block
    auto ptr = static_cast<block_t*>(std::aligned_alloc(alignof(block_t), sizeof(block_t)));
    std::memset(ptr, 0, sizeof(block_t));
    blocks_.emplace_back(ptr);

    // initialize the iterators
    current_ = blocks_.begin();
    entry_ = (*current_)->begin();
}

log::~log() {
    // deallocate memory for the blocks
    for (auto block: blocks_) {
        for (auto& entry: *block) {
            // deallocate the entry if it was allocated
            if (entry.data_length > sizeof(uintptr_t)) {
                std::free(reinterpret_cast<void*>(entry.data));
            }
        }
        std::free(block);
    }
}

void log::add_entries(zip::api::storage_new_epoch& epoch) {
    // if there are no clients in this epoch then return
    if (epoch.num_clients == 0) return;
    auto start = std::chrono::high_resolution_clock::now();

    // this lambda yields a slot for the
    // client at the given index
    auto yield = [&] (uint32_t i) {
        // yeild a slot for this client if this belongs
        // to this storage server's shard
        if (epoch.assignments[i].client_id != std::numeric_limits<client_t>::max()) {
            // if we have reached the end of the block then
            // obtain a new block and advance the iterators
            if (entry_ == (*current_)->end()) {
                auto ptr = static_cast<block_t*>(std::aligned_alloc(alignof(block_t), sizeof(block_t)));
                std::memset(ptr, 0, sizeof(block_t));
                blocks_.emplace_back(ptr);
                current_++;
                entry_ = (*current_)->begin();
            }

            // set the entry's parameters
            auto& entry = *entry_++;
            entry.gsn = epoch.start_gsn;

            // update the first and last entries for this client
            auto& [first, last] = first_last_[epoch.assignments[i].client_id];
            auto& location = last == nullptr ? first : last->next;
            location.store(&entry, std::memory_order_release);
            last = &entry;

            // update the number of entries
            current_entries_++;
        }
        epoch.start_gsn++;
    };

    // create the errors for the iteration
    auto error = epoch.assignments[0].num_slots / epoch.num_clients;
    std::vector<long long> errors(epoch.num_clients - 1, error);

    // generate the slots
    for (uint32_t i = 0; i < epoch.assignments[0].num_slots; i++) {
        // yield a slot for the primary clients
        yield(0);

        // update the errors for secondary clients and yield if needed
        for (uint32_t j = 1; j < epoch.num_clients; j++) {
            if ((errors[j - 1] -= epoch.assignments[j].num_slots) < 0)  {
                errors[j - 1] += epoch.assignments[0].num_slots;
                yield(j);
            }
        }
    }

    // update the total number of entries
    num_entries_.store(current_entries_, std::memory_order_release);

    // send a warning if we did not finish in the deadline
    auto deadline = zip::util::time_from_ns(epoch.begin);
    if (auto now = std::chrono::high_resolution_clock::now(); now > deadline) {
        auto total = zip::util::time_in_us(now - start), late = zip::util::time_in_us(now - deadline);
        logger.error("Missed the deadline for allocating slots for this epoch by ", late, " us, took ", total, " us");
    }
}

position::position(log& log, client_t client_id): log_(log), client_id_(client_id) {}

gsn_t position::insert(void* data, uint32_t data_length, uint32_t close, int fd) {
    // seek to the first or next entry for this client
    auto& first = log_.first_last_[client_id_].first;
    auto& location = current_ == nullptr ? first : current_->next;
    while ((current_ = location.load(std::memory_order_acquire)) == nullptr) zip::util::relax();

    // close the required number of slots for this request
    while (--close) {
        current_->set.store(true, std::memory_order_relaxed);
        auto& location = current_->next;
        while ((current_ = location.load(std::memory_order_acquire)) == nullptr) zip::util::relax();
    }

    // set the attributes of the entry
    if (data_length > sizeof(uintptr_t)) {
        auto temp = std::malloc(data_length);
        std::memcpy(temp, data, data_length);
        current_->data = reinterpret_cast<uintptr_t>(temp);
    } else if (data_length > 0) {
        current_->data = *reinterpret_cast<uintptr_t*>(data);
    }
    current_->data_length = data_length;
    current_->set.store(true, std::memory_order_release);

    /*
    if (write(fd, data, data_length) != data_length) {
        logger.info("Writing less bytes than expected");
    }
    */

    // return the GSN for this entry
    return current_->gsn;
}

void position::fill(uint32_t close) {
    // seek to the first or next entry for this client
    auto& first = log_.first_last_[client_id_].first;
    auto& location = current_ == nullptr ? first : current_->next;
    while ((current_ = location.load(std::memory_order_acquire)) == nullptr) zip::util::relax();

    // close the required number of slots for this request
    while (--close) {
        current_->set.store(true, std::memory_order_relaxed);
        auto& location = current_->next;
        while ((current_ = location.load(std::memory_order_acquire)) == nullptr) zip::util::relax();
    }

    // set the entry as a no-op
    current_->set.store(true, std::memory_order_relaxed);
}

void position::close() {
    // seek to the first or next entry for this client
    auto& first = log_.first_last_[client_id_].first;
    if (current_ == nullptr && (current_ = first.load(std::memory_order_acquire)) != nullptr) {
        current_->set.store(true, std::memory_order_relaxed);
    }

    // if this client has any entries, then close them all
    if (current_ != nullptr) {
        while (auto next = current_->next.load(std::memory_order_acquire)) {
            next->set.store(true, std::memory_order_relaxed);
            current_ = next;
        }
    }
}

iterator::iterator(log& log): log_(log), block_(log_.blocks_.begin()) {}

entry_t* iterator::next_entry() {
    // if we don't have an entry under consideration
    // obtain the next one from the log
    if (current_ == nullptr) {
        // if there are no entries available then return
        if (entry_index_ >= current_entries_ && entry_index_ >=
                (current_entries_ = log_.num_entries_.load(std::memory_order_acquire))) {
            return nullptr;
        }

        // advance the block iterator, if needed
        auto block = entry_index_ / zip::consts::NUM_ENTRIES_PER_BLOCK;
        while (block > block_index_) { block_index_++; block_++; }

        // obtain the current entry
        auto index = entry_index_++ % zip::consts::NUM_ENTRIES_PER_BLOCK;
        current_ = &(*block_)->operator[](index);
    }

    // if the entry hasn't been received yet then return
    if (!current_->set.load(std::memory_order_acquire)) return nullptr;

    // save the current entry and reset the pointer
    auto result = current_;
    current_ = nullptr;
    return result;
}

} // namespace zip::storage
