#include "storage/log.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <string>

#include "api/api.h"
#include "util/log.h"

namespace zip {
namespace storage {

/** create a logger for this file */
static zip::util::logger logger("log");

log::log() {
    // initialise the log with two blocks
    blocks_.emplace_back(static_cast<block*>(zip::util::allocate_huge_page(sizeof(block))));

    // initialise the iterators
    block_ = blocks_.begin();
    entry_ = (*block_)->begin();
}

log::~log() {
    // iterate over each block and each entry
    // and deallocate memory for the buffers
    for (auto& block: blocks_) {
#ifndef COLOCATED_ZIPKAT
        for (auto& entry: *block) {
            if (entry.data != nullptr) {
                std::free(entry.data);
            }
        }
#endif

        // deallocate the whole block
        std::free(block);
    }
}

void log::recurse_generate(zip::api::storage_order_slots& slots, unsigned long index) {
    // get the details for this client
    auto num_clients = slots.num_clients;
    auto& assignments = slots.assignments;
    auto num_iters = assignments[index].num_slots / (index == 0 ? 1 : assignments[index - 1].num_slots);

    // find the number of clients that have the same
    // slots as this one
    unsigned long last = index + 1;
    unsigned long offset = 1;
    for (; last < num_clients; last++) {
        if (assignments[index].num_slots != assignments[last].num_slots) break;
        if (assignments[last].client_id != -1) offset++;
    }

    // perform a given number of iterations for this client
    for (unsigned long i = 0; i < num_iters; i++) {
        // if the following clients have the same number of slots
        // then we can iterate instead of recursing since we will
        // only be adding the same number of slots alternatively
        for (auto c = index; c < last; c++) {
            if (assignments[c].client_id != -1) {
                // if we have reached the end of the block
                // then add a new block and advance the iterators
                if (entry_ == (*block_)->end()) {
                    blocks_.emplace_back(static_cast<block*>(zip::util::allocate_huge_page(sizeof(block))));
                    block_++; entry_ = (*block_)->begin();
                }

                // set the entry's parameters
                auto& entry = *entry_++;
                entry.offset = (i == (num_iters - 1)) ? 1 : offset;
                entry.client_id = assignments[c].client_id;
#if ZIP_CLIENT_SEQ
                entry.client_seq = assignments[c].client_seq++;
#endif
#if ZIP_SHARD_SEQ
                entry.shard_seq = slots.shard_seq++;
#endif
                entry.gsn = slots.start_gsn;
#ifdef COLOCATED_ZIPKAT
                entry.status = log::entry::kUnhandled;
#endif
                //logger.info("create entry gsn=", entry.gsn, ", of client ", entry.client_id);
            }
            slots.start_gsn++;
        }

        // move on to the next client
        if (last < num_clients) {
            recurse_generate(slots, last);
        }
    }
}

void log::add_entries(zip::api::storage_order_slots& slots) {
    // calculate the number of new slots to be added
    for (unsigned long i = 0; i < slots.num_clients; i++) {
        if (slots.assignments[i].client_id != -1)
            current_entries_ += slots.assignments[i].num_slots;
    }

    // recursively generate entries to add
    recurse_generate(slots, 0);

    // update the total number of entries
    num_entries_.store(current_entries_, std::memory_order_release);
}

position::position(uint64_t client_id, log& log):
/*
client_id_(client_id), num_entries_(log.num_entries_), iterator_(log.blocks_.begin()), hist_count(0) {
    hdr_init(1, 10000, 3, &hist);
}
*/
client_id_(client_id), num_entries_(log.num_entries_), iterator_(log.blocks_.begin()) {}

void position::open() {
    // get the number of entries in the log
    auto num_entries = num_entries_.load(std::memory_order_acquire);
    if (num_entries > 0) {
        // if we have some entries then go to
        // the end of the log
        entry_index_ = num_entries - 1;

        // also set the block iterator to the very end
        auto block = (num_entries - 1) / zip::consts::NUM_ENTRIES_PER_BLOCK;
        while (block_index_ < block) { block_index_++; iterator_++; }
    }

    // mark the log as open
    open_.store(true, std::memory_order_release);
}

void position::close() {
    // close all the slots of this client
    auto num_entries = num_entries_.load(std::memory_order_acquire);
    while (entry_index_ < num_entries) {
        // find the entry and block index of the current index
        auto block = entry_index_ / zip::consts::NUM_ENTRIES_PER_BLOCK;
        auto index = entry_index_ % zip::consts::NUM_ENTRIES_PER_BLOCK;

        // advance the block iterator, if needed
        if (block > block_index_) { block_index_++; iterator_++; }

        // skip this entry if it's not for the client
        auto& entry = (*iterator_)->operator[](index);
        if (entry.client_id != client_id_) { entry_index_++; continue; }

        // otherwise mark the index as a no-op
        entry.set.store(true, std::memory_order_relaxed);
        entry_index_ += entry.offset;
    }
}

#ifndef SUBSCRIBER_SEND_IA_ACK
log::entry& position::append_after(zip::api::storage_insert_after& req, zip::api::client_insert_ack& ack) {
#else
void position::append_after(zip::api::storage_insert_after& req, zip::api::client_insert_ack& ack) {
#endif
    // number of slots closed for this request
    ack.closed = 0;

    // allocate and copy the data to a buffer
#ifndef COLOCATED_ZIPKAT
    uint8_t* alloc = nullptr;
    if (req.data_length > 0) {
        alloc = static_cast<uint8_t*>(std::malloc(req.data_length));
        std::memcpy(alloc, req.data, req.data_length);
    }
#endif

    // wait for the log to be opened
    while (!open_.load(std::memory_order_acquire));

    // iterate on each log entry one by one
    // until we satisfy all the requirements
    while (true) {
        // check for if we have reached the end of entries
        while (entry_index_ >= current_entries_) {
            current_entries_ = num_entries_.load(std::memory_order_acquire);
        }

        // check if we have reached the end of blocks and we need
        // to change to the next block
        auto block = entry_index_ / zip::consts::NUM_ENTRIES_PER_BLOCK;
        if (block > block_index_) { block_index_++; iterator_++; }

        // get the index and the current entry
        // and increment the entry index to the next
        auto index = entry_index_ % zip::consts::NUM_ENTRIES_PER_BLOCK;
        auto& entry = (*iterator_)->operator[](index);

        // if this entry does not belong to this client then continue
        if (entry.client_id != client_id_) { entry_index_++; continue; }

        // increment the number of slots closed
        entry_index_ += entry.offset;
        ack.closed++;

        // if this entry does not satisfy the `` and `` requirements
        // then set this entry and move on
        if (   (req.gsn_after != -1 && entry.gsn < req.gsn_after)
            || (req.num_slots != -1 && ack.closed < req.num_slots)) {
            entry.set.store(true, std::memory_order_relaxed);
            continue;
        }

        // set the attributes of the entry
        entry.data_length = req.data_length;
#ifdef COLOCATED_ZIPKAT
        entry.closed = ack.closed;
        // for noop, set the locked to be true
        if (entry.data_length == 0) {
            entry.locked.store(true, std::memory_order_relaxed);
        } else {
            auto commit_req = reinterpret_cast<zip::api::zipkat_commit_request*>(req.data);
            auto txn = std::make_unique<Transaction>(commit_req->nr_reads, commit_req->nr_writes, (char*)commit_req->data);;
            ZIP_ASSERT(txn->serializedSize() == commit_req->data_length, txn->serializedSize(), ", ", commit_req->data_length); 
            entry.txn = std::move(txn);
        }
#else
        entry.data = alloc;
#endif
#if defined(ZIP_MEASURE) || defined(MEASURE_LOG_ITERATE)
        //entry.start = std::chrono::high_resolution_clock::now();
#endif
        entry.set.store(true, std::memory_order_release);

        // set the value for the ack and return
        logger.trace("Assigning GSN ", entry.gsn, " to request of client (", client_id_, ")");
#if ZIP_CLIENT_SEQ
        ack.client_seq = entry.client_seq;
#endif
#if ZIP_SHARD_SEQ
        ack.shard_seq = entry.shard_seq;
#endif
        ack.gsn = entry.gsn;
        ack.global_client_id = req.global_client_id;
        entry.global_client_id = req.global_client_id;

#ifndef SUBSCRIBER_SEND_IA_ACK
        return entry;
#else
        return;
#endif
    }
}

iterator::iterator(log& log, uint64_t start_gsn): num_entries_(log.num_entries_), iterator_(log.blocks_.begin()) {
    // iterate the log to find the first entry that is greater
    // or equal to the given starting GSN
    current_entries_ = num_entries_.load(std::memory_order_acquire);
    auto max_block_index = (current_entries_ - 1) / zip::consts::NUM_ENTRIES_PER_BLOCK;
    auto max_index = (current_entries_ - 1) % zip::consts::NUM_ENTRIES_PER_BLOCK;

    // if we have no entries in the log then return
    if (current_entries_ == 0) return;

    // otherwise, check the very first entry in the log
    auto& first = (*iterator_)->operator[](0);
    if (first.gsn >= start_gsn) return;

    // lastly, find the best position to start
    while (block_index_ < max_block_index) {
        // check if the best position lies somewhere in the current block
        auto& current = (*iterator_)->operator[](0);
        auto& next = std::next(*iterator_)->operator[](0);
        if (current.gsn < start_gsn && next.gsn >= start_gsn) break;

        // otherwise advance to the next block
        entry_index_ += zip::consts::NUM_ENTRIES_PER_BLOCK;
        block_index_++;
        iterator_++;
    }

    // iterate to the best position in the log
    auto pred =  [] (auto& element, auto& value) { return element.gsn < value; };
    auto size = block_index_ == max_block_index ? max_index + 1 : zip::consts::NUM_ENTRIES_PER_BLOCK;
    auto it = std::lower_bound((*iterator_)->begin(), (*iterator_)->begin() + size, start_gsn, pred);
    entry_index_ = block_index_ * zip::consts::NUM_ENTRIES_PER_BLOCK + std::distance((*iterator_)->begin(), it);
}

bool iterator::next_entry(log::entry*& entry) {
    // if we don't have an entry under consideration
    // obtain the next one from the log
    if (entry_ == nullptr) {
        // if there are no entries available then return
        if (entry_index_ >= current_entries_ && entry_index_ >=
                (current_entries_ = num_entries_.load(std::memory_order_acquire))) {
            return false;
        }

        // iterate to the next block if we have to
        auto block = entry_index_ / zip::consts::NUM_ENTRIES_PER_BLOCK;
        if (block > block_index_) { block_index_++; iterator_++; }

        // obtain the current entry
        auto index = entry_index_++ % zip::consts::NUM_ENTRIES_PER_BLOCK;
        entry_ = &(*iterator_)->operator[](index);
    }

    // check the status of the entry and return it
    if (entry_->set.load(std::memory_order_acquire)) {
        entry = entry_;
        entry_ = nullptr;
        return true;
    }

    // otherwise return false
    return false;
}

} // namespace storage
} // namespace zip
