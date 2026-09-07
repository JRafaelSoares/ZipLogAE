// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/store.cc:
 *   Key-value store with support for transactions using TAPIR.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *           2018 Adriana Szekeres <aaasz@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include <thread>

#include "store/meerkatstore/store.h"

namespace meerkatstore {

using namespace std;

// thread_local volatile int waste_cpu = 0;

int
Store::Get(const string &key, pair<Timestamp,string> &value)
{
    Debug("GET %s", key.c_str());

    // for (int i = 0; i < 1000; i++) { waste_cpu++; }
    // return REPLY_OK;

    if (store->Get(key, &value)) {
//    store->GetWithLock(key, &value);
        Debug("Value: \"%s\" at <%lu>",
              value.second.c_str(), value.first);
        return REPLY_OK;
    } else {
        Debug("Key \"%s\" not found.", key.c_str());
        return REPLY_FAIL;
    }
}

int
Store::Get(txnid_t txn_id, const string &key, pair<Timestamp,string> &value)
{
    Debug("GET %s", key.c_str());
    if (store->Get(key, &value)) {
//    store->GetWithLock(key, &value);
        Debug("Value: \"%s\" at <%lu>",
              value.second.c_str(), value.first);
        return REPLY_OK;
    } else {
        Debug("Key \"%s\" not found.", key.c_str());
        return REPLY_FAIL;
    }
}

int
Store::Get(txnid_t txn_id, const string &key, const Timestamp &timestamp, pair<Timestamp,string> &value)
{
    Panic("Gets at a particular timestamp are not supported by Meerkatstore.");
    return REPLY_FAIL;
}

void Store::clean_preparing_transaction(PreparingTransaction *p) {
    Timestamp current_timestamp;

    if (p) {
        for (auto node : p->readNodes) {
            const auto key = node.first;
            store->WriteLock(key, &current_timestamp);
            readers[node.first]->remove(node.second);
            store->WriteUnlock(key);
        }
        for (auto node : p->writeNodes) {
            const auto key = node.first;
            store->WriteLock(key, &current_timestamp);
            writers[node.first]->remove(node.second);
            store->WriteUnlock(key);
        }
        delete p;
    }
}

void Store::clean_transaction(txnid_t txn_id, const Transaction &txn) {

    PreparingTransaction p = {txn_id};
    PreparingTransaction* preparingTransaction = nullptr;
    Timestamp current_timestamp;

    if (!txn.getReadSet().empty()) {
        auto read_key = txn.getReadSet().begin()->first;
        store->WriteLock(read_key, &current_timestamp);
        auto node = readers[read_key]->find(&p);
        //TODO: grab a lock inside the node->key itself
        //      to protect against other threads processing this same transaction
        if (node != readers[read_key]->end()) {
            preparingTransaction = node->key;
        }
        store->WriteUnlock(read_key);
    } else {
        auto write_key = txn.getWriteSet().begin()->first;
        store->WriteLock(write_key, &current_timestamp);
        auto node = writers[write_key]->find(&p);
        //TODO: grab a lock inside the node->key itself
        //      to protect against other threads processing this same transaction
        if (node != writers[write_key]->end()) {
            preparingTransaction = node->key;
        }
        store->WriteUnlock(write_key);
    }

	clean_preparing_transaction(preparingTransaction);
}

int
Store::TryCommit(const Transaction &txn, const Timestamp &timestamp, std::atomic<bool>* entry_lock)
{
//    Debug("TryCommit at %lu", timestamp);
    // Acquire locks
#if 1
    for (const auto &key : txn.getAllKeys()) {
        // TODO: read lock?
        store->WriteLock(key);
    }
#endif

    // set locked flag
    entry_lock->store(true, std::memory_order_relaxed);

#if 0
    bool valid = true;
#else
    // OCC for ReadSet
    bool valid = true;
    for (const auto &read : txn.getReadSet()) {
        const string& key = read.first;
        const Timestamp& read_timestamp = read.second;
        const Timestamp current_timestamp = store->GetTimeStampWithLock(key);
        if (read_timestamp < current_timestamp) {
            valid = false;
/*
            Debug("[%lx] [MultitapirStore::Prepare] "
                  " Read check failed due to modified read key %s"
                  " ts last wrote= %lu; ts read = %lu; commit ts=%lu",
                  std::this_thread::get_id(),
                  key.c_str(),
                  current_timestamp,
                  read_timestamp,
                  timestamp);
*/
            break;
            //continue;
        }
    }

    // Put
    if (valid) {
        for (auto &write : txn.getWriteSet()) {
//            Debug("[%lx] [MultitapirStore::Prepare] Put key %s at timestamp %lu", std::this_thread::get_id(), write.first.c_str(), timestamp);
            store->PutWithLock(write.first, write.second, timestamp);
        }
    }
#endif

#if 1
    // Unlock
    for (const auto &key : txn.getAllKeys()) {
        store->WriteUnlock(key);
    }
#endif

    if (valid) {
        Debug("txn %ld commit!", timestamp);
        return REPLY_OK;
    }
    Debug("txn %ld abort!", timestamp);
    return REPLY_FAIL;
}

int
Store::Prepare(const Transaction &txn, const Timestamp &timestamp)
{
    // shouldn't come here now
    assert(false);
    // TODO: For now assume we do not support inserts
    Debug("PREPARE at %lu", timestamp);

    int valid = true;

    // check for conflicts with the read set
    // assume ordered read check
    for (const auto &read : txn.getReadSet()) {
        const string& key = read.first;
        const Timestamp& read_timestamp = read.second;
        Timestamp current_timestamp;

        // use the store's write lock to
        // protect access to readers and writers list of this key
        // TODO: this will block Gets -> can we use other locks for this?
        store->WriteLock(key, &current_timestamp);

        if (read_timestamp < current_timestamp) {
            valid = false;
            Debug("[MultitapirStore::Prepare] "
                  " Read check failed due to modified read key;"
                  " ts last wrote= %lu; ts read = %lu",
                  current_timestamp,
                  read_timestamp);
        }

/*
        if (!writers[key]->empty() && timestamp > writers[key]->front()->ts) {
            valid = false;
            Debug("[MultitapirStore::Prepare] "
                  " Read check failed due to active conflicting writers");
        }
*/

        store->WriteUnlock(key);

        if (!valid) {
	        return REPLY_FAIL;
        }
    }

    // TODO: Seems impossible in zipkat because zipkat is doing validation in gsn order.
    // check for conflicts with the write set
#if 0
    for (const auto &write : txn.getWriteSet()) {
        const string& key = write.first;
        Timestamp current_timestamp;

        store->WriteLock(key, &current_timestamp);

        if (timestamp < current_timestamp) {
            valid = false;
            Debug("[MultitapirStore::Prepare] [%lu], [%lu] key = %s; Write check failed due to too small timestamp",
                 timestamp, current_timestamp, key.c_str());
        }

/*
        // if there is a pending read for this key, greater than the
        // proposed timestamp, abort
        if (!readers[key]->empty() && timestamp < readers[key]->back()->ts) {
            valid = false;
            Debug("[MultitapirStore::Prepare] Write check failed due to active conflicting readers");
        }

        // if there is a pending write for this key, greater than the
        // proposed timestamp, abort
        if (!writers[key]->empty() && timestamp < writers[key]->back()->ts) {
            valid = false;
            Debug("[MultitapirStore::Prepare] Write check failed due to active conflicting writers");
        }
*/

        store->WriteUnlock(key);

        if (!valid) {
            return REPLY_FAIL;
        }
    }
#endif

    return REPLY_OK;
}

void
Store::Commit(const Transaction &txn, const Timestamp &timestamp)
{

    Debug("COMMIT r = %lu, w = %lu; timestamp = %lu",
        txn.getReadSet().size(), txn.getWriteSet().size(), timestamp);

    // TODO: TICTOC like optimization - maintain and update read timestamp

    // insert writes into versioned key-value store
    for (auto &write : txn.getWriteSet()) {
        store->Put(write.first, write.second, timestamp);
    }
}

void
Store::ForceCommit(txnid_t txn_id, const Timestamp &timestamp, const Transaction &txn) {
    Debug("[%lu - %lu] FORCE_COMMIT r = %lu, w = %lu; timestamp = %lu", txn_id.first, txn_id.second,
          txn.getReadSet().size(), txn.getWriteSet().size(), timestamp);

    // TODO: TICTOC like optimization - maintain and update read timestamp

    // insert writes into versioned key-value store
    for (auto &write : txn.getWriteSet()) {
        store->Put(write.first, write.second, timestamp);
    }
}

void
Store::Abort(txnid_t txn_id, const Transaction &txn)
{
    Debug("[%lu - %lu] ABORT r = %lu, w = %lu", txn_id.first, txn_id.second,
          txn.getReadSet().size(), txn.getWriteSet().size());

    // clean-up metadata
    // remove transaction from readers and writers
    clean_transaction(txn_id, txn);
}

void
Store::Load(const string &key, const string &value, const Timestamp &timestamp)
{
    store->Put(key, value, timestamp);
    readers[key] = new DLinkedList<PreparingTransaction>();
    writers[key] = new DLinkedList<PreparingTransaction>();
}

} // namespace meerkatstore
