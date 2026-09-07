// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/transaction.cc
 *   A transaction implementation.
 *
 **********************************************************************/

#include "store/common/transaction.h"
#include <cstring>

using namespace std;

Transaction::Transaction() :
    readSet(), writeSet() { }
    //readSet(10), writeSet(5) { }

Transaction::Transaction(uint8_t nr_reads, uint8_t nr_writes, char* buf) {
    // no coming here at the client side
    assert(false);

    auto *read_ptr = reinterpret_cast<read_t *> (buf);
    for (int i = 0; i < nr_reads; i++) {
        readSet[std::string(read_ptr->key, 64)] = Timestamp(read_ptr->timestamp);
        read_ptr++;
    }

    auto *write_ptr = reinterpret_cast<write_t *> (read_ptr);
    for (int i = 0; i < nr_writes; i++) {
        writeSet[std::string(write_ptr->key, 64)] = std::string(write_ptr->value, 64);
        write_ptr++;
    }
}

Transaction::~Transaction() { }

const ReadSetMap&
Transaction::getReadSet() const
{
    return readSet;
}

const WriteSetMap&
Transaction::getWriteSet() const
{
    return writeSet;
}

void
Transaction::addReadSet(const string &key, int idx, const Timestamp &readTime)
{
    readSet[key] = readTime;
    keyIndexes.emplace(idx);
}

void
Transaction::addWriteSet(const string &key, int idx, const string &value)
{
    writeSet[key] = value;
    keyIndexes.emplace(idx);
}

void Transaction::serialize(char *reqBuf) const {
    auto *read_ptr = reinterpret_cast<read_t *> (reqBuf);
    for (auto read : readSet) {
        read_ptr->timestamp = read.second.getTimestamp();
        std::memcpy(read_ptr->key, read.first.c_str(), 64);
        read_ptr++;
    }

    auto *write_ptr = reinterpret_cast<write_t *> (read_ptr);
    for (auto write : writeSet) {
        std::memcpy(write_ptr->key, write.first.c_str(), 64);
        std::memcpy(write_ptr->value, write.second.c_str(), 64);
        write_ptr++;
    }

    auto index_ptr = reinterpret_cast<int *>(write_ptr);
    for (auto idx : keyIndexes) {
        *index_ptr = idx;
        index_ptr++;
    }
}

void
Transaction::clear()
{
    readSet.clear();
    writeSet.clear();
    keyIndexes.clear();
}
