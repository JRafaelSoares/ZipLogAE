// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/transaction.cc
 *   A transaction implementation.
 *
 **********************************************************************/

#include "store/common/transaction.h"
#include <cstring>

Transaction::Transaction() :
    readSet(), writeSet() { }

Transaction::Transaction(uint8_t nr_reads, uint8_t nr_writes, char* buf) {
    auto *read_ptr = reinterpret_cast<read_t *> (buf);
    for (int i = 0; i < nr_reads; i++) {
        std::string key(read_ptr->key, 64);
        readSet[key] = Timestamp(read_ptr->timestamp);
        read_ptr++;
        allKeys.emplace(key);
    }

    auto *write_ptr = reinterpret_cast<write_t *> (read_ptr);
    for (int i = 0; i < nr_writes; i++) {
        std::string key(write_ptr->key, 64);
        writeSet[std::string(write_ptr->key, 64)] = std::string(write_ptr->value, 64);
        write_ptr++;
        allKeys.emplace(key);
    }

    auto *index_ptr = reinterpret_cast<int *> (write_ptr);
    for (int i = 0; i < allKeys.size(); i++) {
        keyIndexes.emplace(*index_ptr);
        index_ptr++;
    }
}

Transaction::~Transaction() { }

const ReadSetMap& Transaction::getReadSet() const
{
    return readSet;
}
const WriteSetMap& Transaction::getWriteSet() const
{
    return writeSet;
}
const std::unordered_set<std::string>& Transaction::getAllKeys() const
{
    return allKeys;
}
const std::set<int>& Transaction::getKeyIndexes() const
{
    return keyIndexes;
}

void
Transaction::addReadSet(const std::string &key, const Timestamp &readTime)
{
    readSet[key] = readTime;
}

void
Transaction::addWriteSet(const std::string &key, const std::string &value)
{
    writeSet[key] = value;
}

void Transaction::serialize(char *reqBuf) const {
    // no coming here at the storage server side
    assert(false);
    auto *read_ptr = reinterpret_cast<read_t *> (reqBuf);
    for (auto read : readSet) {
        read_ptr->timestamp = read.second;
        std::memcpy(read_ptr->key, read.first.c_str(), 64);
        read_ptr++;
    }

    auto *write_ptr = reinterpret_cast<write_t *> (read_ptr);
    for (auto write : writeSet) {
        std::memcpy(write_ptr->key, write.first.c_str(), 64);
        std::memcpy(write_ptr->value, write.second.c_str(), 64);
        write_ptr++;
    }
}

void
Transaction::clear()
{
    readSet.clear();
    writeSet.clear();
    allKeys.clear();
    keyIndexes.clear();
}
