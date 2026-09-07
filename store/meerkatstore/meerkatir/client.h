// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/meerkatstore/meerkatir/client.h:
 *   Meerkatir client interface (uses meerkatir for replcation and the
 *   meerkatstore transactional storage system).
 *
 * Copyright 2015 Irene Zhang  <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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
 
#ifndef _MEERKATSTORE_MEERKATIR_CLIENT_H_
#define _MEERKATSTORE_MEERKATIR_CLIENT_H_

#include "client/client.h"
#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/common/transaction.h"
#include "network/buffer.h"

#include <memory>
#include <thread>
#include <hdr/hdr_histogram.h>

namespace meerkatstore {
namespace meerkatir {

typedef void (*yield_t)(void);

class Client {
public:
    Client(int nsthreads, int nShards, uint32_t id,
           std::shared_ptr<zip::client::client> client,
           zip::network::manager& manager);

    // Overriding functions from ::Client.
    void Begin();
    int Get(const std::string &key, int idx, std::string &value, yield_t yield);
    int Put(const std::string &key, int idx, const std::string &value);
    bool Commit(yield_t yield);
    void Abort();
    std::vector<int> Stats();
    uint64_t ClientId() { return client_id; }

public:
    // Returns the underlying read and write set.
    const Transaction& GetTransaction() const { return txn; }

private:
    zip::network::buffer& ZiplogBuffer() { return ziplogBuffer.front(); }

    int Prepare(yield_t yield);

private:
    // Unique ID for this client.
    const uint64_t client_id;

    // Transaction to keep track of read and write set.
    Transaction txn;

    // Ongoing transaction ID.
    uint64_t t_id;

    // Ziplog data structures
    std::shared_ptr<zip::client::client> ziplogClient;
    std::list<zip::network::buffer> ziplogBuffer;

#ifdef ZIP_MEASURE
    hdr_histogram* hist_get;
    int hdr_count_get;
    hdr_histogram* hist_commit;
    int hdr_count_commit;
    hdr_histogram* hist_yield;
    int hdr_count_yield;
#endif

    // TODO: remove
    //char* buf;
};

} // namespace meerkatir
} // namespace meerkatstore

#endif /* _MEERKATSTORE_MEERKATIR_CLIENT_H_ */
