#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <ratio>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <hdr/hdr_histogram.h>
#include <cxxopts.hpp>

#include "api/api.h"
#include "lock_service/lock_service.h"
#include "network/buffer.h"
#include "network/manager.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/util.h"

namespace zip::lock_service {

/** create a logger for this file */
static zip::util::logger logger("lock_service");

void lock_service::callback(unsigned long index, zip::api::subscriber_log_entry& entry) {
    //if (entry.data_length > 0)
    //    logger.info("Delivered entry from client (", entry.client_id, ") with GSN ", entry.gsn, " data_length=", entry.data_length);
    auto& req = *reinterpret_cast<zip::lock_service::LockRequest*>(entry.data);
    auto key = req.lock_key;
    ZIP_ASSERT(key < lock_queue_.size(), "invalid lock key");
    auto& queue = lock_queue_[key];
    logger.trace("Request from client (", req.client, ") action=", req.action, ", lock_key=", key);

    if (req.action == LockAction::Lock) {
        if (queue.empty()) {
            //logger.info("Acquire client-", req.client, " lock_key-", req.lock_key, " done\n");
            if (req.client == cid_)
                req.done->store(true, std::memory_order_relaxed);
        }
        queue.push_back(std::move(req));
    } else {
        ZIP_ASSERT(!queue.empty(), "trying to unlock an invalid key ", req.lock_key);
        auto& top = queue.front();
        queue.pop_front();
        ZIP_ASSERT(top.action == LockAction::Lock, "invalid action");
        ZIP_ASSERT(top.client == req.client, "client mismatched: ", top.client, " vs ", req.client);
        ZIP_ASSERT(top.lock_key == req.lock_key, "key mismatched: ", top.lock_key, " vs ", req.lock_key);

        if (top.client == cid_)
            top.done->store(true, std::memory_order_relaxed);
        //logger.info("Release client-", req.client, " lock_key-", req.lock_key, " done\n");

        if (!queue.empty()) {
            auto& top = queue.front();
            if (top.client == cid_)
                top.done->store(true, std::memory_order_relaxed);
            //logger.info("Acquire client-", top.client, " lock_key-", top.lock_key, " done\n");
        }
    }
}

constexpr size_t kNumBuffers = 10;

lock_service::lock_service(
     size_t num_clients, size_t num_keys, zip::network::manager& manager, zip::network::manager& manager2,
     const std::string& ord, int cid, int sid, int16_t client_cpu, uint64_t rate, uint64_t fal,
     const std::set<uint16_t>& polling_cpus, const std::set<uint16_t>& application_cpus,
     int sub_id)
 : num_keys_(num_keys),
   cid_(cid),
   lock_queue_(num_keys_),
   manager_(manager),
   manager2_(manager2) {
    
   //buffers_(num_clients, manager.get_buffers(10)),
   //buffer_iter_(buffers_[0])  {
   for (size_t i = 0; i < num_clients; ++i) {
       buffers_.emplace_back(manager.get_buffers(kNumBuffers));
       //buffer_iter_.emplace_back(zip::util::wraparound_iterator(buffers_[i]));
       buffer_idx_.emplace_back(0);
    }

    subscriber_ = std::make_unique<zip::subscriber::subscriber>(
        manager, polling_cpus, application_cpus, ord, sub_id, /* seq */true, fal,
        std::bind(&lock_service::callback, this, std::placeholders::_1, std::placeholders::_2));

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(5s);

    ziplog_client_= std::make_unique<zip::client::client>(manager2, ord, cid, sid, client_cpu, rate, fal);
}

void lock_service::request(uint64_t client, uint64_t lock_key, LockAction action) {
    //logger.info(action, " Request client-", client, " lock_key-", lock_key);
    std::atomic<bool> done = false;
    auto buffer = &buffers_[0][buffer_idx_[0]];
    //auto buffer = &buffers_[client][buffer_idx_[client]];
    buffer_idx_[0] = (buffer_idx_[0] + 1) % kNumBuffers;
    //buffer_idx_[client] = (buffer_idx_[client] + 1) % kNumBuffers;
    //auto buffer = buffer_iter_[client].get_and_increment();
    auto& req = buffer->as<zip::api::storage_insert>();
    req.data_length = sizeof(struct LockRequest);
    auto& data = *reinterpret_cast<struct LockRequest*>(req.data);
    data.action = action;
    data.client = client;
    data.lock_key = lock_key;
    data.done = &done;

    std::atomic<uint64_t> response;
    ziplog_client_->insert(*buffer, response);

    while (!done.load(std::memory_order_relaxed));
    logger.trace(action, " Request client-", client, " lock_key-", lock_key, " finish\n");
}

void lock_service::acquire(uint64_t client, uint64_t lock_key) {
    request(client, lock_key, LockAction::Lock);
}
void lock_service::release(uint64_t client, uint64_t lock_key) {
    request(client, lock_key, LockAction::Unlock);
}

void lock_service::exit() {
    subscriber_->stop();
}

} // namespace zip::subscriber
