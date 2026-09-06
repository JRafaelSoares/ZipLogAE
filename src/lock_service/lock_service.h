#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "api/api.h"
#include "client/client.h"
#include "network/buffer.h"
#include "subscriber/subscriber.h"
#include "util/util.h"

namespace zip {

/** Forward declaration for manager and buffer. */
namespace network { class manager; class buffer; }

namespace lock_service {

enum LockAction {
    Lock,
    Unlock
};

struct LockRequest {
    LockAction action;
    uint64_t client;
    uint64_t lock_key;
    std::atomic<bool>* done;
};

/**
 * This class represents a linearizable lock service.
 */
class lock_service {

public:

    /**
     * Initialize a lock.
     */
    lock_service(
        size_t num_clients, size_t num_keys, zip::network::manager& manager, zip::network::manager& manager2,
        const std::string& ord, int cid, int sid, int16_t client_cpu, uint64_t rate, uint64_t fal,
        const std::set<uint16_t>& polling_cpus, const std::set<uint16_t>& application_cpus, int sub_id);

    /**
     * Initialize a lock.
     */
    void acquire(uint64_t client, uint64_t lock_key);

    /**
     * Release a lock.
     */
    void release(uint64_t client, uint64_t lock_key);

    /**
     * Finish the lock service.
     */
    void exit();

private:
    void request(uint64_t client, uint64_t lock_key, LockAction action);
    void callback(unsigned long, zip::api::subscriber_log_entry& entry);

private:
    const uint64_t num_keys_;

    const uint64_t cid_;

    // The queue of each lock key.
    // TODO: pointer LockRequest?
    std::vector<std::list<LockRequest>> lock_queue_;

    /** network manager */
    zip::network::manager& manager_;
    zip::network::manager& manager2_;

    /** network buffers */
    std::vector<std::vector<zip::network::buffer>> buffers_;

    /** network buffer iterator */
    //util::wraparound_iterator<std::vector<zip::network::buffer>> buffer_iter_;
    //std::vector<util::wraparound_iterator<std::vector<zip::network::buffer>>> buffer_iter_;
    std::vector<uint64_t> buffer_idx_;

    /** ziplog subscriber that receive ordering reults */
    std::unique_ptr<zip::subscriber::subscriber> subscriber_;
    //zip::subscriber::subscriber subscriber_;

    /** ziplog client that issue ordering request */
    std::unique_ptr<zip::client::client> ziplog_client_;
    //zip::client::client ziplog_client_;

};

} // namespace lock

} // namespace zip
