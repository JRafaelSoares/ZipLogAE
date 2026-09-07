#pragma once

#include <array>
#include <cstdint>
#include <functional>
#include <list>
#include <string>
#include <vector>

#include "api/api.h"
#include "network/buffer.h"
#include "network/recv_queue.h"
#include "network/send_queue.h"
#include "util/consts.h"
#include "util/util.h"

namespace zip {

/** Forward declaration for manager. */
namespace network { class manager; }

namespace subscriber {

/**
 * This class represents a Ziplog subscriber.
 */
class subscriber {

public:

    /**
     * Initialise a subscriber.
     *
     * @param manager       the network manager
     * @param order         the address of the ordering server
     * @param subscriber_id the ID of the subscriber
     * @param start_gsn     starting GSN from which to deliver packets
     * @param ordered       whether the entries are delivered in order
     */
    subscriber(
        zip::network::manager& manager,
        std::string order,
        uint64_t subscriber_id,
        uint64_t start_gsn,
        bool ordered
    );

    /**
     * Deinitialise a subscriber.
     */
    ~subscriber();

    /**
     * Poll for a record and execute the callback.
     *
     * @param callback the function to run
     *
     * @return number of times the callback was executed
     */
    unsigned long poll(const std::function<void(zip::api::subscriber_log_entry&)>& callback);

private:

    /** ID of the subscriber */
    uint64_t subscriber_id_;

    /** next global sequence number to be delivered */
    uint64_t expected_gsn_;

    /** whether to deliver packets in order */
    bool ordered_;

    /** intro message for this subscriber */
    zip::api::subscriber_intro intro_;

    /** network manager */
    zip::network::manager& manager_;

    /** current subscriber's receive queue */
    zip::network::recv_queue recv_queue_;

    /** network send queue of the ordering service */
    zip::network::send_queue order_;

    /** map from shard IDs to vector of replica IDs in that shard */
    std::array<std::vector<uint64_t>, zip::consts::NUM_SHARDS> storage_replicas_;

    /** map from shard IDs to number of client queues */
    std::array<unsigned int, zip::consts::NUM_SHARDS> storage_client_queues_{};

    /** map from shard IDs to number of subscriber queues */
    std::array<unsigned int, zip::consts::NUM_SHARDS> storage_subscriber_queues_{};

    /** map from shard IDs to send queues to storage server replicas */
    std::array<std::vector<zip::network::send_queue>, zip::consts::NUM_SHARDS> storage_queues_;

    /** list of log entries currently being processed */
    using requests_list = std::list<zip::util::unique_ptr_malloc<zip::api::subscriber_log_entry>>;
    requests_list current_requests_;

    /** map from shard IDs to the position in the list of current requests for each replica */
    std::array<std::array<requests_list::iterator, zip::consts::MAX_REPLICAS>, zip::consts::NUM_SHARDS> storage_iterators_;

    /** free list of allocated buffers for log entries */
    requests_list free_requests_;

    /** map from shard IDs to the latest GSN received */
    std::array<uint64_t, zip::consts::NUM_SHARDS> latest_gsn_;

    /** array of lists to store all the buffers in use */
    std::array<std::list<zip::network::buffer>, zip::consts::NUM_BUFFER_SIZES> used_buffers_;

};

} // namespace subscriber
} // namespace zip
