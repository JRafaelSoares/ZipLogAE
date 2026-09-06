#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "api/api.h"
#include "network/send_queue.h"
#include "subscriber/log.h"

namespace zip {

namespace network {

/** Forward declaration for manager and receive queue. */
class manager; class recv_queue;

} // namespace network

namespace subscriber {

/**
 * This class represents a Ziplog subscriber.
 */
class subscriber {

public:

    /**
     * Initialize a subscriber.
     *
     * @param manager          the network manager
     * @param polling_cpus     CPUs to use for polling network receive queues
     * @param application_cpus CPUs to use for processing log entries
     * @param order            the address of the ordering server
     * @param subscriber_id    the ID of the subscriber
     * @param sequential       whether the entries are delivered in order
     * @param failures         the number of failures to tolerate
     * @param callback         callback function to run on each delivered entry
     */
    subscriber(
        zip::network::manager& manager,
        std::set<uint16_t> polling_cpus,
        std::set<uint16_t> application_cpus,
        std::string order,
        uint64_t subscriber_id,
        bool sequential,
        unsigned long failures,
        const std::function<void(unsigned long, zip::api::subscriber_log_entry&)>& callback
    );

    /**
     * Stop the subscriber.
     */
    void stop();

private:

    /**
     * This method is for communicating with
     * the ordering service.
     *
     * @param recv_queue the receive queue for this thread
     */
    void control(zip::network::recv_queue recv_queue);

    /**
     * This method is for polling the receive queue
     * and adding the entries to the log.
     *
     * @param recv_queue the receive queue for this thread
     * @param index      the index of this thread
     */
    void poll(zip::network::recv_queue recv_queue);

    /**
     * This method is for iterating the log and processing log entries.
     *
     * @param index      the index of this thread
     * @param sequential whether to process sequentially
     */
    void process(unsigned long index, bool sequential);

    /** ID of the subscriber */
    uint64_t subscriber_id_;

    /** log for this subscriber */
    log log_;

    /** signal to stop the control thread */
    std::atomic<bool> stop_ = false;

    /** signal to stop the processing threads */
    std::atomic<bool> process_ = true;

    /** intro message for this subscriber */
    zip::api::subscriber_intro intro_;

    /** network manager */
    zip::network::manager& manager_;

    /** network send queue of the ordering service */
    zip::network::send_queue order_;

    /** vector of processing threads */
    std::vector<std::thread> threads_;

    /** number of polling threads */
    unsigned long polling_threads_;

    /** number of application threads */
    unsigned long application_threads_;

    /** number of failures to tolerate */
    unsigned long failures_;

    /** callback function to execute */
    const std::function<void(unsigned long, zip::api::subscriber_log_entry&)> callback_;

};

} // namespace subscriber

} // namespace zip
