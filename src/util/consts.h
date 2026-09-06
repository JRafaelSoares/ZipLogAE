#pragma once

#include <chrono>
#include <ctime>

#include "api/api.h"

namespace zip::consts {

/**
 * Log levels.
 */
enum log_level {
    TRACE = 0,
    DEBUG,
    INFO,
    WARN,
    ERROR
};

/** the current log level */
static constexpr log_level LOG_LEVEL = INFO;

/** huge page size and granularity for memory allocation */
static constexpr unsigned long HUGE_PAGE_SIZE = 1 << 21; /* 2 MiB */

/** buffer size to be used to send messages */
static constexpr unsigned long SEND_BUFFER_SIZE = 1 << 11; /* 2 KiB */

/** circular buffer size to be used to receive messages */
static constexpr unsigned long RECEIVE_BUFFER_SIZE = 1 << 18; /* 256 KiB */

/**
 * The constants required for RDMA.
 */
namespace rdma {

    /** maximum number of outstanding requests on a queue */
    static constexpr unsigned long MAX_OUTSTANDING_REQUESTS = RECEIVE_BUFFER_SIZE / SEND_BUFFER_SIZE;

    /** maximum number of bytes for inline data */
    static constexpr unsigned long MAX_INLINE_DATA = 192;

    /** maximum number of receive requests to prepare */
    static constexpr unsigned long MAX_RECEIVE_REQUESTS = 64;
    static_assert(MAX_RECEIVE_REQUESTS <= MAX_OUTSTANDING_REQUESTS);

    /** maximum receive requests to poll at once */
    static constexpr unsigned long RECEIVE_BATCH_SIZE = 8;

    /** boundary to which to align messages to be sent */
    static constexpr unsigned long MESSAGE_ALIGNMENT = 64;

    /** maximum number of queue pairs supported */
    static constexpr unsigned long MAX_QUEUE_PAIRS = 2048;
    static_assert(MAX_QUEUE_PAIRS <= (1UL << 32) / RECEIVE_BUFFER_SIZE);

    /** timeout waiting for an ACK/NACK after 524 us */
    static constexpr uint8_t TIMEOUT = 7;

    /** send a NACK after 120 us if no receive WRs are available */
    static constexpr uint8_t RNR_TIMER = 7;

    /** if receiveing RNR within timeout, retry infinitely */
    static constexpr uint8_t RNR_RETRY = 7;

    /** only retry once retry in case of timeout */
    static constexpr uint8_t RETRY = 1;

} // namespace rdma

/** number of shards */
static constexpr unsigned long NUM_SHARDS = 1;

/**
 * maximum number of storage servers in a given shard
 * that can fit in a single message
 */
static constexpr unsigned long MAX_REPLICAS = 10;

/** number of log entries in one block */
static constexpr unsigned long NUM_ENTRIES_PER_BLOCK = 1024 * 256;

/** maximum size of the payload in a single message */
static constexpr unsigned long MAX_PAYLOAD = SEND_BUFFER_SIZE
    - sizeof(zip::api::subscriber_log_entry);

/**
 * maximum size of the clients for which
 * GSNs can fit in a single message
 */
static constexpr unsigned long MAX_CLIENTS =
    (SEND_BUFFER_SIZE - sizeof(zip::api::storage_slots))
    / sizeof(zip::api::storage_slots::assignment);

/** duration of a single epoch */
static constexpr std::chrono::milliseconds EPOCH_DURATION(5);

/** minimum number of slots assigned per epoch */
static constexpr unsigned long MIN_SLOTS = 100;

/** timeout for waiting for a `::poll` call on a socket */
static constexpr timespec POLL_TIMEOUT { .tv_sec = 1 };

/** default network device to use */
static constexpr const char* DEFAULT_DEVICE = "mlx5_0";

/** default network device port to use */
static constexpr uint8_t DEFAULT_DEVICE_PORT = 1;

/** default network device port GID index */
static constexpr int DEFAULT_GID = -1;

/** default port to run server on */
static constexpr uint16_t DEFAULT_PORT = 6666;

static constexpr std::chrono::microseconds HEARTBEAT_INTERVAL(300);

} // namespace zip::util
