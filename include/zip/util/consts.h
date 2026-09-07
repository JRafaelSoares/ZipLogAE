#pragma once

#include <chrono>
#include <ctime>

#include <limits>
#include <new>
#include <zip/api/api.h>

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
static constexpr uint32_t HUGE_PAGE_SIZE = 1 << 21; /* 2 MiB */

/** buffer size to be used to send messages */
static constexpr uint32_t SEND_BUFFER_SIZE = 1 << 11; /* 2 KiB */

/** circular buffer size to be used to receive messages */
static constexpr uint32_t RECV_BUFFER_SIZE = 1 << 20; /* 1 MiB */
static_assert(RECV_BUFFER_SIZE <= HUGE_PAGE_SIZE);

/** maximum number of outstanding messages */
static constexpr uint32_t MAX_OUTSTANDING = RECV_BUFFER_SIZE / SEND_BUFFER_SIZE;
//static constexpr uint32_t MAX_OUTSTANDING = 1;

/**
 * The constants required for RDMA.
 */
namespace rdma {

    /** size of the entire region used to receive messages (has to be less than 4 GiB) */
    static constexpr uint64_t RECV_REGION_SIZE = 1UL << 30; /* 1 GiB */
    static_assert(RECV_REGION_SIZE <= (1UL << 32));

    /** size of the entire region used to receive messages */
    static constexpr uint32_t MAX_RECV_PAGES = RECV_REGION_SIZE / HUGE_PAGE_SIZE;

    /** size of the RDMA send queue */
    static constexpr uint32_t SEND_QUEUE_SIZE = RECV_BUFFER_SIZE / SEND_BUFFER_SIZE;

    /** size of the RDMA receive queue */
    static constexpr uint32_t RECV_QUEUE_SIZE = RECV_BUFFER_SIZE / SEND_BUFFER_SIZE;

    /** maximum number of bytes for inline data */
    static constexpr uint32_t MAX_INLINE_DATA = 256 - (16 * 3);

    /** boundary to which to align messages to be sent */
    static constexpr uint32_t MESSAGE_ALIGNMENT = 64;

    /** maximum number of queue pairs supported */
    static constexpr uint32_t MAX_QUEUE_PAIRS = RECV_REGION_SIZE / RECV_BUFFER_SIZE;

    /** timeout waiting for an ACK/NACK after 524 us */
    static constexpr uint8_t TIMEOUT = 7;

    /** send a NACK after 120 us if no receive WRs are available */
    static constexpr uint8_t RNR_TIMER = 7;

    /** if receiveing NACK within timeout, retry infinitely */
    static constexpr uint8_t RNR_RETRY = 7;

    /** only retry once retry in case of timeout */
    static constexpr uint8_t RETRY = 1;

} // namespace rdma

/** number of log entries in one block */
static constexpr uint32_t NUM_ENTRIES_PER_BLOCK = 1024 * 256;

/** size of the metadata in a single log entry */
static constexpr uint32_t PAYLOAD_META_SIZE = std::max(sizeof(zip::api::storage_append), sizeof(zip::api::subscriber_log_entries) + sizeof(zip::api::subscriber_log_entries::entry_t));

/** maximum size of the payload in a single log entry */
static constexpr uint32_t MAX_PAYLOAD = SEND_BUFFER_SIZE - PAYLOAD_META_SIZE;

/** maximum size of the payload in a single inline log entry */
static constexpr uint32_t MAX_PAYLOAD_INLINE = rdma::MAX_INLINE_DATA - PAYLOAD_META_SIZE;

/**
 * maximum number of clients for which GSNs can fit in a single message
 */
static constexpr uint32_t MAX_CLIENTS =
    (SEND_BUFFER_SIZE - sizeof(zip::api::storage_new_epoch))
    / sizeof(zip::api::storage_new_epoch::assignment_t);
static_assert(MAX_CLIENTS <= std::numeric_limits<client_t>::max());

/** number of blocks that are recycles at the subscriber */
static constexpr uint32_t NUM_SUBSCRIBER_BLOCKS = 3;

/** duration of a single epoch */
static constexpr auto EPOCH_DURATION = std::chrono::milliseconds(10);

/** minimum number of slots assigned per epoch */
static constexpr uint32_t MIN_SLOTS = 1000;

/** timeout for waiting for a `::poll` call on a socket */
static constexpr timespec POLL_TIMEOUT { .tv_sec = 1 };

/** maximum number of outstanding `::accept`s on a socket */
static constexpr uint32_t MAX_SOCKET_LISTEN = 4096;

/** default network device to use */
static constexpr const char* DEFAULT_DEVICE = "rxe0";

/** default network device port to use */
static constexpr uint8_t DEFAULT_DEVICE_PORT = 1;

/** default network device port GID index */
static constexpr int DEFAULT_GID = -1;

/** default port to run server on */
static constexpr uint16_t DEFAULT_SERVER_PORT = 6666;

} // namespace zip::util
