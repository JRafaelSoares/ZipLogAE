#pragma once

#include <array>
#include <chrono>

#include "api/api.h"

namespace zip {
namespace util {

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

} // namespace util

namespace consts {

/** the current log level */
//static constexpr zip::util::log_level LOG_LEVEL = zip::util::TRACE;
static constexpr zip::util::log_level LOG_LEVEL = zip::util::INFO;

/**
 * The constants required for RDMA.
 */
namespace rdma {

    /** RDMA device to use for communication */
    static constexpr const char* DEFAULT_DEVICE = "mlx5_0";

    /** RDMA device port to use for communication */
    static constexpr uint8_t DEFAULT_PORT = 1;

    /** RDMA device port to use for communication */
    static constexpr int8_t DEFAULT_GID = -1;

    /** maximum number of SGE elements in a work request */
    static constexpr unsigned long MAX_SCATTER_GATHER_ELEMENTS = 1;

    /** maximum number of outstanding requests on a queue */
    static constexpr unsigned long MAX_OUTSTANDING_REQUESTS = 1024;
    //static constexpr unsigned long MAX_OUTSTANDING_REQUESTS = 128;

    /** maximum number of bytes for inline data */
    static constexpr unsigned long MAX_INLINE_DATA = 256;

    /** maximum number of receive requests to prepare */
    //static constexpr unsigned long MAX_RECEIVE_REQUESTS = 512;
    static constexpr unsigned long MAX_RECEIVE_REQUESTS = 64;

    /** maximum receive requests to poll at once */
    //static constexpr unsigned long RECEIVE_BATCH_SIZE = 16;
    static constexpr unsigned long RECEIVE_BATCH_SIZE = 4;

    /** maximum number of queue pairs supported */
    static constexpr unsigned long MAX_QUEUE_PAIRS = 1 << 11;

} // namespace rdma

/** page size and the MTU for RDMA communication */
static constexpr unsigned long PAGE_SIZE = 4096; /* 4 KiB */

/** huge page size and granularity for memory allocation */
static constexpr unsigned long HUGE_PAGE_SIZE = 2 * 1024 * 1024; /* 2 MiB */

/** default port to run server on */
static constexpr uint16_t DEFAULT_PORT = 6666;

/** number of receive queues for different sizes */
static constexpr unsigned long NUM_BUFFER_SIZES = 3;

/** buffer sizes for different receive queues
 *
 * NOTE: These sizes are chosen because they provide
 *       a good spread, as well as making it easy to choose
 *       the network queue indices based on buffer size
 *       using bithacking. Think carefully about modifying them.
 */
static constexpr std::array<unsigned long, NUM_BUFFER_SIZES> BUFFER_SIZES {256, 1024, 4096};

/** number of shards */
static constexpr unsigned long NUM_SHARDS = 1;

/** maximum number of replicas in a given shard */
static constexpr unsigned long MAX_REPLICAS = 10;

/** number of log entries in one block */
static constexpr unsigned long NUM_ENTRIES_PER_BLOCK = 1024 * 256;

/** maximum size of the payload in a single message */
static constexpr unsigned long MAX_PAYLOAD = BUFFER_SIZES.back()
    - sizeof(zip::api::subscriber_log_entry);

/**
 * maximum size of the clients for which
 * GSNs can fit in a single message
 */
static constexpr unsigned long MAX_CLIENTS =
    (BUFFER_SIZES.back() - sizeof(zip::api::storage_order_slots))
    / sizeof(zip::api::storage_order_slots::assignment);

/** duration of a single epoch */
static constexpr std::chrono::milliseconds EPOCH_DURATION(10);

/** minimum number of slots assigned per epoch */
static constexpr unsigned long MIN_SLOTS = 8;

/** timeout for waiting for an `::accept` or `::poll` call on a socket */
static constexpr timespec ACCEPT_TIMEOUT { .tv_sec = 1 };

/** verify that important small messages can be sent inline */
static_assert(sizeof(zip::api::client_finished) <= rdma::MAX_INLINE_DATA);
static_assert(sizeof(zip::api::client_insert_ack) <= rdma::MAX_INLINE_DATA);
static_assert(sizeof(zip::api::subscriber_finished) <= rdma::MAX_INLINE_DATA);
static_assert(sizeof(zip::api::client_insert_ack) <= rdma::MAX_INLINE_DATA);
static_assert(sizeof(zip::api::storage_insert_after) <= rdma::MAX_INLINE_DATA);
static_assert(sizeof(zip::api::storage_client_finalize) <= rdma::MAX_INLINE_DATA);
static_assert(sizeof(zip::api::storage_client_initialize) <= rdma::MAX_INLINE_DATA);

#ifdef COLOCATED_ZIPKAT                                                             
//#define SUBSCRIBER_THREAD_HANDLE_GET 1
#define CLIENT_THREAD_HANDLE_GET 1
//#define SUBSCRIBER_SEND_IA_ACK 1
//#define GET_THREAD_HANDLE_GET 1
//#define SUBSCRIBER_THREAD_HANDLE_INSERT_AFTER 1
#endif

} // namespace consts
} // namespace zip
