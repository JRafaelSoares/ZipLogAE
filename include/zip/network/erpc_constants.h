#pragma once

#include <string>

namespace zip::network {

/// Constants for RPC ID ranges
constexpr uint8_t RPC_ID_RANGE_SIZE = 64;
constexpr uint8_t ORDER_SERVER_OFFSET = 0;
constexpr uint8_t STORAGE_SERVER_OFFSET = 2;
constexpr uint8_t SUBSCRIBER_SERVER_OFFSET = 66;
constexpr uint8_t CLIENT_SERVER_OFFSET = 130;

/// ============================================================================
/// Timeouts and Limits
/// ============================================================================

/// Native RPC request timeout in milliseconds
constexpr int NATIVE_REQUEST_TIMEOUT_MS = 250000;

/// Maximum connection attempts before giving up
constexpr int MAX_CONNECT_ATTEMPTS = 200;

/// Sleep interval during connection retry (milliseconds)
constexpr int CONNECT_RETRY_INTERVAL_MS = 5;

/// Blocking receive timeout (milliseconds)
constexpr int BLOCKING_RECV_TIMEOUT_MS = 100;

/// ============================================================================
/// Message Protocol Constants
/// ============================================================================

/// Request type ID for ZipLog native messages
constexpr uint8_t ZIPLOG_REQUEST_TYPE = 1;

/// Size of native wire header (payload_length only)
constexpr std::size_t NATIVE_HEADER_SIZE = sizeof(uint32_t);

/// Size of ACK response (single uint32_t)
constexpr std::size_t NATIVE_ACK_SIZE = sizeof(uint32_t);

}  // namespace zip::network

