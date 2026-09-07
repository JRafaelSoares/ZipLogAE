#pragma once
#include "logger.h"

// libraries
#include <string>
#include <vector>
#include <set>
#include <unordered_map>

#include <thread>
#include <mutex>
#include <atomic>

#include <chrono>
#include <cstdint>
#include <utility>

#include <iostream>
#include <optional>

#include <arpa/inet.h> // htonl/ntohl
#include <cstring>     // memcpy
#include <unistd.h>

// commonly used types in implementation files
using std::cerr;
using std::cout;
using std::endl;
using std::pair;
using std::set;
using std::string;
using std::unordered_map;
using std::vector;

// std items
using std::atomic;
using std::lock_guard;
using std::mutex;
using std::nullopt;
using std::optional;
using std::thread;

// chrono aliases
using namespace std::chrono;
using namespace std::chrono_literals; // for 100ms, 1s, etc.

namespace ziplog
{

    // constants
    static constexpr uint32_t MAX_MESSAGE_SIZE = 65535; // 2 ^ 16... 2 bytes read in for message header on tcp connection
    static constexpr uint64_t EPOCH_DURATION = 1000;    // defaults to this value if config doesnt specify
    static constexpr uint64_t MAX_EPOCH_HISTORY = 10;   // defaults to this value if config doesnt specify

    // Define htonll/ntohll if not available (not standard on all platforms) - used in messages
#ifndef htonll
    inline uint64_t htonll(uint64_t value)
    {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        uint32_t high = htonl(static_cast<uint32_t>(value >> 32));
        uint32_t low = htonl(static_cast<uint32_t>(value & 0xFFFFFFFF));
        return (static_cast<uint64_t>(low) << 32) | high;
#else
        return value;
#endif
    }

    inline uint64_t ntohll(uint64_t value)
    {
        return htonll(value);
    }
#endif
}