#pragma once

#include <concepts>
#include <cstdint>
#include <memory>
#include <new>
#include <utility>

#include "util/consts.h"

namespace zip {

/** Forward declaration for message. */
namespace api { template <typename T> struct __attribute__((packed)) message; }

namespace network {

/** Forward declaration for manager. */
class manager;

/**
 * This class represents a single network buffer.
 */
class buffer {

public:

    /** Use the default implicit constructor. */
    buffer() = default;

    /** Delete the copy constructor. */
    buffer(const buffer&) = delete;

    /** Delete the copy assignment operator. */
    buffer& operator=(const buffer&) = delete;

    /**
     * Move constructor for the buffer object.
     *
     * @param other the other buffer
     */
    inline buffer(buffer&& other) {
        std::swap(buffer_, other.buffer_);
        std::swap(local_key_, other.local_key_);
        std::swap(manager_, other.manager_);
    }

    /**
     * Move assignment operator for the buffer object.
     *
     * @param other the other buffer
     *
     * @return a reference to this buffer object
     */
    inline buffer& operator=(buffer&& other) {
        // if the given object is different
        // then swap the elements
        if (this != &other) {
            std::swap(buffer_, other.buffer_);
            std::swap(local_key_, other.local_key_);
            std::swap(manager_, other.manager_);
        }

        // return a reference to this
        return *this;
    };

    /**
     * Initialize an network buffer.
     *
     * @param buffer     address of the buffer
     * @param local_key  local key for network device access to this buffer
     * @param manager    the network manager
     */
    buffer(void* buffer, uint32_t local_key, manager& manager);

    /**
     * Deinitialize a buffer.
     */
    ~buffer();

    /**
     * Use the buffer as a void pointer.
     *
     * @return the buffer's underlying pointer
     */
    template <typename T> requires std::derived_from<T, zip::api::message<T>>
    inline T& as() {
        return *std::launder(reinterpret_cast<T*>(std::assume_aligned<zip::consts::SEND_BUFFER_SIZE>(buffer_)));
    }

private:

    /** Make send and receive queues friends of this class. */
    friend class send_queue;
    friend class recv_queue;

    /** address of the buffer */
    void* buffer_ = nullptr;

    /** local key for network device access to this buffer */
    uint32_t local_key_;

    /** pointer to the network manager */
    manager* manager_ = nullptr;

};

} // namespace network

} // namespace zip
