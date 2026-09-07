#pragma once

#include <cstdint>
#include <utility>

namespace zip {
namespace network {

/** Forward declaration for manager. */
class manager;

/**
 * This class represents a single network buffer.
 */
class buffer {

public:

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
        std::swap(index_, other.index_);
        std::swap(length_, other.length_);
        std::swap(local_key_, other.local_key_);
        std::swap(manager_, other.manager_);
    }

    /**
     * Move assignment operator for the buffer object.
     *
     * @param other the other buffer
     *
     * @returns a reference to this buffer object
     */
    inline buffer& operator=(buffer&& other) {
        // if the given object is different
        // then swap the elements
        if (this != &other) {
            std::swap(buffer_, other.buffer_);
            std::swap(index_, other.index_);
            std::swap(length_, other.length_);
            std::swap(local_key_, other.local_key_);
            std::swap(manager_, other.manager_);
        }

        // return a reference to this
        return *this;
    };

    /**
     * Initialise an network buffer.
     *
     * @param buffer     address of the buffer
     * @param local_key  local key for network device access to this buffer
     * @param index      size index of the buffer
     * @param manager    the network manager
     */
    buffer(void* buffer, uint32_t local_key, unsigned long index, manager& manager);

    /**
     * Deinitialise a buffer.
     */
    ~buffer();

    /**
     * Use the buffer as a void pointer.
     *
     * @return the buffer's underlying pointer
     */
    template <typename T>
    inline T& as() {
        return *reinterpret_cast<T*>(buffer_);
    }

    unsigned long length() { return length_; }

private:

    /** Make send and receive queues friends of this class. */
    friend class send_queue;
    friend class recv_queue;

    /** address of the buffer */
    void* buffer_ = nullptr;

    /** length of the buffer */
    unsigned long length_ = 0;

    /** size index for this buffer */
    unsigned long index_;

    /** local key for network device access to this buffer */
    uint32_t local_key_;

    /** pointer to the network manager */
    manager* manager_ = nullptr;

};

} // namespace network
} // namespace zip
