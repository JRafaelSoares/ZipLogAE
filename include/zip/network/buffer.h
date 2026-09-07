#pragma once

#include <cstdint>
#include <memory>
#include <new>

#include <zip/util/consts.h>

namespace zip::network {

/** Forward declaration for manager and send queue. */
class manager;

/**
 * This class represents a single network buffer.
 */
class buffer {

public:

    /**
     * Initialize an network buffer.
     *
     * @param buffer     address of the buffer
     * @param manager    the network manager
     */
    buffer(void* buffer, manager& manager):
        buffer_(buffer), manager_(manager) {}

    /**
     * Deinitialize a buffer.
     */
    ~buffer();

    /**
     * Use the buffer as an object of the given type.
     *
     * @return the buffer's underlying pointer as a reference
     *         to the given type
     */
    template <typename T>
    inline T& as() const {
        return *std::launder(static_cast<T*>(std::assume_aligned<zip::consts::SEND_BUFFER_SIZE>(buffer_)));
    }

    /** address of the buffer */
    void* buffer_;
private:
    /** network manager */
    manager& manager_;

};

} // namespace zip::network
