#include <zip/network/buffer.h>

#include <zip/network/manager.h>

namespace zip::network {

/**
 * Deinitialize a buffer.
 */
buffer::~buffer() {
    // put the buffer back into the free list
    manager_.put_buffer(buffer_);
}

} // namespace zip::network
