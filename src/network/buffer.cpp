#include "network/buffer.h"

#include "network/manager.h"
#include "util/log.h"

namespace zip::network {

/** create a logger for this file */
static zip::util::logger logger("buffer");

buffer::buffer(void* buffer, uint32_t local_key, manager& manager):
buffer_(buffer), local_key_(local_key), manager_(&manager) {}

buffer::~buffer() {
    // put the buffer back into the free list
    if (manager_ != nullptr) manager_->put_buffer(buffer_, local_key_);
}

} // namespace zip::network
