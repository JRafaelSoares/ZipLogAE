#include "network/buffer.h"

#include <array>
#include <string>

#include "network/manager.h"
#include "util/consts.h"
#include "util/log.h"

namespace zip {
namespace network {

/** create a logger for this file */
static zip::util::logger logger("buffer");

buffer::buffer(void* buffer, uint32_t local_key, unsigned long index, manager& manager):
buffer_(buffer), length_(zip::consts::BUFFER_SIZES[index]), index_(index),
local_key_(local_key), manager_(&manager) {}

buffer::~buffer() {
    // put the buffer back into the free list
    if (manager_ != nullptr && buffer_ != nullptr) manager_->put_buffer(buffer_, local_key_, index_);
}

} // namespace network
} // namespace zip
