#pragma once

#include <cstdint>

#include <zip/api/api.h>

namespace zip::pc::api {

/**
 * Message types.
 */
enum message_types {

    /*=== begin messages ===*/
    SERVER_INTRO = zip::api::MAX_MESSAGE_TYPE,

};



struct server_request_debug: zip::api::message<server_request_debug> {

    /** type tag for this message */
    static constexpr uint32_t tag = SERVER_INTRO;

    uint32_t  request_id; /** size of the data */

} __attribute__((packed));

} // namespace zip::pc::api
