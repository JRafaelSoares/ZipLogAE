#pragma once

#include <cstddef>

namespace zip {
namespace app {

enum CommandType {
    /*
     *  input:  char* key
     *  output: std::pair<uint64_t, std::string>, as timestampt and value
     *          if the key doesn't exist, the timestampt will be set to kKeyNotFound.
     *  return REPLY_OK or REPLY_FAIL
     */
    GET,
    /*
     *  // TODO: should be zipkat_commit_request + gsn
     *  input: zip::api::subscriber_log_entry*
     *  return REPLY_OK or REPLY_FAIL
     */
    COMMIT,
};

class Application {
public:
    static constexpr int FAIL = -1;

public:
    Application() = default;
    virtual ~Application() = default;
    // Return FAIL or an application-specific int.
    virtual int Execute(CommandType cmd, void* req, void* resp,
                        size_t req_leng, size_t resp_leng) = 0;
};

} // namespace app
} // namespace zip
