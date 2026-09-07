#pragma once

#include "app/app.h"
#include "store/common/backend/pthread_kvs.h"
#include "store/meerkatstore/store.h"
#include "storage/log.h"
#include "util/log.h"
#include "util/util.h"

namespace zip {
namespace app {
namespace zipkat {

static zip::util::logger logger("zipkat");

class Zipkat : public app::Application {
public:
    static constexpr uint64_t kKeyNotFound = -2;

public:
    Zipkat(uint64_t num_keys);

public:
    int Execute(CommandType c, void* req, void* resp,
                size_t req_leng, size_t resp_leng) override;

private:
    std::unique_ptr<PthreadKvs> kvs_;
    std::unique_ptr<meerkatstore::Store> store_;
};

} // namespace zipkat
} // namespace app
} // namespace zip
