#include <fstream>

#include "zipkat.h"

namespace zip {
namespace app {
namespace zipkat {

// TODO: refine
static const std::string kKeysFile = "/home/yh885/zipkat/keys";

Zipkat::Zipkat(uint64_t num_keys)
    : kvs_(std::make_unique<PthreadKvs>()),
      store_(std::make_unique<meerkatstore::Store>(
        /* twopc */false, /* replicated */true, kvs_.get())) {

    // Load keys in memory
    std::string key;
    std::ifstream in;
    in.open(kKeysFile);
    if (!in) {
        fprintf(stderr, "Could not read keys from: %s\n", kKeysFile.c_str());
        exit(0);
    }

    Debug("Load %lu keys", num_keys);
    for (unsigned int i = 0; i < num_keys; i++) {
        getline(in, key);

        // TODO: only one shard
/*
        uint64_t hash = 5381;
        const char* str = key.c_str();
        for (unsigned int j = 0; j < key.length(); j++) {
            hash = ((hash << 5) + hash) + (uint64_t)str[j];
        }

        if (hash % FLAGS_numShards == FLAGS_shardIndex) {
*/
            store_->Load(key, "null", Timestamp());
//        }
    }
    in.close();
    logger.info("Zipkat done loading ", num_keys, " keys");
}

int Zipkat::Execute(CommandType cmd, void* req, void* resp,
                    size_t req_leng, size_t resp_leng) {
    switch (cmd) {
        case app::CommandType::GET: {
            static constexpr size_t kMeerkatKeyLeng = 64;
            auto out = reinterpret_cast<std::pair<Timestamp, std::string>*>(resp);
            ZIP_ASSERT(req_leng == kMeerkatKeyLeng, "meerkat::store only supports 64B keys but receive ", req_leng);
            ZIP_ASSERT(resp_leng == sizeof(std::pair<Timestamp, std::string>), "resp_leng not match");
            auto status = store_->Get(std::string(reinterpret_cast<char*>(req), kMeerkatKeyLeng), *out);
            if (status == REPLY_FAIL)
                out->first = Zipkat::kKeyNotFound;
            return status;
        }
        case app::CommandType::COMMIT: {
            auto entry = reinterpret_cast<zip::storage::log::entry*>(req);
            ZIP_ASSERT(entry->txn, "null Transaction");
            return store_->TryCommit(*entry->txn, entry->gsn, &entry->locked);
        }
        default:
            ZIP_ASSERT(false, "Unknown CommandType %d", cmd);
    }
    return 0;
}

} // namespace zipkat
} // namespace app
} // namespace zip
