#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <cxxopts.hpp>

#include "api/api.h"
#include "app/zipkat/zipkat.h"
#include "network/manager.h"
#include "storage/storage.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/switch.h"
#include "util/util.h"

/** create a logger for this file */
static zip::util::logger logger("storage_main");

/** signal to stop the storage server */
static std::atomic<bool> stop = false;

/**
 * Start the storage server thread execution.
 */
void start_storage(cxxopts::ParseResult& options) {
    // get the arguments parsed
    auto dev = options["device"].as<std::string>();
    auto por = options["port"].as<uint8_t>();
    auto gid = options["gid"].as<int>();
    auto sid = options["shard_id"].as<uint64_t>();
    auto rid = options["replica_id"].as<uint64_t>();
    auto ccs = options.count("client_cpus") == 0 ? std::vector<unsigned int>() :
        options["client_cpus"].as<std::vector<unsigned int>>();
    auto srv = options["server"].as<unsigned int>();
    auto ord = options["order"].as<std::string>();
    auto scs = options.count("subscriber_cpus") == 0 ? std::vector<unsigned int>() :
        options["subscriber_cpus"].as<std::vector<unsigned int>>();
#ifdef COLOCATED_ZIPKAT
    auto gcs = options.count("get_cpus") == 0 ? std::vector<unsigned int>() :
        options["get_cpus"].as<std::vector<unsigned int>>();
#endif
    auto num_keys = options["num_keys"].as<uint64_t>();

    // initialise the CPUs set
    auto client_cpus = std::set<unsigned int>(ccs.begin(), ccs.end());
    auto subscriber_cpus = std::set<unsigned int>(scs.begin(), scs.end());
    auto get_cpus = std::set<unsigned int>(gcs.begin(), gcs.end());

    // create the struct which is storage server's first message
    zip::api::storage_intro intro;
    intro.message_type = zip::api::STORAGE_INTRO;
    intro.subscriber_queues = subscriber_cpus.size();
    intro.client_queues = client_cpus.size();
#ifdef COLOCATED_ZIPKAT
    intro.get_queues = get_cpus.size();
#endif
    intro.shard_id = sid;
    intro.replica_id = rid;
    intro.port = srv;

    // initialise the network manager and the storage server
    auto manager = zip::network::manager(dev, por, gid);

    // TODO: cmdline arg
    auto zipkat = std::make_unique<zip::app::zipkat::Zipkat>(num_keys);
    auto server = zip::storage::storage(manager, client_cpus, subscriber_cpus, get_cpus,
                                        ord, intro, sid, rid, std::move(zipkat));

    // bind the manager and start listening for connections
    manager.bind_server(srv);

    // wait for connections in a loop
    while (!stop.load(std::memory_order_relaxed)) {
        // try to accept a connection and exchange the first message
        // 2 for client's InsertAfter and ZipkatGet
        //auto result = manager.accept(&intro, intro.length(), 5); if (!result) continue;
        auto result = manager.accept(&intro, intro.length()); if (!result) continue;
        auto& [send_queues, buffer, length, _] = *result;

        // add the client or subscriber
        zip::util::apply(logger, buffer.get(),
            [&, &send_queues=send_queues] (zip::api::client_intro& intro) {
                // add the client to the state
                ZIP_ASSERT_EQ(sid, intro.shard_id, "invalid shard ID received from client");
                server.add_client(intro.client_id, std::move(send_queues));
            },
            [&, &send_queues=send_queues] (zip::api::subscriber_intro& intro) {
                // add the subscriber to the state
#ifdef COLOCATED_ZIPKAT
                ZIP_ASSERT(false, "no subscriber in COLOCATED_ZIPKAT");
#else
                server.add_subscriber(intro.subscriber_id, intro.start_gsn, std::move(send_queues.front()));
#endif
            }
        );
    }
}

/**
 * Entrypoint for the server.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the storage server
    cxxopts::Options options("storage", "The storage server for Ziplog.\n");
    options.add_options()
        ("device", "RDMA device to use.", cxxopts::value<std::string>()->default_value(zip::consts::rdma::DEFAULT_DEVICE))
        ("port", "RDMA device port to use.", cxxopts::value<uint8_t>()->default_value(std::to_string(zip::consts::rdma::DEFAULT_PORT)))
        ("gid", "RDMA device port GID index to use.", cxxopts::value<int>()->default_value(std::to_string(zip::consts::rdma::DEFAULT_GID)))
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("server", "Server port to listen on for connection.", cxxopts::value<unsigned int>()->default_value(std::to_string(zip::consts::DEFAULT_PORT)))
        ("subscriber_cpus", "List of CPUs for running subscriber threads.", cxxopts::value<std::vector<unsigned int>>())
        ("client_cpus", "List of CPUs for running client threads.", cxxopts::value<std::vector<unsigned int>>())
#ifdef COLOCATED_ZIPKAT
        ("get_cpus", "List of CPUs for handling zipkat get requests.", cxxopts::value<std::vector<unsigned int>>())
#endif
        ("shard_id", "ID of the storage server shard.", cxxopts::value<uint64_t>())
        ("replica_id", "ID of the replica within a shard.", cxxopts::value<uint64_t>())
        ("num_keys", "number of keys of zipkat.", cxxopts::value<uint64_t>())
        ("h,help", "Show help.")
    ;

    try {
        auto result = options.parse(argc, argv);
        if (   result.count("help")            != 0
            || result.count("order")           == 0
            || result.count("shard_id")        == 0
            || result.count("replica_id")      == 0
            || result.count("num_keys")        == 0) {
            std::cout << options.help() << std::endl;
            std::exit(0);
        }
        if (result["shard_id"].as<uint64_t>() >= zip::consts::NUM_SHARDS) {
            std::cerr << "Invalid shard_id, should be between 0 and " << (zip::consts::NUM_SHARDS - 1) << std::endl;
            std::exit(0);
        }
        if (result["replica_id"].as<uint64_t>() >= zip::consts::MAX_REPLICAS) {
            std::cerr << "Invalid replica_id, should be between 0 and " << (zip::consts::MAX_REPLICAS - 1) << std::endl;
            std::exit(0);
        }

        // setup signal handler and start the storage server
        std::signal(SIGINT, [] (int signal) { std::cerr << "SIGINT caught\n"; stop.store(true, std::memory_order_relaxed); });
        start_storage(result);
    } catch (const cxxopts::OptionParseException& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
