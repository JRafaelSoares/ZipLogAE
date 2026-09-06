#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <cxxopts.hpp>
#include <netinet/in.h>

#include "api/api.h"
#include "network/manager.h"
#include "network/send_queue.h"
#include "storage/storage.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/switch.h"

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
    auto num = options["numa"].as<int>();
    auto sid = options["shard_id"].as<uint64_t>();
    auto rid = options["replica_id"].as<uint64_t>();
    auto ccs = options["client_cpus"].as<std::vector<uint16_t>>();
    auto srv = options["server"].as<uint16_t>();
    auto ord = options["order"].as<std::string>();
    auto sdh = options["subscriber_depth"].as<unsigned long>();
    auto scs = options.count("subscriber_cpus") == 0 ? std::vector<uint16_t>() :
        options["subscriber_cpus"].as<std::vector<uint16_t>>();

    // initialize the CPUs set
    auto client_cpus = std::set<uint16_t>(ccs.begin(), ccs.end());
    auto subscriber_cpus = std::set<uint16_t>(scs.begin(), scs.end());

    // create the struct which is storage server's first message
    zip::api::storage_intro intro;
    intro.message_type = zip::api::STORAGE_INTRO;
    intro.client_queues = client_cpus.size();
    intro.shard_id = sid;
    intro.replica_id = rid;
    intro.port = srv;

    // initialize the network manager and the storage server
    auto manager = zip::network::manager(dev, por, gid, num);
    auto server = zip::storage::storage(manager, client_cpus, subscriber_cpus, sdh, ord, intro, sid, rid);

    // bind the manager and start listening for connections
    manager.bind_server(srv);
    logger.info("Storage server initialized");

    // wait for connections in a loop
    while (!stop.load(std::memory_order_relaxed)) {
        // try to accept a connection and exchange the first message
        auto result = manager.accept(&intro, intro.length(), subscriber_cpus.size());
        if (!result) continue;

        // add the client or subscriber
        auto& [send_queues, buffer, length, _] = *result;
        zip::util::apply(logger, buffer, length,
            [&] (zip::api::client_intro& intro) {
                // add the client to the state
                ZIP_ASSERT_EQ(sid, intro.shard_id, "invalid shard ID");
                server.add_client(intro.client_id, std::move(send_queues.front()));
            },
            [&] (zip::api::subscriber_intro& intro) {
                // add the subscriber to the state
                server.add_subscriber(intro.subscriber_id, intro.num_queues, std::move(send_queues));
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
    options.set_width(std::numeric_limits<size_t>::max());
    options.add_options("Network")
        ("device", "RDMA device to use.", cxxopts::value<std::string>()->default_value(zip::consts::DEFAULT_DEVICE))
        ("gid", "RDMA device port GID index to use.", cxxopts::value<int>()->default_value(std::to_string(zip::consts::DEFAULT_GID)))
        ("numa", "NUMA node to use for memory allocation.", cxxopts::value<int>()->default_value("-1"))
        ("port", "RDMA device port to use.", cxxopts::value<uint8_t>()->default_value(std::to_string(zip::consts::DEFAULT_DEVICE_PORT)))
    ;
    options.add_options("Ziplog storage server")
        ("client_cpus", "List of CPUs for running client threads.", cxxopts::value<std::vector<uint16_t>>())
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("replica_id", "ID of the replica within a shard.", cxxopts::value<uint64_t>())
        ("server", "Server port to listen on for connection.", cxxopts::value<uint16_t>()->default_value(std::to_string(zip::consts::DEFAULT_PORT)))
        ("shard_id", "ID of the storage server shard.", cxxopts::value<uint64_t>())
        ("subscriber_cpus", "List of CPUs for running subscriber threads.", cxxopts::value<std::vector<uint16_t>>())
        ("subscriber_depth", "Maximum number of outstanding entries to subscribers.", cxxopts::value<unsigned long>()->default_value("8"))
    ;
    options.add_options()
        ("h,help", "Show help.")
    ;

    try {
        auto result = options.parse(argc, argv);
        if (result.count("help") != 0) {
            std::cout << options.help() << std::endl;
            std::exit(0);
        }
        if (   result.count("order")           == 0
            || result.count("client_cpus")     == 0
            || result.count("shard_id")        == 0
            || result.count("replica_id")      == 0) {
            std::cerr << options.help() << std::endl;
            std::exit(1);
        }

        // setup signal handler and start the storage server
        std::signal(SIGINT, [] ([[maybe_unused]] int signal) { stop.store(true, std::memory_order_relaxed); });
        start_storage(result);
    } catch (const cxxopts::OptionParseException& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
