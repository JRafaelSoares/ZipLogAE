#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <string>

#include <cxxopts.hpp>

#include <zip/api/api.h>
#include <zip/network/manager.h>
#include <zip/storage/storage.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/util.h>

#include <zip/network/erpc_transport.h>
#include <zip/network/erpc_constants.h>

/** create a logger for this file */
static zip::util::logger logger("storage_main");

/** signal to stop the server */
static std::atomic<bool> stop = false;

/**
 * Start the server.
 */
void start_server(cxxopts::ParseResult& options) {
    // get the arguments parsed
    auto addr = options["address"].as<std::string>();
    auto por = options["port"].as<uint16_t>();
    auto num = options["numa"].as<int>();
    auto sid = options["shard_id"].as<shard_t>();
    auto rid = options["replica_id"].as<replica_t>();
    auto ncs = options["client_cpus"].as<std::vector<uint16_t>>();
    auto ord = options["order"].as<std::string>();
    auto tim = options["timeout"].as<uint64_t>();

    // initialize the network manager and server
    auto manager = zip::network::manager(num);
    auto transport = std::make_unique<zip::network::erpc_transport_factory>(manager, zip::network::STORAGE_SERVER_OFFSET, addr, por);


    // initialize the set of client and subscriber CPUs
    auto client_cpus = std::set(ncs.begin(), ncs.end());
    std::set<uint16_t> subscriber_cpus;
    if (options.count("subscriber_cpus") > 0) {
        auto nss = options["subscriber_cpus"].as<std::vector<uint16_t>>();
        subscriber_cpus = std::set(nss.begin(), nss.end());
    }

    // initialize the storage server
    auto storage = zip::storage::storage(manager, *transport, ord, sid, rid, client_cpus, subscriber_cpus, std::chrono::nanoseconds(tim),
        addr, por);

    // wait for the storage server to be killed
    stop.wait(false, std::memory_order_relaxed);
}

/**
 * Entrypoint for the server.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the server
    cxxopts::Options options("server", "The storage server for Ziplog.\n");
    options.set_width(std::numeric_limits<size_t>::max());
    options.add_options("Network")
        ("address","IP address to use", cxxopts::value<std::string>()->default_value("127.0.0.1"))
        ("numa", "NUMA node to use for memory allocation.", cxxopts::value<int>()->default_value("-1"))
        ("port", "RDMA device port to use.", cxxopts::value<uint16_t>()->default_value(std::to_string(zip::consts::DEFAULT_DEVICE_PORT)))
    ;
    options.add_options("Ziplog")
        ("client_cpus", "CPUs to run client threads.", cxxopts::value<std::vector<uint16_t>>())
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("replica_id", "ID of the replica for this storage server.", cxxopts::value<replica_t>())
        ("shard_id", "ID of the shard for this storage server.", cxxopts::value<shard_t>())
        ("subscriber_cpus", "CPUs to run subscriber threads.", cxxopts::value<std::vector<uint16_t>>())
        ("timeout", "Timeout for batching log entries to the subscriber in nanoseconds.", cxxopts::value<uint64_t>())
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
        if (   result.count("order")       == 0
            || result.count("client_cpus") == 0
            || result.count("timeout")     == 0
            || result.count("shard_id")    == 0
            || result.count("replica_id")  == 0) {
            std::cerr << options.help() << std::endl;
            std::exit(1);
        }

        // setup signal handler to stop the server
        std::signal(SIGINT, [] ([[maybe_unused]] int signal) {
            stop.store(true, std::memory_order_relaxed);
            stop.notify_one();
        });

        // start the server
        start_server(result);
    } catch (const cxxopts::exceptions::exception& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
