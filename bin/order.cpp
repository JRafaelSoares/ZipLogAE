#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <string>

#include <cxxopts.hpp>

#include <zip/network/manager.h>
#include <zip/order/order.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/network/erpc_constants.h>

/** create a logger for this file */
static zip::util::logger logger("order_main");

/** signal to stop the ordering server */
static std::atomic<bool> stop = false;

/**
 * Start the ordering server.
 */
void start_order(cxxopts::ParseResult& options) {
    // get the arguments parsed
    auto addr = options["address"].as<std::string>();
    auto por = options["port"].as<uint16_t>();
    auto num = options["numa"].as<int>();
    auto cpu = options["cpu"].as<uint16_t>();

    // initialize the network manager and the ordering server
    auto manager = zip::network::manager(num);
    auto transport = std::make_unique<zip::network::erpc_transport_factory>(manager, zip::network::ORDER_SERVER_OFFSET, addr, por);

    auto order = zip::order::order(manager, *transport, cpu);

    // wait for the ordering server to be killed
    stop.wait(false, std::memory_order_relaxed);
}

/**
 * Entrypoint for the ordering server.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the ordering server
    cxxopts::Options options("order", "The ordering server for Ziplog.\n");
    options.set_width(std::numeric_limits<size_t>::max());
    options.add_options("Network")
        ("address","IP address to use", cxxopts::value<std::string>()->default_value("127.0.0.1"))
        ("numa", "NUMA node to use for memory allocation.", cxxopts::value<int>()->default_value("-1"))
        ("port", "eRPC UDP port to use.", cxxopts::value<uint16_t>()->default_value(std::to_string(zip::consts::DEFAULT_SERVER_PORT)))
    ;
    options.add_options("Ziplog")
        ("cpu", "CPU for running the ordering server.", cxxopts::value<uint16_t>())
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
        if (result.count("cpu") == 0) {
            std::cerr << options.help() << std::endl;
            std::exit(1);
        }

        // setup signal handler to stop the ordering server
        std::signal(SIGINT, [] ([[maybe_unused]] int signal) {
            stop.store(true, std::memory_order_relaxed);
            stop.notify_one();
        });

        // start the ordering server
        start_order(result);
    } catch (const cxxopts::exceptions::exception& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
