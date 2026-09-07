#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <netinet/in.h>

#include <cxxopts.hpp>

#include "api/api.h"
#include "network/manager.h"
#include "order/order.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/switch.h"
#include "util/util.h"

/** create a logger for this file */
static zip::util::logger logger("order_main");

/** signal to stop the ordering server */
static std::atomic<bool> stop = false;

/**
 * Start the ordering server thread execution.
 */
void start_order(cxxopts::ParseResult& options) {
    // get the arguments parsed
    auto dev = options["device"].as<std::string>();
    auto por = options["port"].as<uint8_t>();
    auto gid = options["gid"].as<int>();
    auto cpu = options["cpu"].as<unsigned int>();
    auto srv = options["server"].as<unsigned int>();

    // create the struct which is ordering server's first message
    zip::api::order_intro req;
    req.message_type = zip::api::ORDER_INTRO;

    // initialise the network manager and the ordering server
    auto manager = zip::network::manager(dev, por, gid);
    auto order = zip::order::order(manager, cpu);

    // bind the manager and start listening for connections
    manager.bind_server(srv);
    logger.info("Ordering server initialised");

    // in an endless loop wait to connect to the storage
    // servers, clients, and subscribers
    while (!stop.load(std::memory_order_relaxed)) {
        // try to accept a connection and exchange the first message
        auto result = manager.accept(&req, req.length()); if (!result) continue;
        auto& [send_queue, buffer, length, address] = *result;

        // add the storage server, the client or the subscriber
        zip::util::apply(logger, buffer.get(),
            [&, &send_queue=send_queue] (zip::api::client_intro& intro) {
                // add the client to the state
                order.add_client(intro.client_id, intro.shard_id, intro.num_slots, std::move(send_queue));
            },
            [&, &send_queue=send_queue] (zip::api::subscriber_intro& intro) {
                // add the subscriber to the state
                order.add_subscriber(intro.subscriber_id, std::move(send_queue));
            },
            [&, &address=address, &send_queue=send_queue] (zip::api::storage_intro& intro) {
                // add the storage server to the state
                address.sin_port = htons(intro.port);
                order.add_storage(
                    intro.shard_id,
                    intro.replica_id,
                    intro.client_queues,
                    intro.subscriber_queues,
                    address,
                    std::move(send_queue)
                );
            }
        );
    }
}

/**
 * Entrypoint for the ordering server.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the ordering server
    cxxopts::Options options("order", "The ordering server for Ziplog.\n");
    options.add_options()
        ("device", "RDMA device to use.", cxxopts::value<std::string>()->default_value(zip::consts::rdma::DEFAULT_DEVICE))
        ("port", "RDMA device port to use.", cxxopts::value<uint8_t>()->default_value(std::to_string(zip::consts::rdma::DEFAULT_PORT)))
        ("gid", "RDMA device port GID index to use.", cxxopts::value<int>()->default_value(std::to_string(zip::consts::rdma::DEFAULT_GID)))
        ("server", "Server port to listen on for connection.", cxxopts::value<unsigned int>()->default_value(std::to_string(zip::consts::DEFAULT_PORT)))
        ("cpu", "CPU to run the ordering service thread on.", cxxopts::value<unsigned int>())
        ("h,help", "Show help.")
    ;

    try {
        auto result = options.parse(argc, argv);
        if (   result.count("help") != 0
            || result.count("cpu")  == 0) {
            std::cout << options.help() << std::endl;
            std::exit(0);
        }

        // setup signal handler and start the ordering server
        std::signal(SIGINT, [] (int signal) { std::cerr << "SIGINT caught\n"; stop.store(true, std::memory_order_relaxed); });
        start_order(result);
    } catch (const cxxopts::OptionParseException& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
