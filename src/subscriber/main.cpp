#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ratio>
#include <string>

#include <hdr/hdr_histogram.h>
#include <cxxopts.hpp>

#include "api/api.h"
#include "network/manager.h"
#include "subscriber/subscriber.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/util.h"

/** create a logger for this file */
static zip::util::logger logger("subscriber_main");

/** signal to stop the subscriber */
static std::atomic<bool> stop = false;

/**
 * Start the subscriber thread execution.
 */
void start_subscriber(cxxopts::ParseResult& options) {
    // get the arguments parsed
    auto dev = options["device"].as<std::string>();
    auto por = options["port"].as<uint8_t>();
    auto sid = options["subscriber_id"].as<uint64_t>();
    auto cpu = options["cpu"].as<unsigned int>();
    auto ord = options["order"].as<std::string>();
    auto seq = options["sequential"].as<bool>();
    auto ssn = options["start_gsn"].as<uint64_t>();
    auto iml = options["inter"].as<bool>();

    // pin this thread on the CPU
    zip::util::pin_thread(cpu);

    // initialise the network manager and the subscriber
    auto manager = zip::network::manager(dev, por);
    auto subscriber = zip::subscriber::subscriber(manager, ord, sid, ssn, seq);

    // initialise the state for the clients
    unsigned long num_entries = 0;
    hdr_histogram* hist; hdr_init(1, 10000, 3, &hist);
    std::chrono::high_resolution_clock::time_point start, end;

    // create the callback for the subscriber
    const auto& callback = [&] (zip::api::subscriber_log_entry& entry) {
        logger.trace("Delivered entry from client (", entry.client_id, ") with GSN ", entry.gsn);
        auto now = std::chrono::high_resolution_clock::now();
        if (iml) {
            hdr_record_value(hist, zip::util::time_in_us(now - end));
        } else {
            auto begin = *reinterpret_cast<std::chrono::high_resolution_clock::time_point*>(entry.data);
            hdr_record_value(hist, zip::util::time_in_us(now - begin));
        }
        if (num_entries++ == 0) start = now;
        end = now;
    };

    // poll until the stop signal is issued
    while (!stop.load(std::memory_order_relaxed)) {
        subscriber.poll(callback);
    }

    // calculate and print the statistics
    if (num_entries > 0) {
        auto throughput = num_entries * std::micro::den / zip::util::time_in_us(end - start);
        auto lat_50 = hdr_value_at_percentile(hist, 50);
        auto lat_99 = hdr_value_at_percentile(hist, 99);
        auto lat_999 = hdr_value_at_percentile(hist, 99.9);
        logger.info("Final statistics: throughput: ", throughput, " IOPS\tmedian latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us");
    }

    // free memory
    hdr_close(hist);
}

/**
 * Entrypoint for the subscriber.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the subscriber
    cxxopts::Options options("subscriber", "A subscriber application for Ziplog.\n");
    options.add_options()
        ("device", "RDMA device to use.", cxxopts::value<std::string>()->default_value(zip::consts::rdma::DEFAULT_DEVICE))
        ("port", "RDMA device port to use.", cxxopts::value<uint8_t>()->default_value(std::to_string(zip::consts::rdma::DEFAULT_PORT)))
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("cpu", "CPU to run the subscriber thread on.", cxxopts::value<unsigned int>())
        ("subscriber_id", "ID of the subscriber.", cxxopts::value<uint64_t>())
        ("start_gsn", "Starting GSN for the subscriber.", cxxopts::value<uint64_t>()->default_value("18446744073709551615"))
        ("sequential", "Whether to deliver entries in order.", cxxopts::value<bool>()->default_value("false"))
        ("inter", "Whether to measure inter-message latency or end-to-end latency.", cxxopts::value<bool>()->default_value("true"))
        ("h,help", "Show help.")
    ;

    try {
        auto result = options.parse(argc, argv);
        if (   result.count("help")          != 0
            || result.count("order")         == 0
            || result.count("cpu")           == 0
            || result.count("subscriber_id") == 0) {
            std::cout << options.help() << std::endl;
            std::exit(0);
        }

        // setup signal handler and start the subscriber
        std::signal(SIGINT, [] (int signal) { stop.store(true, std::memory_order_relaxed); });
        start_subscriber(result);
    } catch (const cxxopts::OptionParseException& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
