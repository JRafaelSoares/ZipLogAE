#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <ratio>
#include <set>
#include <string>
#include <vector>

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
    auto gid = options["gid"].as<int>();
    auto num = options["numa"].as<int>();
    auto sid = options["subscriber_id"].as<uint64_t>();
    auto pcs = options["polling_cpus"].as<std::vector<uint16_t>>();
    auto acs = options["application_cpus"].as<std::vector<uint16_t>>();
    auto ord = options["order"].as<std::string>();
    auto seq = options["sequential"].as<bool>();
    auto fal = options["failures"].as<unsigned long>();

    // get the CPUs set
    std::set<uint16_t> polling_cpus(pcs.begin(), pcs.end()), application_cpus(acs.begin(), acs.end());
    auto num_threads = application_cpus.size();

    // initialize the state for the clients
    std::vector<std::chrono::high_resolution_clock::time_point> start(num_threads), end(num_threads);
    std::vector<unsigned long> num_entries(num_threads);
    std::vector<hdr_histogram*> hist(num_threads);
    for (auto& h: hist) {
        hdr_init(1, 10000000, 3, &h);
    }

    // create the callback for the subscriber
    auto callback = [&] (unsigned long index, zip::api::subscriber_log_entry& entry) {
        logger.trace("Delivered entry from client (", entry.client_id, ") with GSN ", entry.gsn);
        auto now = std::chrono::high_resolution_clock::now();

        // measure the inter-message latency or the end-to-end latency
        if (entry.data_length >= sizeof(std::chrono::high_resolution_clock::time_point)) {
            auto begin = *reinterpret_cast<std::chrono::high_resolution_clock::time_point*>(entry.data);
            hdr_record_value(hist[index], zip::util::time_in_us(now - begin));
        }

        // increment the number of entries and timestamp
        if (num_entries[index]++ == 0) start[index] = now;
        end[index] = now;
    };

    // initialize the network manager and the subscriber
    auto manager = zip::network::manager(dev, por, gid, num);
    auto subscriber = zip::subscriber::subscriber(manager, polling_cpus, application_cpus, ord, sid, seq, fal, callback);

    const auto sleep_us = 10000;
    std::vector<unsigned long> old_entries(num_threads);
    std::vector<std::chrono::high_resolution_clock::time_point> old_ends(num_threads);

    // wait until asked to stop, then stop the subscriber
    // dump throughput periodically
    while (!stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
        unsigned long throughput = 0;
        for (unsigned long i = 0; i < num_threads; i++) {
            if (old_entries[i] != 0) {
                if (end[i] != old_ends[i])
                    throughput += (num_entries[i] - old_entries[i]) * std::micro::den / zip::util::time_in_us(end[i] - old_ends[i]);
            }
            old_entries[i] = num_entries[i];
            old_ends[i] = end[i];
        }
        //logger.info("Throughput: ", throughput);
    }
    subscriber.stop();

    // calculate the throughput
    unsigned long throughput = 0;
    for (unsigned long i = 0; i < num_threads; i++) {
        if (num_entries[i] > 1) {
            ZIP_ASSERT_EQ(hist[i]->total_count, static_cast<long long>(num_entries[i]), "missed some histogram entries");
            throughput += num_entries[i] * std::micro::den / zip::util::time_in_us(end[i] - start[i]);
        }
    }

    // merge the histograms
    for (unsigned long i = 1; i < num_threads; i++) {
        hdr_add(hist[0], hist[i]);
    }

    // calculate and print the statistics
    if (throughput > 0) {
        auto lat_50 = hdr_value_at_percentile(hist[0], 50);
        auto lat_99 = hdr_value_at_percentile(hist[0], 99);
        auto lat_999 = hdr_value_at_percentile(hist[0], 99.9);
        logger.info("Final statistics: throughput: ", throughput, " IOPS\tmedian latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us");
    }

    // free memory
    for (auto& h: hist) {
        hdr_close(h);
    }
}

/**
 * Entrypoint for the subscriber.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the subscriber
    cxxopts::Options options("subscriber", "A subscriber application for Ziplog.\n");
    options.set_width(std::numeric_limits<size_t>::max());
    options.add_options("Network")
        ("device", "RDMA device to use.", cxxopts::value<std::string>()->default_value(zip::consts::DEFAULT_DEVICE))
        ("gid", "RDMA device port GID index to use.", cxxopts::value<int>()->default_value(std::to_string(zip::consts::DEFAULT_GID)))
        ("numa", "NUMA node to use for memory allocation.", cxxopts::value<int>()->default_value("-1"))
        ("port", "RDMA device port to use.", cxxopts::value<uint8_t>()->default_value(std::to_string(zip::consts::DEFAULT_DEVICE_PORT)))
    ;
    options.add_options("Ziplog subscriber")
        ("application_cpus", "CPUs to run the subscriber application threads on.", cxxopts::value<std::vector<uint16_t>>())
        ("failures", "Number of failures to tolerate.", cxxopts::value<unsigned long>()->default_value("1"))
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("polling_cpus", "CPUs to run the subscriber polling threads on.", cxxopts::value<std::vector<uint16_t>>())
        ("sequential", "Whether to deliver entries in order.", cxxopts::value<bool>()->default_value("true"))
        ("subscriber_id", "ID of the subscriber.", cxxopts::value<uint64_t>())
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
        if (   result.count("order")             == 0
            || result.count("polling_cpus")      == 0
            || result.count("application_cpus")  == 0
            || result.count("subscriber_id")     == 0) {
            std::cerr << options.help() << std::endl;
            std::exit(1);
        }

        // setup signal handler and start the subscriber
        std::signal(SIGINT, [] ([[maybe_unused]] int signal) { stop.store(true, std::memory_order_relaxed); });
        start_subscriber(result);
    } catch (const cxxopts::OptionParseException& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
