#include <algorithm>
#include <atomic>
#include <chrono>
#include <compare>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <ratio>
#include <string>
#include <vector>

#include <cxxopts.hpp>
#include <hdr/hdr_histogram.h>
#include <hdr/hdr_histogram_log.h>

#include "api/api.h"
#include "client/client.h"
#include "network/buffer.h"
#include "network/manager.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/util.h"

/** create a logger for this file */
static zip::util::logger logger("client_main");

/** signal to crash the client */
static std::atomic<bool> force_stop = false;

/**
 * This struct contains the data for a single
 * request.
 */
struct request {

    /** start time of the request */
    std::chrono::high_resolution_clock::time_point begin;

    /** location of the response */
    std::atomic<uint64_t> response;
};

/**
 * Run the test with the client.
 */
void run_test(
    zip::network::manager& manager,
    zip::client::client& client,
    std::chrono::microseconds duration,
    unsigned long burst,
    unsigned long size
) {
    // verify arguments for correctness
    ZIP_ASSERT(burst > 0 && burst <= zip::consts::rdma::MAX_OUTSTANDING_REQUESTS, "invalid value of burst");

    // initialize the histogram
    hdr_histogram* hist;
    hdr_init(1, 10000000, 3, &hist);
    unsigned long total_requests = 0;

    // create variables for in-flight requests
    auto requests = std::make_unique<request[]>(burst);

    // allocate buffers for the request
    auto length = std::min(size, zip::consts::MAX_PAYLOAD);
    auto buffers = manager.get_buffers(zip::consts::rdma::MAX_OUTSTANDING_REQUESTS + 1);
    auto iterator = zip::util::wraparound_iterator(buffers);

    // keep track of when to end the experiment
    auto finish = std::chrono::high_resolution_clock::now() + duration;

    // generate and send a given request with the given timestamp
    auto generate_and_send = [&] (request& request, std::chrono::high_resolution_clock::time_point now) {
        // get the next buffer
        auto buffer = iterator.get_and_increment();
        auto& req = buffer->as<zip::api::storage_insert>();
        req.data_length = length;

        // put the timestamp in the request if needed
        auto& begin = *reinterpret_cast<std::chrono::high_resolution_clock::time_point*>(req.data);
        if (length >= sizeof(std::chrono::high_resolution_clock::time_point)) begin = now;
        request.begin = now;

        // send the request
        client.insert(*buffer, request.response);
    };

    // send the first burst of requests and then send one
    // every time one of them finished
    for (unsigned long i = 0; i < burst; i++) {
        generate_and_send(requests[i], std::chrono::high_resolution_clock::now());
    }

    // loop while we have time
    while (true) {
        // get the current time and check if we need to quit
        auto now = std::chrono::high_resolution_clock::now();
        if (now > finish) break;
        if (force_stop.load(std::memory_order_relaxed)) {
            client.stop(/* trigger_failure */true);
            return;
        }

        // if we have outstanding requests then check if one has
        // delivered, and measure the latency
        auto& request = requests[total_requests % burst];
        if (request.response.load(std::memory_order_relaxed) != std::numeric_limits<uint64_t>::max()) {
            // record the latency of the request and send the next one
            auto us = zip::util::time_in_us(now - request.begin);
            logger.trace("Received ACK for request in ", us, " us.");
            generate_and_send(request, now);
            hdr_record_value(hist, us);
            total_requests++;
        }
    }

    // stop the client
    client.stop();

    // find and print the statistics
    auto throughput = total_requests * std::micro::den / zip::util::time_in_us(duration);
    auto lat_50 = hdr_value_at_percentile(hist, 50);
    auto lat_99 = hdr_value_at_percentile(hist, 99);
    auto lat_999 = hdr_value_at_percentile(hist, 99.9);
    logger.info("Final statistics: throughput: ", throughput, " IOPS\tmedian latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us");

    // dump the historgam as a base64 encoded string
    char* dump; hdr_log_encode(hist, &dump);
    logger.info("Histogram: ", dump);

    // free memory
    hdr_close(hist);
}

/**
 * Start the client thread execution.
 */
void start_client(cxxopts::ParseResult& options) {
    // get the arguments parsed
    auto dev = options["device"].as<std::string>();
    auto por = options["port"].as<uint8_t>();
    auto gid = options["gid"].as<int>();
    auto num = options["numa"].as<int>();
    auto cid = options["client_id"].as<uint64_t>();
    auto cpu = options["cpu"].as<uint16_t>();
    auto dur = options["duration"].as<unsigned long>();
    auto ord = options["order"].as<std::string>();
    auto sid = options["shard_id"].as<uint64_t>();
    auto bur = options["burst"].as<unsigned long>();
    auto siz = options["size"].as<unsigned long>();
    auto fal = options["failures"].as<unsigned long>();
    auto tgt = options["target"].as<double>();
    auto prc = options["proportional"].as<double>();
    auto inc = options["integral"].as<double>();
    auto dec = options["derivative"].as<double>();
    auto rat = options.count("rate") == 0 ? 0 : options["rate"].as<unsigned long>();

    // initialise the parameters for PID
    auto pid = zip::client::pid { tgt, prc, inc, dec };

    // initialize the network manager and the client
    auto manager = zip::network::manager(dev, por, gid, num);
    auto client = rat == 0 ? zip::client::client(manager, ord, cid, sid, cpu, pid, fal)
                           : zip::client::client(manager, ord, cid, sid, cpu, rat, fal);

    // run the test for the specified duration
    auto duration = std::chrono::microseconds(dur * std::micro::den);
    run_test(manager, client, duration, bur, siz);
}

/**
 * Entrypoint for the client.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the client
    cxxopts::Options options("client", "A client application for Ziplog.\n");
    options.set_width(std::numeric_limits<size_t>::max());
    options.add_options("Network")
        ("device", "RDMA device to use.", cxxopts::value<std::string>()->default_value(zip::consts::DEFAULT_DEVICE))
        ("gid", "RDMA device port GID index to use.", cxxopts::value<int>()->default_value(std::to_string(zip::consts::DEFAULT_GID)))
        ("numa", "NUMA node to use for memory allocation.", cxxopts::value<int>()->default_value("-1"))
        ("port", "RDMA device port to use.", cxxopts::value<uint8_t>()->default_value(std::to_string(zip::consts::DEFAULT_DEVICE_PORT)))
    ;
    options.add_options("Ziplog client")
        ("burst", "Maximum number of concurrent requests.", cxxopts::value<unsigned long>()->default_value("1"))
        ("client_id", "ID of the client.", cxxopts::value<uint64_t>())
        ("cpu", "CPU to run the client thread on.", cxxopts::value<uint16_t>())
        ("duration", "Time to run the experiment for in seconds.", cxxopts::value<unsigned long>())
        ("failures", "Number of failures to tolerate.", cxxopts::value<unsigned long>()->default_value("1"))
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("shard_id", "ID of the shard.", cxxopts::value<uint64_t>())
        ("size", "Average size of the request in bytes.", cxxopts::value<unsigned long>()->default_value("8"))
    ;
    options.add_options("Rate estimation")
        ("rate", "Fixed rate to run the client with in IOPS.", cxxopts::value<unsigned long>())
        ("target", "Target fraction of used slots for PID rate estimation.", cxxopts::value<double>()->default_value("0.9"))
        ("proportional", "Proportional constant for PID rate estimation.", cxxopts::value<double>()->default_value("500"))
        ("integral", "Integral constant for PID rate estimation.", cxxopts::value<double>()->default_value("40"))
        ("derivative", "Derivative for PID rate estimation.", cxxopts::value<double>()->default_value("30"))
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
        if (   result.count("order")     == 0
            || result.count("cpu")       == 0
            || result.count("client_id") == 0
            || result.count("shard_id")  == 0
            || result.count("duration")  == 0) {
            std::cerr << options.help() << std::endl;
            std::exit(1);
        }

        // crash the client
        std::signal(SIGINT, [] ([[maybe_unused]] int signal) {
            logger.info("kill client");
            force_stop.store(true, std::memory_order_relaxed);
        });

        // start the main client program
        start_client(result);
    } catch (const cxxopts::OptionParseException& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
