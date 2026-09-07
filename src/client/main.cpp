#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <ratio>
#include <string>

#include <cxxopts.hpp>
#include <hdr/hdr_histogram.h>

#include "api/api.h"
#include "client/client.h"
#include "network/buffer.h"
#include "network/manager.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/util.h"

/** create a logger for this file */
static zip::util::logger logger("client_main");

/**
 * Run the test with the client.
 */
void run_test(
    zip::network::manager& manager,
    zip::client::client& client,
    std::chrono::microseconds duration,
    std::chrono::microseconds warmup,
    unsigned long burst,
    unsigned long size
) {
    // initialise the histogram
    hdr_histogram* hist;
    hdr_init(1, 10000, 3, &hist);
    unsigned long total_requests = 0;

    // create variables for in-flight requests
    auto requests = std::make_unique<zip::client::client::request[]>(burst);
    unsigned long head = 0, tail = 0;

    // allocate buffers for the request
    auto length = sizeof(zip::api::storage_insert_after)
        + std::min(size + sizeof(std::chrono::high_resolution_clock::time_point), zip::consts::MAX_PAYLOAD);
    auto buffers = manager.get_buffers(length, zip::consts::rdma::MAX_OUTSTANDING_REQUESTS);
    auto iterator = zip::util::wraparound_iterator(buffers);

    // keep track of when to end the experiment
    auto now = std::chrono::high_resolution_clock::now(),
         finish = now + warmup + duration,
         measure = now + warmup;
    bool started = false;

    // loop while we have time
    while (true) {
        // if we have outstanding requests then check if one has
        // delivered, and measure the latency
        if (head != tail) {
            auto& request = requests[head % burst];
            if (request.response.load(std::memory_order_relaxed) != -1) {
                if (started || (started = (request.begin > measure))) {
                    auto end = std::chrono::high_resolution_clock::now();
                    auto us = zip::util::time_in_us(end - request.begin);
                    logger.trace("Received ACK for request in ", us, " us.");
                    hdr_record_value(hist, us);
                    total_requests++;

                    // if we have finished the experiment
                    // then exit from the loop
                    if (end > finish) {
                        break;
                    }
                }
                head++;
            }
        }

        // if we have more slots for outstanding requests then send one
        if (tail - head != burst) {
            // generate the request
            auto current = iterator.get_and_increment();
            auto& req = current->as<zip::api::storage_insert_after>();
            req.data_length = length;
            req.gsn_after = -1;

            // set the request parameters
            auto& request = requests[tail++ % burst];
            request.buffer = &(*current);

            // send the request with the timestamp
            auto& begin = *reinterpret_cast<std::chrono::high_resolution_clock::time_point*>(req.data);
            request.begin = begin = std::chrono::high_resolution_clock::now();
            client.insert_after(request);
        }
    }

    // find and print the statistics
    auto throughput = total_requests * std::micro::den / zip::util::time_in_us(duration);
    auto lat_50 = hdr_value_at_percentile(hist, 50);
    auto lat_99 = hdr_value_at_percentile(hist, 99);
    auto lat_999 = hdr_value_at_percentile(hist, 99.9);
    logger.info("Final statistics: throughput: ", throughput, " IOPS\tmedian latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us");

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
    auto cid = options["client_id"].as<uint64_t>();
    auto cpu = options["cpu"].as<unsigned int>();
    auto dur = options["duration"].as<float>();
    auto war = options["warmup"].as<float>();
    auto ord = options["order"].as<std::string>();
    auto rat = options["rate"].as<unsigned long>();
    auto sid = options["shard_id"].as<uint64_t>();
    auto bur = options["burst"].as<unsigned long>();
    auto siz = options["size"].as<unsigned long>();

    // initialise the network manager and the client
    auto manager = zip::network::manager(dev, por, gid);
    auto client = zip::client::client(manager, ord, cid, sid, cpu, rat);

    // run the test for the specified duration
    auto duration = std::chrono::microseconds(static_cast<unsigned long>(dur * std::micro::den));
    auto warmup = std::chrono::microseconds(static_cast<unsigned long>(war * std::micro::den));
    run_test(manager, client, duration, warmup, bur, siz);
}

/**
 * Entrypoint for the client.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the client
    cxxopts::Options options("client", "A client application for Ziplog.\n");
    options.add_options()
        ("device", "RDMA device to use.", cxxopts::value<std::string>()->default_value(zip::consts::rdma::DEFAULT_DEVICE))
        ("port", "RDMA device port to use.", cxxopts::value<uint8_t>()->default_value(std::to_string(zip::consts::rdma::DEFAULT_PORT)))
        ("gid", "RDMA device port GID index to use.", cxxopts::value<int>()->default_value(std::to_string(zip::consts::rdma::DEFAULT_GID)))
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("cpu", "CPU to run the client thread on.", cxxopts::value<unsigned int>())
        ("client_id", "ID of the client.", cxxopts::value<uint64_t>())
        ("shard_id", "ID of the shard.", cxxopts::value<uint64_t>())
        ("duration", "Time to run the experiment for in seconds.", cxxopts::value<float>())
        ("warmup", "Warm-up duration in seconds.", cxxopts::value<float>())
        ("rate", "IOPS rate for the experiment.", cxxopts::value<unsigned long>())
        ("burst", "Maximum number of concurrent requests.", cxxopts::value<unsigned long>()->default_value("1"))
        ("size", "Average size of the request in bytes.", cxxopts::value<unsigned long>()->default_value("0"))
        ("h,help", "Show help.")
    ;

    try {
        auto result = options.parse(argc, argv);
        if (   result.count("help")      != 0
            || result.count("order")     == 0
            || result.count("cpu")       == 0
            || result.count("rate")      == 0
            || result.count("client_id") == 0
            || result.count("shard_id")  == 0
            || result.count("duration")  == 0
            || result.count("warmup")    == 0) {
            std::cout << options.help() << std::endl;
            std::exit(0);
        }
        if (result["client_id"].as<uint64_t>() >= zip::consts::MAX_CLIENTS) {
            std::cerr << "Invalid client_id, should be between 0 and " << (zip::consts::MAX_CLIENTS - 1) << std::endl;
            std::exit(0);
        }
        if (result["burst"].as<unsigned long>() == 0) {
            std::cerr << "Invalid burst, should be greater than 0" << std::endl;
            std::exit(0);
        }

        // start the main client program
        start_client(result);
    } catch (const cxxopts::OptionParseException& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
