#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <ratio>
#include <set>
#include <string>
#include <vector>

#include <cxxopts.hpp>
#include <hdr/hdr_histogram.h>
#include <hdr/hdr_histogram_log.h>

#include <zip/api/api.h>
#include <zip/network/buffer.h>
#include <zip/network/manager.h>
#include <zip/client/client.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/util.h>

#include "zip/network/erpc_constants.h"

/** create a logger for this file */
static zip::util::logger logger("client");

/**
 * This struct contains the data for a single
 * request.
 */
struct request {

    /** start time of the request */
    std::chrono::high_resolution_clock::time_point begin;

    /** location of the response */
    std::atomic<gsn_t> response;

};

/**
 * Start the client.
 */
void start_client(cxxopts::ParseResult& options) {
    // get the arguments parsed
    auto addr = options["address"].as<std::string>();
    auto por = options["port"].as<uint16_t>();
    auto num = options["numa"].as<int>();

    auto cid = options["client_id"].as<client_t>();
    auto sid = options["shard_id"].as<shard_t>();
    auto dur = options["duration"].as<uint32_t>();
    auto ord = options["order"].as<std::string>();
    auto bur = options["burst"].as<uint32_t>();
    auto siz = options["size"].as<uint32_t>();
    auto fal = options["failures"].as<uint32_t>();
    auto min = options["min_fraction"].as<double>();
    auto max = options["max_fraction"].as<double>();
    auto ccu = options["client_cpu"].as<uint16_t>();
    auto bcu = options["bench_cpu"].as<uint16_t>();
    auto srv = options["servers"].as<std::vector<std::string>>();

    // verify arguments for correctness
    ZIP_ASSERT(0 < bur && bur <= zip::consts::MAX_OUTSTANDING, "invalid value of burst");

    // initialize the network manager and the client
    auto manager = zip::network::manager(num);
    auto transport = std::make_unique<zip::network::erpc_transport_factory>(manager, zip::network::CLIENT_SERVER_OFFSET, addr, por);

    auto servers = std::set(srv.begin(), srv.end());
    auto client = zip::client::client(manager, *transport, ord, cid, sid, fal, min, max, ccu, servers, addr, por);

    // pin this thread on the other CPU
    zip::util::pin_thread(bcu);

    // run the benchmark for the specified duration
    auto duration = std::chrono::microseconds(dur * std::micro::den);

    // initialize the histogram
    hdr_histogram* hist;
    hdr_init(1, 100000000000, 3, &hist);
    uint32_t total_sent = 0, total_received = 0;

    // create variables for in-flight requests
    auto requests = std::make_unique<request[]>(bur);

    // allocate buffers for the request
    auto length = std::min(siz, zip::consts::MAX_PAYLOAD);
    auto buffers = manager.get_buffers(zip::consts::MAX_OUTSTANDING);
    auto iterator = zip::util::wraparound_iterator(buffers);

    // keep track of when to end the experiment
    auto start = std::chrono::high_resolution_clock::now(), finish = start + duration, last = start;

    // generate and send a given request with the given timestamp
    auto generate = [&] (std::chrono::high_resolution_clock::time_point now) {
        // initialize the next request
        auto& request = requests[total_sent % bur];
        auto& buffer = iterator.get_and_increment();
        auto& message = buffer->as<zip::api::storage_append>();
        message.data_length = length;
        request.begin = now;

        // send the request
        client.append(*buffer, request.response);
        total_sent++;
    };

    // send the first burst of requests
    for (uint32_t i = 0; i < bur; i++) {
        generate(std::chrono::high_resolution_clock::now());
    }

    // loop while we have not gotten a response for all requests
    while (total_received < total_sent) {
        // wait until the next request has been delivered
        gsn_t gsn;
        auto& request = requests[total_received % bur];
        while ((gsn = request.response.load(std::memory_order_relaxed)) == std::numeric_limits<gsn_t>::max()) zip::util::relax();
        total_received++;

        // record the latency of the request
        last = std::chrono::high_resolution_clock::now();
        auto ns = zip::util::time_in_ns(last - request.begin);
        hdr_record_value(hist, ns);
        logger.trace("Received ACK for request in ", ns, " us with GSN ", gsn);

        // if we're still in the benchmark phase then send the next request
        if (last < finish) generate(last);
    }

    // stop the client
    client.stop();

    // find and print the statistics
    auto throughput = total_received * std::nano::den / zip::util::time_in_ns(last - start);
    std::cout << "[append_bench]: write throughput " << throughput << " ops/sec" << std::endl;

    std::cout << "[append_bench]: latency metrics " << std::endl;
    hdr_percentiles_print(hist, stdout, 5, 1, CLASSIC);
    std::cout << "[append_bench]: percentile latencies " << std::endl
              << "\tp50: " << hdr_value_at_percentile(hist, 50.0) << std::endl
              << "\tp95: " << hdr_value_at_percentile(hist, 95.0) << std::endl
              << "\tp99: " << hdr_value_at_percentile(hist, 99.0) << std::endl
              << "\tp99.9: " << hdr_value_at_percentile(hist, 99.9) << std::endl;


    //auto lat_50 = hdr_value_at_percentile(hist, 50);
    //auto lat_99 = hdr_value_at_percentile(hist, 99);
    //auto lat_999 = hdr_value_at_percentile(hist, 99.9);
    //logger.info("Final statistics: throughput: ", throughput, " IOPS\tmedian latency: ", lat_50, " ns\t99% latency: ", lat_99, " ns\t99.9% latency: ", lat_999, " ns");

    // dump the historgam as a base64 encoded string
    //char* dump; hdr_log_encode(hist, &dump);
    //logger.info("Histogram: ", dump);

    // free memory
    hdr_close(hist);
    //std::free(dump);
}

/**
 * Entrypoint for the client.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the client
    cxxopts::Options options("client", "A client benchmark for Ziplog.\n");
    options.set_width(std::numeric_limits<size_t>::max());
    options.add_options("Network")
        ("address","IP address to use", cxxopts::value<std::string>()->default_value("127.0.0.1"))
        ("numa", "NUMA node to use for memory allocation.", cxxopts::value<int>()->default_value("-1"))
        ("port", "RDMA device port to use.", cxxopts::value<uint16_t>()->default_value(std::to_string(zip::consts::DEFAULT_DEVICE_PORT)))
    ;
    options.add_options("Client")
        ("bench_cpu", "CPU for running the benchmark.", cxxopts::value<uint16_t>())
        ("burst", "Maximum number of concurrent requests.", cxxopts::value<uint32_t>()->default_value("1"))
        ("duration", "Time to run the experiment for in seconds.", cxxopts::value<uint32_t>()->default_value("10"))
        ("size", "Size of the payload in bytes.", cxxopts::value<uint32_t>()->default_value("8"))
    ;
    options.add_options("Ziplog")
        ("client_cpu", "CPU for running the client.", cxxopts::value<uint16_t>())
        ("client_id", "ID of the client.", cxxopts::value<client_t>())
        ("cpus", "CPUs for running the client.", cxxopts::value<std::vector<uint16_t>>())
        ("failures", "Number of failures to tolerate.", cxxopts::value<uint32_t>()->default_value("1"))
        ("max_fraction", "Minimum fraction of used slots to trigger decrease.", cxxopts::value<double>()->default_value("0.99"))
        ("min_fraction", "Maximum fraction of used slots to trigger increase.", cxxopts::value<double>()->default_value("0.95"))
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("servers", "Addresses of the Ziplog storage servers.", cxxopts::value<std::vector<std::string>>())
        ("shard_id", "ID of the shard for this client.", cxxopts::value<shard_t>())
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
        if (   result.count("order")      == 0
            || result.count("bench_cpu")  == 0
            || result.count("client_cpu") == 0
            || result.count("client_id")  == 0
            || result.count("shard_id")   == 0
            || result.count("servers")    == 0) {
            std::cerr << options.help() << std::endl;
            std::exit(1);
        }

        // start the main client program
        start_client(result);
    } catch (const cxxopts::exceptions::exception& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
