#include <atomic>
#include <algorithm>
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
#include "lock_service/lock_service.h"
#include "network/manager.h"
#include "util/consts.h"
#include "util/log.h"
#include "util/util.h"

/** create a logger for this file */
static zip::util::logger logger("lock_main");

/** signal to stop the subscriber */
static std::atomic<bool> stop = false;

zip::lock_service::lock_service init_lock_service(
    const cxxopts::ParseResult& options, size_t num_clients, size_t num_keys,
    zip::network::manager& manager, zip::network::manager& manager2) {
    // initialize the ziplog client
    auto cid = options["client_id"].as<uint64_t>();
    auto cpu = options["cpu"].as<uint16_t>();
    auto ord = options["order"].as<std::string>();
    auto sid = options["shard_id"].as<uint64_t>();
    auto fal = options["failures"].as<unsigned long>();
    auto rat = options.count("rate") == 0 ? 0 : options["rate"].as<unsigned long>();
    
    // initialize the subscriber
    auto sub_id = options["subscriber_id"].as<uint64_t>();
    auto pcs = options["polling_cpus"].as<std::vector<uint16_t>>();
    auto acs = options["application_cpus"].as<std::vector<uint16_t>>();

    // get the CPUs set
    std::set<uint16_t> polling_cpus(pcs.begin(), pcs.end()), application_cpus(acs.begin(), acs.end());
    return zip::lock_service::lock_service(num_clients, num_keys, manager, manager2, ord, cid, sid, cpu, rat, fal, polling_cpus, application_cpus, sub_id);
}

std::vector<std::array<hdr_histogram*, 2>> hist;
std::vector<std::array<std::vector<uint64_t>, 2>> lats;

static void worker(
    zip::lock_service::lock_service& lock, int cid,
    //zip::lock_service::lock_service& lock, std::array<hdr_histogram*, 2>& hist, int cid,
    size_t num_keys, uint64_t time) {
    // initialize the state for the clients
#ifdef USE_HDR
    //std::array<hdr_histogram*, 2> hist;
    //for (auto& h : hist)
    //    hdr_init(1, 10000000, 3, &h);
#else
    //std::array<std::vector<uint64_t>, 2> lats;
#endif

    auto start = std::chrono::high_resolution_clock::now();
    int i = 0;
    while (!stop.load(std::memory_order_relaxed)) {
        // TODO: choose key
        auto key = cid % num_keys;
        auto t1 = std::chrono::high_resolution_clock::now();
        lock.acquire(cid, key);
        auto t2 = std::chrono::high_resolution_clock::now();
        lock.release(cid, key);
        auto t3 = std::chrono::high_resolution_clock::now();
        auto acquire_lat = zip::util::time_in_us(t2 - t1);
        auto release_lat = zip::util::time_in_us(t3 - t2);
        //logger.info("acquire_lat=", acquire_lat, ", release_lat=", release_lat);
#ifdef USE_HDR
        hdr_record_value(hist[cid][0], acquire_lat);
        hdr_record_value(hist[cid][1], release_lat);
#else
        lats[0][0].push_back(acquire_lat);
        lats[0][1].push_back(release_lat);
#endif

        //if (++i % 10 == 1)
        //    logger.info("client-", cid, ", lat=", acquire_lat, ", ", release_lat);
        if (zip::util::time_in_us(t3 - start) > time * 1000000) {
            logger.info("client-", cid, " finish");
            break;
        }
    }

//#ifdef USE_HDR
//    for (auto& h : hist) {
//        std::string action = (&h == &h.front()) ? "acquire" : "release";
//        auto lat_50 = hdr_value_at_percentile(h, 50);
//        auto lat_99 = hdr_value_at_percentile(h, 99);
//        auto lat_999 = hdr_value_at_percentile(h, 99.9);
//        logger.info("Final statistics: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us");
//    }
//#else
//    for (int i = 0; i < lats.size(); ++i) {
//        std::string action = (i == 0) ? "acquire" : "release";
//        std::sort(lats[i][cid].begin(), lats[i].end());
//        auto leng = lats[i][cid].size();
//        auto lat_50 = lats[i][cid][int(0.5 * leng)];
//        auto lat_99 = lats[i][cid][int(0.99 * leng)];
//        auto lat_999 = lats[i][cid][int(0.999 * leng)];
//        logger.info("Final statistics [", action, "]: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us");
//    }
//#endif
}

void run(const cxxopts::ParseResult& options) {
    // initialize the network manager
    auto dev = options["device"].as<std::string>();
    auto por = options["port"].as<uint8_t>();
    auto gid = options["gid"].as<int>();
    auto num = options["numa"].as<int>();
    auto cid = options["client_id"].as<uint64_t>();
    auto num_clients = options["num_clients"].as<uint64_t>();
    auto num_keys = options["num_lock_keys"].as<uint64_t>();
    auto time = options["time"].as<uint64_t>();
    auto manager = zip::network::manager(dev, por, gid, num); 
    auto manager2 = zip::network::manager(dev, por, gid, num); 

    auto lock = init_lock_service(options, num_clients, num_keys, manager, manager2);

    // Each client has two histogram, one for acquire and another for release.
    //std::vector<std::array<hdr_histogram*, 2>> hist(num_clients);
    for (size_t i = 0; i < num_clients; ++i) {
        hist.emplace_back(std::array<hdr_histogram*, 2>());
        for (auto& h : hist.back())
            ZIP_ASSERT(hdr_init(1, 10000000, 3, &h) == 0, "hdr init error");

        lats.emplace_back(std::array<std::vector<uint64_t>, 2>());
    }


    worker(lock, cid, num_keys, time);
    //worker(lock, hist[0], 0, 3);
    //std::vector<std::thread> threads;
    //for (size_t cid = 0; cid < num_clients; ++cid)
    //    threads.emplace_back(&worker, std::ref(lock), cid, num_keys, time);
    //    //threads.emplace_back(&worker, std::ref(lock), std::ref(hist[cid]), cid, num_keys, time);
    //for (auto& t : threads)
    //    t.join();

    lock.exit();

#ifdef USE_HDR
    // Merge histograms: fold all clients into hist[0]
    for (unsigned long i = 1; i < num_clients; i++) {
        for (int j = 0; j < 2; j++) {
            auto lat_50 = hdr_value_at_percentile(hist[i][j], 50);
            auto lat_99 = hdr_value_at_percentile(hist[i][j], 99);
            auto lat_999 = hdr_value_at_percentile(hist[i][j], 99.9);
            logger.info("Final statistics: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us");
            if (hdr_add(hist[0][j], hist[i][j]) != 0) {
                throw std::runtime_error("hdr_add failed");
            }
        }
    }

    for (auto& h : hist[0]) {
        std::string action = (&h == &hist[0][0]) ? "acquire" : "release";
        auto lat_50 = hdr_value_at_percentile(h, 50);
        auto lat_99 = hdr_value_at_percentile(h, 99);
        auto lat_999 = hdr_value_at_percentile(h, 99.9);
        logger.info("Final statistics: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us");
    }

    // Free histograms
    for (auto& hdrs : hist) {
        for (auto& h : hdrs) {
            hdr_close(h);
        }
    }
#else
    // Merge histograms: fold all clients into hist[0]
    std::array<std::vector<uint64_t>, 2> all_lats;
    for (unsigned long i = 0; i < num_clients; i++) {
        for (int j = 0; j < 2; j++) {
            all_lats[j].insert(all_lats[j].end(), lats[i][j].begin(), lats[i][j].end());
        }
    }

    for (size_t i = 0; i < all_lats.size(); ++i) {
        std::string action = (i == 0) ? "acquire" : "release";
        std::sort(all_lats[i].begin(), all_lats[i].end());
        auto leng = all_lats[i].size();
        auto lat_50 = all_lats[i][int(0.5 * leng)];
        auto lat_99 = all_lats[i][int(0.99 * leng)];
        auto lat_999 = all_lats[i][int(0.999 * leng)];
        logger.info("Final statistics [", action, "]: median latency: ", lat_50, " us\t99% latency: ", lat_99, " us\t99.9% latency: ", lat_999, " us");
    }
#endif
}

/**
 * Entrypoint for the subscriber.
 */
int main(int argc, char* argv[]) {
    // create and parse options
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
        ("failures", "Number of failures to tolerate.", cxxopts::value<unsigned long>()->default_value("1"))
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("shard_id", "ID of the shard.", cxxopts::value<uint64_t>())
    ;
    options.add_options("Rate estimation")
        ("rate", "Fixed rate to run the client with in IOPS.", cxxopts::value<unsigned long>())
        ("target", "Target fraction of used slots for PID rate estimation.", cxxopts::value<double>()->default_value("0.9"))
        ("proportional", "Proportional constant for PID rate estimation.", cxxopts::value<double>()->default_value("500"))
        ("integral", "Integral constant for PID rate estimation.", cxxopts::value<double>()->default_value("40"))
        ("derivative", "Derivative for PID rate estimation.", cxxopts::value<double>()->default_value("30"))
    ;
    options.add_options("Ziplog subscriber")
        ("application_cpus", "CPUs to run the subscriber application threads on.", cxxopts::value<std::vector<uint16_t>>())
        ("polling_cpus", "CPUs to run the subscriber polling threads on.", cxxopts::value<std::vector<uint16_t>>())
        ("sequential", "Whether to deliver entries in order.", cxxopts::value<bool>()->default_value("false"))
        ("subscriber_id", "ID of the subscriber.", cxxopts::value<uint64_t>())
    ;
    options.add_options("Serializable log")
        ("num_clients", "Total number of clients.", cxxopts::value<unsigned long>())
        ("num_lock_keys", "Total number of lock keys.", cxxopts::value<unsigned long>()->default_value("10"))
        ("time", "Execution time.", cxxopts::value<unsigned long>()->default_value("10"))
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
            || result.count("application_cpus")  == 0) {
            std::cerr << options.help() << std::endl;
            std::exit(1);
        }

        // setup signal handler and start the subscriber
        std::signal(SIGINT, [] ([[maybe_unused]] int signal) { std::cout << "yo\n"; stop.store(true, std::memory_order_relaxed); });
        run(result);
    } catch (const cxxopts::OptionParseException& x) {
        std::cerr << "Error parsing program arguments: " << x.what() << std::endl;
        std::cerr << options.help() << std::endl;
        std::exit(1);
    }

    return 0;
}
