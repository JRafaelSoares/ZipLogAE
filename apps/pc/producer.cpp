//
// Created by jrsoares on 10/05/26.
//
#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <cxxopts.hpp>

#include <zip/api/api.h>
#include <zip/network/manager.h>
#include <zip/subscriber/log.h>
#include <zip/subscriber/subscriber.h>
#include <zip/util/consts.h>
#include <zip/util/logger.h>
#include <zip/util/switch.h>
#include <zip/util/util.h>

#include "hdr/hdr_histogram.h"
#include "include/zip/pc/api/api.h"
#include "zip/client/client.h"
#include "zip/network/erpc_constants.h"


/** create a logger for this file */
static zip::util::logger logger("pc");

/** signal to stop the server */
static std::atomic<bool> stop = false;
static std::mutex stats_mutex;
static std::atomic<bool> producer_stop = false;
static std::atomic<bool> request_more_data = true;

std::atomic<int> ready = 1;

class timestamp_fifo {
public:
    explicit timestamp_fifo(std::size_t capacity) : buffer_(capacity) {}

    inline void push(uint64_t value) {
        while (true) {
            auto tail = tail_.load(std::memory_order_relaxed);
            auto head = head_.load(std::memory_order_acquire);

            if (tail - head < buffer_.size()) {
                buffer_[tail % buffer_.size()] = value;
                tail_.store(tail + 1, std::memory_order_release);
                return;
            }

            zip::util::relax();
        }
    }

    inline bool try_pop(uint64_t& value) {
        auto head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        value = buffer_[head % buffer_.size()];
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

private:
    std::vector<uint64_t> buffer_;
    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
};
/**
 * Start the server.
 */
class RateLimiter {
public:
    RateLimiter(int64_t r, int64_t b) : r_(r * TOKEN_PRECISION), b_(b * TOKEN_PRECISION), tokens_(0), last_(Clock::now()) {}

    inline void Consume(int64_t n) {
        std::unique_lock<std::mutex> lock(mutex_);

        if (r_ <= 0) {
            return;
        }

        // refill tokens
        auto now = Clock::now();
        auto diff = std::chrono::duration_cast<Duration>(now - last_);
        tokens_ = std::min(b_, tokens_ + diff.count() * r_ / 1000000000);
        last_ = now;

        // check tokens
        tokens_ -= n * TOKEN_PRECISION;

        // sleep
        if (tokens_ < 0) {
            lock.unlock();
            int64_t wait_time = -tokens_ * 1000000000 / r_;
            std::this_thread::sleep_for(std::chrono::nanoseconds(wait_time));
        }
    }

    inline void SetRate(int64_t r) {
        std::lock_guard<std::mutex> lock(mutex_);

        // refill tokens
        auto now = Clock::now();
        auto diff = std::chrono::duration_cast<Duration>(now - last_);
        tokens_ = std::min(b_, tokens_ + diff.count() * r_ * TOKEN_PRECISION / 1000000000);
        last_ = now;

        // set rate
        r_ = r * TOKEN_PRECISION;
    }

private:
    using Clock = std::chrono::steady_clock;
    using Duration = std::chrono::nanoseconds;
    static constexpr int64_t TOKEN_PRECISION = 10000;

    std::mutex mutex_;
    int64_t r_;
    int64_t b_;
    int64_t tokens_;
    Clock::time_point last_;
};

void subscriber_thread(zip::network::manager& manager,
        zip::network::erpc_transport_factory& transport,
        timestamp_fifo& timestamps,
        hdr_histogram *latency,
        subscriber_t sid,
        uint32_t fal,
        std::set<uint16_t> subscriber_cpus,
        std::set<std::string> servers,
        std::string addr,
uint16_t por
        )
{
    transport.set_local_rpc_id_(0);
    transport.set_global_rpc_id_(zip::network::SUBSCRIBER_SERVER_OFFSET);

    auto subscriber = zip::subscriber::subscriber(manager, transport, sid, fal, subscriber_cpus, 1, servers, addr, por);
    auto& iterator = subscriber.iterators().front();

    uint32_t total_received = 0;
    auto start = std::chrono::high_resolution_clock::now();

    // process the messages in sequential order and send a response
    while (!stop.load(std::memory_order_relaxed)) {

        if (auto entry = iterator.next_entry()) {

            if (entry->data_length > 0)
            {
                auto& request = *reinterpret_cast<zip::pc::api::server_request_debug*>(entry->data);
                ZIP_ASSERT(request.request_id == total_received, "Incorrect Sequence Number");
                if (total_received == 0)
                {
                    start = std::chrono::high_resolution_clock::now();
                }
                uint64_t sent_at_ns;
                while (!timestamps.try_pop(sent_at_ns)) {
                    zip::util::relax();
                }

                hdr_record_value_atomic(latency, zip::util::curr_time_in_ns() - sent_at_ns);
                total_received++;

                // Signal the producer that we're ready for more data
                //request_more_data.store(true, std::memory_order_release);
                ready.fetch_add(1, std::memory_order_relaxed);
            }

        }

    }

    auto duration = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - start).count();

    {
        std::lock_guard<std::mutex> lock(stats_mutex);
        std::cout << "[consumer]: Throughput: " << total_received / duration << " msg/s" << std::endl;
        std::cout << "[consumer]: latency metrics " << std::endl;
        std::cout << "[consumer]: percentile latencies " << std::endl
                  << "\tp50: " << hdr_value_at_percentile(latency, 50.0) << std::endl
                  << "\tp95: " << hdr_value_at_percentile(latency, 95.0) << std::endl
                  << "\tp99: " << hdr_value_at_percentile(latency, 99.0) << std::endl
                  << "\tp99.9: " << hdr_value_at_percentile(latency, 99.9) << std::endl;
    }
    producer_stop.store(true);
    ready.store(true);
    ready.notify_all();
}

void producer_thread(zip::network::manager& manager,
        zip::network::erpc_transport_factory& transport,
        timestamp_fifo& timestamps,
        hdr_histogram *latency,
        std::string ord,
        client_t cid,
        shard_t sid,
        uint32_t fal,
        double min,
        double max,
        uint16_t ccu,
        std::set<std::string> servers,
        std::string addr,
        uint16_t por,
        int64_t rate,
        uint32_t siz)
{
    transport.set_local_rpc_id_(0);

    auto client = zip::client::client(manager, transport, ord, cid, sid, fal, min, max, ccu, servers, addr, por);
    RateLimiter *limiter = new RateLimiter(rate, rate);

    // allocate buffers for the request
    auto buffers = manager.get_buffers(zip::consts::MAX_OUTSTANDING);
    auto iterator = zip::util::wraparound_iterator(buffers);

    auto total_sent = 0;

    auto length = std::min(siz, zip::consts::MAX_PAYLOAD);
    auto start = std::chrono::steady_clock::now();

    auto issue_request = [&] () {
        auto& buffer = iterator.get_and_increment();
        auto& message = buffer->as<zip::api::storage_append>();
        auto& request = *reinterpret_cast<zip::pc::api::server_request_debug*>(message.data);

        message.data_length = length;
        request.request_id = total_sent;

        std::atomic<gsn_t> response;
        auto now = zip::util::curr_time_in_ns();

        timestamps.push(now);
        // send the request
        client.append(*buffer, response);
        while (response.load(std::memory_order_relaxed) == std::numeric_limits<gsn_t>::max()) zip::util::relax();
        hdr_record_value_atomic(latency, zip::util::curr_time_in_ns() - now);
        total_sent++;
    };


    while (!producer_stop.load(std::memory_order_relaxed)) {

        // Check if the subscriber is ready for more data
        //if (request_more_data.load(std::memory_order_acquire));

        if (auto x = ready.load(std::memory_order_relaxed))
        {
            if (x > 1)
            {
                logger.info("wtf");
            }

            if (rate > 0)
            {
                limiter->Consume(1);
                issue_request();
                ready.fetch_sub(1, std::memory_order_relaxed);
            }
        }

        //}


        // spin loop optimization
    }
    client.stop();

    auto duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    {
        std::lock_guard<std::mutex> lock(stats_mutex);
        std::cout << "[producer]: Throughput: " << total_sent / duration << " msg/s" << std::endl;
        std::cout << "[producer]: latency metrics " << std::endl;
        std::cout << "[producer]: percentile latencies " << std::endl
                  << "\tp50: " << hdr_value_at_percentile(latency, 50.0) << std::endl
                  << "\tp95: " << hdr_value_at_percentile(latency, 95.0) << std::endl
                  << "\tp99: " << hdr_value_at_percentile(latency, 99.0) << std::endl
                  << "\tp99.9: " << hdr_value_at_percentile(latency, 99.9) << std::endl;
    }
    delete limiter;
}
void start_server(cxxopts::ParseResult& options) {
    // get the arguments parsed
    auto addr = options["address"].as<std::string>();
    auto por = options["port"].as<uint16_t>();
    auto num = options["numa"].as<int>();

    // Stub
    auto ccpu = options["client_cpu"].as<uint16_t>();
    auto cid = options["client_id"].as<client_t>();
    auto max_fraction = options["max_fraction"].as<double>();
    auto min_fraction = options["min_fraction"].as<double>();
    auto ord = options["order"].as<std::string>();
    auto sid = options["shard_id"].as<shard_t>();

    auto rate = options["rate"].as<int64_t>();
    auto siz = options["size"].as<uint32_t>();

    auto subid = options["subscriber_id"].as<subscriber_t>();
    auto nss = options["subscriber_cpus"].as<std::vector<uint16_t>>();
    auto fal = options["failures"].as<uint32_t>();
    auto srv = options["servers"].as<std::vector<std::string>>();

    // initialize the network manager, server, and the subscriber
    auto manager = zip::network::manager(num);
    auto transport = std::make_unique<zip::network::erpc_transport_factory>(manager, zip::network::SUBSCRIBER_SERVER_OFFSET, addr, por);
    timestamp_fifo timestamps(zip::consts::MAX_OUTSTANDING);


    auto servers = std::set(srv.begin(), srv.end());
    auto subscriber_cpus = std::set(nss.begin(), nss.end());
    hdr_histogram *producer_latency, *consumer_latency;
    hdr_init(1, INT64_C(3600000000), 5, &producer_latency);
    hdr_init(1, INT64_C(3600000000), 5, &consumer_latency);

    std::vector<std::thread> threads;

    threads.emplace_back(subscriber_thread, std::ref(manager), std::ref(*transport), std::ref(timestamps), consumer_latency, subid, fal, subscriber_cpus, servers, addr, por);

    sleep(4);

    threads.emplace_back(producer_thread, std::ref(manager), std::ref(*transport), std::ref(timestamps), producer_latency,  ord,
        cid, sid, fal, min_fraction, max_fraction, ccpu, servers, addr, por, rate, siz);

    sleep(options["duration"].as<uint32_t>());

    stop.store(true);
    ready.notify_all();
    // join the running threads before exiting
    for (auto& thread: threads) thread.join();

    std::cout << "[producer]: latency metrics " << std::endl;
    hdr_percentiles_print(producer_latency, stdout, 5, 1, CLASSIC);
    std::cout << "[producer]: percentile latencies " << std::endl
              << "\tp50: " << hdr_value_at_percentile(producer_latency, 50.0) << std::endl
              << "\tp95: " << hdr_value_at_percentile(producer_latency, 95.0) << std::endl
              << "\tp99: " << hdr_value_at_percentile(producer_latency, 99.0) << std::endl
              << "\tp99.9: " << hdr_value_at_percentile(producer_latency, 99.9) << std::endl;
}

/**
 * Entrypoint for the server.
 */
int main(int argc, char* argv[]) {
    // create and parse options for the server
    cxxopts::Options options("server", "A server for SMR based on Ziplog.\n");
    options.set_width(std::numeric_limits<size_t>::max());
    options.add_options("Network")
        ("address","IP address to use", cxxopts::value<std::string>()->default_value("127.0.0.1"))
        ("numa", "NUMA node to use for memory allocation.", cxxopts::value<int>()->default_value("-1"))
        ("port", "RDMA device port to use.", cxxopts::value<uint16_t>()->default_value(std::to_string(zip::consts::DEFAULT_DEVICE_PORT)))
    ;
    options.add_options("Server")
        ("duration", "Time to run the experiment for in seconds.", cxxopts::value<uint32_t>()->default_value("10"))
        ("rate", "Requests per second issued by the stub", cxxopts::value<int64_t>()->default_value("50000"))
        ("size", "Size of the payload in bytes.", cxxopts::value<uint32_t>()->default_value("1024"))
    ;
    options.add_options("Stub")
        ("client_cpu", "CPU for running the client.", cxxopts::value<uint16_t>())
        ("client_id", "ID of the client.", cxxopts::value<client_t>())
        ("max_fraction", "Minimum fraction of used slots to trigger decrease.", cxxopts::value<double>()->default_value("0.99"))
        ("min_fraction", "Maximum fraction of used slots to trigger increase.", cxxopts::value<double>()->default_value("0.95"))
        ("order", "Address of the ordering server.", cxxopts::value<std::string>())
        ("shard_id", "ID of the shard for this client.", cxxopts::value<shard_t>())
    ;

    options.add_options("Subscriber")
        ("failures", "Number of failures to tolerate.", cxxopts::value<uint32_t>()->default_value("1"))
        ("servers", "Addresses of the Ziplog storage servers.", cxxopts::value<std::vector<std::string>>())
        ("subscriber_cpus", "CPUs for running subscriber threads.", cxxopts::value<std::vector<uint16_t>>())
        ("subscriber_id", "ID of the subscriber.", cxxopts::value<subscriber_t>())
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
        if (   result.count("subscriber_cpus") == 0
            || result.count("subscriber_id")   == 0
            || result.count("servers")         == 0) {
            std::cerr << options.help() << std::endl;
            std::exit(1);
        }

        // setup signal handler to stop the server
        std::signal(SIGINT, [] ([[maybe_unused]] int signal) {
            stop.store(true, std::memory_order_relaxed);
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
