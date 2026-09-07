// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/benchClient.cc:
 *   Benchmarking client for a distributed transactional store.
 *
 **********************************************************************/

#include "store/common/consts.h"
#include "store/common/truetime.h"
#include "store/meerkatstore/meerkatir/client.h"
#include "store/common/flags.h"
#include "network/buffer.h"
#include "network/manager.h"

#include <boost/fiber/all.hpp>
#include <csignal>
#include <fstream>
#include <signal.h>
#include <random>

#include <iostream>

using namespace std;

//#define ZIPKAT_SEPARATE_THREAD 1

// Function to pick a random key according to some distribution.
int rand_key();

bool ready = false;
double *zipf;
vector<string> keys;
std::mt19937 key_gen;
vector<std::uniform_int_distribution<uint32_t>> keys_distributions;
thread_local std::uniform_int_distribution<uint32_t> key_dis;

struct measurement {
    uint64_t nTransaction;
    timeval start;
    timeval end;
    bool status;
};
constexpr size_t kNumMeasurement = 1000000;
constexpr int kTxnSucc = 1;
constexpr int kWriteTxnAbort = 2;
constexpr int kReadOnlyTxnAbort = 3;

void client_fiber_func(int thread_id, std::shared_ptr<zip::client::client> ziplogClient,
                       zip::network::manager* manager) {
//    vector<measurement> results(kNumMeasurement);
    vector<string> results;

    std::mt19937 core_gen;
    std::mt19937 replica_gen;
    //std::uniform_int_distribution<uint32_t> core_dis(0, FLAGS_numServerThreads - 1);
    //std::uniform_int_distribution<uint32_t> replica_dis(0, nReplicas - 1);
    std::random_device rd;
    uint8_t preferred_thread_id;
    uint32_t localReplica = -1;

    core_gen = std::mt19937(rd());
    replica_gen = std::mt19937(rd());
    key_dis = std::uniform_int_distribution<uint32_t>(0, FLAGS_numKeys - 1);

    // Open file to dump results
    //uint32_t global_client_id = FLAGS_nhost * 1000 + FLAGS_ncpu * FLAGS_numClientThreads + thread_id;
    //FILE* fp = fopen((FLAGS_logPath + "/client." + std::to_string(global_client_id) + ".log").c_str(), "w");
    uint32_t global_thread_id = thread_id;
    FILE* fp = fopen((FLAGS_logPath + "/client." + std::to_string(global_thread_id) + ".log").c_str(), "w");

    std::cout << "Start RetwisClient-" << global_thread_id << std::endl;
    // Trying to distribute as equally as possible the clients on the
    // replica cores.

    //preferred_core_id = core_dis(core_gen);

    // pick the prepare and commit thread on the replicas in a round-robin fashion
    preferred_thread_id = global_thread_id % FLAGS_numServerThreads;

    // pick the replica and thread id for read in a round-robin fashion
    int global_preferred_read_thread_id  = global_thread_id % FLAGS_numServerThreads;
    int local_preferred_read_thread_id = global_preferred_read_thread_id;

    if (FLAGS_closestReplica == -1) {
        //localReplica =  (global_thread_id / nsthreads) % nReplicas;
        // localReplica = replica_dis(replica_gen);
        localReplica = global_preferred_read_thread_id;
    } else {
        localReplica = FLAGS_closestReplica;
    }

    //fprintf(stderr, "global_thread_id = %d; localReplica = %d\n", global_thread_id, localReplica);
    Assert(FLAGS_mode == "meerkatstore");
    auto client = std::make_unique<meerkatstore::meerkatir::Client>(
                                        FLAGS_numServerThreads, FLAGS_numShards,
                                        global_thread_id, ziplogClient, *manager);
    struct timeval t0, t1, t2;

    uint64_t nTransactions = 0;
    int tCount = 0;
    double tLatency = 0.0;
    int getCount = 0;
    double getLatency = 0.0;
    int commitCount = 0;
    double commitLatency = 0.0;
    string key, value;
    bool status;
    char buffer[100];
    string v (56, 'x'); //56 bytes

    gettimeofday(&t0, NULL);
    srand(t0.tv_sec + t0.tv_usec);

    std::vector<int> keyIdx;
    int ttype; // Transaction type.
    int ret;


#ifdef ZIP_MEASURE
    hdr_histogram* hist_wrk;
    hdr_init(1, 10000, 3, &hist_wrk);
    int hdr_count_wrk = 0;
#endif

    while (1) {
        keyIdx.clear();
        status = true;

        gettimeofday(&t1, NULL);
        client->Begin();

        // Decide which type of retwis transaction it is going to be.
        ttype = rand() % 100;

        if (ttype < 5) {
            // 5% - Add user transaction. 1,3
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            int idx = keyIdx[0];
            if ((ret = client->Get(keys[idx], idx, value, boost::this_fiber::yield))) {
                Warning("Aborting due to %s %d", keys[idx].c_str(), ret);
                status = false;
            }

            for (int i = 0; i < 3 && status; i++) {
                int idx = keyIdx[i];
                client->Put(keys[idx], idx, v);
            }
            ttype = 1;
        } else if (ttype < 20) {
            // 15% - Follow/Unfollow transaction. 2,2
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            for (int i = 0; i < 2 && status; i++) {
                int idx = keyIdx[i];
                if ((ret = client->Get(keys[idx], idx, value, boost::this_fiber::yield))) {
                    Warning("Aborting due to %s %d", keys[idx].c_str(), ret);
                    status = false;
                }
                client->Put(keys[idx], idx, v);
            }
            ttype = 2;
        } else if (ttype < 50) {
            // 30% - Post tweet transaction. 3,5
#ifdef ZIP_MEASURE
            auto start = std::chrono::high_resolution_clock::now();
#endif
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            for (int i = 0; i < 3 && status; i++) {
                int idx = keyIdx[i];
                if ((ret = client->Get(keys[idx], idx, value, boost::this_fiber::yield))) {
                    Warning("Aborting due to %d %s %d", idx, keys[idx].c_str(), ret);
                    status = false;
                }
                client->Put(keys[idx], idx, v);
            }
            for (int i = 0; i < 2; i++) {
                int idx = keyIdx[i+3];
                //client->Put(keys[keyIdx[i]], v);
                client->Put(keys[idx], idx, v);
            }
            ttype = 3;

#ifdef ZIP_MEASURE
            auto end = std::chrono::high_resolution_clock::now();
            hdr_record_value(hist_wrk, zip::util::time_in_us(end - start));
            if (++hdr_count_wrk == 100000) {
                hdr_count_wrk = 0;
                auto lat_50 = hdr_value_at_percentile(hist_wrk, 50);
                auto lat_99 = hdr_value_at_percentile(hist_wrk, 99);
                auto lat_999 = hdr_value_at_percentile(hist_wrk, 99.9);
                auto mean = hdr_mean(hist_wrk);
                std::cerr << "Client-wrk (" << global_thread_id << ") statistics: median latency: " << lat_50 << " us\t99% latency: " << lat_99 << " us\t99.9% latency: " << lat_999 << " us\tmean: " << mean << std::endl;
            }
#endif
        } else {
            // 50% - Get followers/timeline transaction. rand(1,10),0
            int nGets = 1 + rand() % 10;
            for (int i = 0; i < nGets; i++) {
                keyIdx.push_back(rand_key());
            }

            sort(keyIdx.begin(), keyIdx.end());
            for (int i = 0; i < nGets && status; i++) {
                int idx = keyIdx[i];
                if ((ret = client->Get(keys[idx], idx, value, boost::this_fiber::yield))) {
                    Warning("Aborting due to %s %d", keys[idx].c_str(), ret);
                    status = false;
                }
            }
            ttype = 4;
        }

        if (status) {
            status = client->Commit(boost::this_fiber::yield);
        }
        gettimeofday(&t2, NULL);
        //fprintf(fp, "Done commit\n");

        //commitCount++;
        //commitLatency += ((t2.tv_sec - t3.tv_sec)*1000000 + (t2.tv_usec - t3.tv_usec));

        // log only the transactions that finished in the interval we actually measure
        if ((t2.tv_sec >= FLAGS_secondsFromEpoch + FLAGS_warmup) &&
            (t2.tv_sec < FLAGS_secondsFromEpoch + FLAGS_duration - FLAGS_warmup)) {
            long latency = (t2.tv_sec - t1.tv_sec)*1000000 + (t2.tv_usec - t1.tv_usec);
            sprintf(buffer, "%d %ld.%06ld %ld.%06ld %ld %d\n", ++nTransactions, t1.tv_sec,
                    t1.tv_usec, t2.tv_sec, t2.tv_usec, latency, status ? kTxnSucc : (ttype == 4 ? kReadOnlyTxnAbort : kWriteTxnAbort));
            results.push_back(string(buffer));
            if (status) {
                tCount++;
                tLatency += latency;
            }
            //printf("client-%d, %lu %ld.%06ld %ld.%06ld %ld %d\n", global_thread_id, nTransactions, t1.tv_sec,
            //        t1.tv_usec, t2.tv_sec, t2.tv_usec, latency, status?1:0);
/*
            if (nTransactions > results.size())
                results.emplace_back(measurement{nTransactions, t1, t2, status});
            else
                results[nTransactions] = measurement {nTransactions + 1, t1, t2, status};
*/
            //++nTransactions;
        }
        gettimeofday(&t1, NULL);
        if (((t1.tv_sec-t0.tv_sec)*1000000 + (t1.tv_usec-t0.tv_usec)) > FLAGS_duration*1000000) {
            break;
        }
    }
  
/*
    std::cout << "start writing to log file\n";
    for (auto& r : results) {
        if (r.nTransaction == 0) {
            // Skip the pre-filled elements   
            break;
        }
        const auto latency = (r.end.tv_sec - r.start.tv_sec)*1000000 + (r.end.tv_usec - r.start.tv_usec);
        fprintf(fp, "%d %ld.%06ld %ld.%06ld %ld %d\n",
            r.nTransaction, r.start.tv_sec, r.start.tv_usec, r.end.tv_sec, r.end.tv_usec, latency, r.status?1:0);

        if (r.status) {
            tCount++;
            tLatency += latency;
        }
    }
    std::cout << "Write to log file done\n";
*/

    for (auto line : results) {
        fprintf(fp, "%s", line.c_str());
    }
    fprintf(fp, "# Commit_Ratio: %lf\n", (double)tCount/nTransactions);
    fprintf(fp, "# Overall_Latency: %lf\n", tLatency/tCount);
    fprintf(fp, "# Get: %d, %lf\n", getCount, getLatency/getCount);
    fprintf(fp, "# Commit: %d, %lf\n", commitCount, commitLatency/commitCount);
    fclose(fp);
    std::cout << "RetwisClient client-" << global_thread_id << " done\n";
}

void* client_thread_func(int ziplog_id, int cpu_id, zip::network::manager* manager) {
    // create the client fibers
    boost::fibers::fiber client_fibers[FLAGS_numClientFibers];

    // Use cores of NUMA1.
    printf("ziplog id=%d, cpu_id=%d, client_rate=%lu\n", ziplog_id, cpu_id, FLAGS_ziplogClientRate);
    auto ziplogClient = std::make_shared<zip::client::client>(
        *manager, kOrderAddr, ziplog_id, kZiplogShardId, cpu_id, FLAGS_ziplogClientRate);
    for (int i = 0; i < FLAGS_numClientFibers; i++) {
        boost::fibers::fiber f(
            client_fiber_func, ziplog_id * FLAGS_numClientFibers + i, ziplogClient, manager);
        client_fibers[i] = std::move(f);
    }

    for (int i = 0; i < FLAGS_numClientFibers; i++) {
        client_fibers[i].join();
        //printf("ziplog id=%d, done joinging i=%d\n", ziplog_id, i);
    }
    return NULL;
};


void segfault_sigaction(int signal, siginfo_t *si, void *arg)
{
    fprintf(stderr, "Caught segfault at address %p, code = %d\n", si->si_addr, si->si_code);
    exit(0);
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

/*
    struct sigaction sa;

    memset(&sa, 0, sizeof(struct sigaction));
    sigemptyset(&sa.sa_mask);
    sa.sa_sigaction = segfault_sigaction;
    sa.sa_flags   = SA_SIGINFO;

    sigaction(SIGSEGV, &sa, NULL);
*/

    std::signal(SIGINT, [] (int signal) { std::cerr << "SIGINT caught\n"; exit(1); });

    // initialize the uniform distribution
    std::random_device rd;
    key_gen = std::mt19937(rd());

    // Read in the keys from a file.
    string key, value;
    ifstream in;
    in.open(FLAGS_keysFile);
    if (!in) {
        fprintf(stderr, "Could not read keys from: %s\n",
                FLAGS_keysFile.c_str());
        exit(0);
    }
    for (int i = 0; i < FLAGS_numKeys; i++) {
        getline(in, key);
        keys.push_back(key);
    }
    in.close();

    // Create the transport threads; each transport thread will run
    // FLAGS_numClientThreads client fibers
    // TODO: specify client/ziplog client ratio.
//    const auto ziplog_id = FLAGS_nhost * FLAGS_numClientThreads;
//    const auto cpu_id = 1;
    std::vector<std::unique_ptr<zip::network::manager>> managers;

    std::vector<std::thread> client_thread_arr(FLAGS_numClientThreads);
    const auto id_base = FLAGS_nhost * FLAGS_numClientThreads;
    for (size_t i = 0; i < FLAGS_numClientThreads; i++) {
/*
        if (i % 2 == 0) {
            printf("thread id=%d at %s\n", i, zip::consts::rdma::DEFAULT_DEVICE1);
            managers.emplace_back(std::make_unique<zip::network::manager>(zip::consts::rdma::DEFAULT_DEVICE1, zip::consts::rdma::DEFAULT_PORT, 1));
        } else {
*/
            printf("thread id=%d at %s\n", i, zip::consts::rdma::DEFAULT_DEVICE);
            managers.emplace_back(std::make_unique<zip::network::manager>(zip::consts::rdma::DEFAULT_DEVICE, zip::consts::rdma::DEFAULT_PORT, zip::consts::rdma::DEFAULT_GID));
        int ziplog_core;
        int txn_core;
#ifdef ZIPKAT_SEPARATE_THREAD

#if 0
        assert(FLAGS_numClientThreads <= 4); // Run with FLAGS_numClientThreads <= 4;
        if (i < 2) {
            // *2 for using the core of the same NUMA, *2 again because one ziplog client uses one cores
            ziplog_core = 3 * 2 * i + 1;
            txn_core = 3 * 2 * i + 5;
        } else {
            ziplog_core = 3 * 2 * (i - 2);
            txn_core = 3 * 2 * (i - 2) + 4;
        }
#else
        if (i < 4) {
            // *2 for using the core of the same NUMA, *2 again because one ziplog client uses one cores
            ziplog_core = 3 * 2 * i + 1;
            txn_core = 3 * 2 * i + 5;
        } else {
            ziplog_core = 3 * 2 * (i - 3);
            txn_core = 3 * 2 * (i - 3) + 4;
        }
#endif

#else
        // TODO: use const for determining the 4, it's basically related to # of real (not hyper) cores on a machine.
#if 1
        if (i < 4) {
            ziplog_core = 2 * 2 * i + 1;
            txn_core = 2 * 2 * i + 3;
        } else {
            ziplog_core = 2 * 2 * (i - 4);
            txn_core = 2 * 2 * (i - 4) + 2;
        }
#else
        // Try using hyper cores.
        ziplog_core = 16 + 2 * i + 1; //2 * i + 1;
        txn_core = 2 * i + 1; //16 + 2 * i + 1;
#endif
#endif
        client_thread_arr[i] = std::thread(client_thread_func, id_base + i, ziplog_core, managers.back().get());
        zip::util::pin_thread(client_thread_arr[i], txn_core);
        // client_thread_arr[i] = std::thread(client_fiber_func, i, ziplogClient);
        // uint8_t idx = i/2 + (i % 2) * 12;
        // erpc::bind_to_core(client_thread_arr[i], 0, i);
    }
    for (auto &thread : client_thread_arr) thread.join();

    return 0;
}

int rand_key()
{
    if (FLAGS_zipf <= 0) {
        // Uniform selection of keys.
        return key_dis(key_gen);
    } else {
        // Zipf-like selection of keys.
        if (!ready) {
            zipf = new double[FLAGS_numKeys];

            double c = 0.0;
            for (int i = 1; i <= FLAGS_numKeys; i++) {
                c = c + (1.0 / pow((double) i, FLAGS_zipf));
            }
            c = 1.0 / c;

            double sum = 0.0;
            for (int i = 1; i <= FLAGS_numKeys; i++) {
                sum += (c / pow((double) i, FLAGS_zipf));
                zipf[i-1] = sum;
            }
            ready = true;
        }

        double random = 0.0;
        while (random == 0.0 || random == 1.0) {
            random = (1.0 + rand())/RAND_MAX;
        }

        // binary search to find key;
        int l = 0, r = FLAGS_numKeys, mid;
        while (l < r) {
            mid = (l + r) / 2;
            if (random > zipf[mid]) {
                l = mid + 1;
            } else if (random < zipf[mid]) {
                r = mid - 1;
            } else {
                break;
            }
        }
        return mid;
    }
}
