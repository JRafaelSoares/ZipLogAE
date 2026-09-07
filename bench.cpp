#include <iostream>
#include <algorithm>
#include <numeric>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <functional>
#include <random>
#include <set>
#include <mutex>
#include <bitset>
#include <cstring>
#include <immintrin.h>

// constants
#define MAX_KEYS 7
#define NUM_KEYS (1 << 22)

// entry in the log
struct entry {
    uint8_t num_keys;
    uint64_t keys[MAX_KEYS];
    std::atomic_flag locked;
};

// setup the entire array
void setup(std::unique_ptr<entry[]>& log, unsigned long length) {
    // setup a random number generator
    std::mt19937 gen(0);
    std::uniform_int_distribution<uint8_t> num_keys_distr(1, MAX_KEYS);
    std::uniform_int_distribution<uint64_t> key_distr(0, NUM_KEYS - 1);

    for (unsigned long i = 0; i < length; i++) {
        // generate an set of keys
        auto num_keys = num_keys_distr(gen);
        std::set<uint64_t> keys;
        for (unsigned long j = 0; j < num_keys; j++) {
            keys.insert(key_distr(gen));
        }

        // add the keys to the entry
        auto& entry = log[i];
        entry.num_keys = 0;
        for (auto key: keys) {
            entry.keys[entry.num_keys++] = key;
        }
    }
}

// perform work for that many microseconds
template <typename R, typename P>
inline void wait(std::chrono::duration<R, P> work) {
    auto begin = std::chrono::high_resolution_clock::now();
    while (std::chrono::high_resolution_clock::now() - begin < work) { _mm_pause(); }
}

// calculate the intersection between two arrays
bool intersect(uint64_t F[], uint64_t S[], uint8_t f, uint8_t s) {
    for (uint8_t i = 0; i < f; i++) {
        for (uint8_t j = 0; j < s; j++) {
            if (F[i] == S[j]) return true;
        }
    }
    return false;
}

// this is the thread that does processing
void process(
    std::atomic<bool>& go,
    std::unique_ptr<entry[]>& log,
    std::unique_ptr<std::mutex[]>& locks,
    unsigned long length,
    unsigned long work,
    unsigned long index,
    unsigned long num_threads
) {
    // setup constants
    unsigned long current = 0, last_locked = 0, l_conflicts = 0, l_totals = 0;
    std::chrono::microseconds us(work);

    // wait until starting
    while (!go.load(std::memory_order_relaxed));

    // process in a loop until the end
    while (current < length) {
        // decrease the window if possible
        if (log[last_locked].locked.test(std::memory_order_relaxed)) {
            last_locked++;
        }

        // we need to process this entry
        if (current % num_threads == index) {
            // make sure that we can proceed
            for (unsigned long temp = last_locked; temp < current; temp++) {
                // check whether the last locked entry is locked
                if (!log[temp].locked.test(std::memory_order_relaxed)) {
                    // if it's not locked, then check if this conflicts,
                    // if it does not conflict then move on, otherwise
                    // we will wait on this entry to become locked
                    auto conflict = intersect(log[temp].keys, log[current].keys, log[temp].num_keys, log[current].num_keys);
                    if (conflict) while (!log[temp].locked.test(std::memory_order_relaxed)) { _mm_pause(); }
                }

                // if this entry is locked, and we are on
                // track with the last locked then update both
                // otherwise just update the index
                if (last_locked == temp) last_locked++;
            }

            if (current != 0) {
                while (!log[current - 1].locked.test(std::memory_order_relaxed)) { _mm_pause(); }
            }

            // lock the associated keys
            for (unsigned long i = 0; i < log[current].num_keys; i++) {
                locks[log[current].keys[i]].lock();
            }

            // set the status
            log[current].locked.test_and_set(std::memory_order_relaxed);

            // perform the given amount of work
            wait(us);

            // unlock the associated keys
            for (unsigned long i = 0; i < log[current].num_keys; i++) {
                locks[log[current].keys[i]].unlock();
            }
        }

        // move on to the next entry
        current++;
    }
}

int main(int argc, char* argv[]) {
    // parse arguments
    if (argc != 4) {
        std::cout << "usage: " << argv[0] << " NUM_THREADS LOG_LENGTH WORK\n";
        std::exit(1);
    }
    unsigned long num_threads = std::stoi(argv[1]);
    unsigned long length = std::stoul(argv[2]);
    unsigned long work = std::stoul(argv[3]);

    // initialise shared variables
    auto locks = std::make_unique<std::mutex[]>(NUM_KEYS);
    auto log = std::make_unique<entry[]>(length);
    std::atomic<bool> go = false;

    // setup the log
    setup(log, length);

    // create the threads
    std::vector<std::thread> threads;
    for (unsigned int i = 0; i < num_threads; i++) {
        threads.emplace_back(process, std::ref(go), std::ref(log), std::ref(locks), length, work, i, num_threads);
    }

    // start the experiment and measure the results
    auto begin = std::chrono::high_resolution_clock::now();
    go.store(true, std::memory_order_relaxed);
    for (auto& thread: threads) {
        thread.join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count(); 
    std::cout << num_threads << "\t" << length << "\t" << ((length * std::nano::den) / ns) / 1e6 << "\n";
    return 0;
}
