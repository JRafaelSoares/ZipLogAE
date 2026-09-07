# ZipLog (eRPC)

This repo is the artifact for the eRPC version of Ziplog, a totally ordered shared log.
ZipLog is described in the paper:

**Linearizable Shared Log API Considered Harmful**

Yu-Ju Huang<sup>1</sup>, Shubham Chaudhary<sup>1</sup>, Rafael Soares<sup>2</sup>, Shir Cohen Gahtan<sup>1</sup>, Sabrina Johnson<sup>1</sup>, Arnav Kaul<sup>1</sup>, Lorenzo Alvisi<sup>1</sup>, Luis Rodrigues<sup>2</sup>, and Robbert van Renesse<sup>1</sup>

<sup>1</sup> Cornell University
<sup>2</sup> INESC-ID, Instituto Superior Técnico, Universidade de Lisboa

## Requirements

Before building or running ZipLog, make sure you have:

### Software Requirements
- **Linux** — the networking layer is built directly on `libibverbs`/`rdma-core`, which are Linux-only.
- **Root/sudo access** — needed to configure huge pages (below).

### ⚠️ Hardware Requirement: RDMA + mlx5

**This codebase requires a real RDMA-capable network device to run**

The networking layer is built on top of [eRPC](https://github.com/erpc-io/eRPC) (vendored under `deps/eRPC`), configured to use its InfiniBand transport. If you don't have access to `mlx5`-class RDMA NICs, you will not be able to exercise this codebase's actual networking path.

## Dependencies

- **Compiler**: `clang++` with C++20 support (see `CMakeLists.txt`). `Release` builds enable per-target LTO, which requires an `llvm-ar`/`llvm-ranlib` matching the installed Clang version.
- **Build tools**: `cmake`, `make`, `git` (with submodule support).
- **RDMA**: `libibverbs` (part of `rdma-core`) — both `zip::network::manager` and the vendored `eRPC` InfiniBand transport are built directly on top of it.
- **eRPC**: vendored under `deps/eRPC` (plain vendored copy, not a git submodule) and must be built separately before building ziplog — see Building below. ziplog links against it with `ERPC_INFINIBAND=true`, so it must be configured with `-DTRANSPORT=infiniband`.
- **NUMA**: `libnuma-dev` — used for NUMA-aware memory allocation of RDMA buffers.
- **zlib**: `zlib1g-dev` — linked into the append benchmark client (`apps/append`) for histogram log encoding.
- **Vendored submodules** (fetched via `git submodule`):
  - [`cxxopts`](https://github.com/jarro2783/cxxopts) — command-line argument parsing for every binary.
  - [`HdrHistogram_c`](https://github.com/HdrHistogram/HdrHistogram_c) — latency histograms in the benchmark clients.
  - [`jemalloc`](https://github.com/jemalloc/jemalloc).

### Installing dependencies (Ubuntu/Debian)

```bash
sudo apt install -y \
  cmake \
  build-essential \
  git \
  libibverbs-dev \
  ibverbs-providers \
  ibverbs-utils \
  rdma-core \
  libnuma-dev \
  zlib1g-dev
```

Huge pages must also be configured, on the host, for RDMA buffer allocation:

```bash
echo 2048 | sudo tee /proc/sys/vm/nr_hugepages
```

## Building

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++
cmake --build build -j
```

The configure step fetches the vendored submodules (`git submodule update --init --recursive`) and builds `eRPC` (under `deps/eRPC`) automatically if they're missing.

This produces the following binaries under `build/out/`:
- `build/out/bin/order` — the Zipper server
- `build/out/bin/storage` — the Storage server
- `build/out/apps/append/client` — the append benchmark client
- `build/out/apps/pc/pc` — the producer-consumer benchmark

Every binary supports `-h`/`--help` to print its full argument list.

## Service Descriptions

ZipLog is made up of two core services — the **Zipper** and **storage servers** — plus a client/subscriber library and two example applications built on top of them. Figure 1 shows how the core services fit together for a deployment with two shards, each replicated across multiple storage servers:

![Figure 1: ZipLog architecture](docs/ZipLog-arch.png)

Clients append records directly, in FIFO order, to the storage servers of their shard. The Zipper measures each client's request rate and uses it to pre-assign and deterministically interleave log slots across shards; storage servers then deliver the resulting totally-ordered log to subscribers.

### Zipper (`order`)

- Manages storage servers and clients.
- Measures each client's request rate over fixed time windows ("epochs") to pre-allocate log slots and deterministically interleave them across shards for that epoch.
- Code: `bin/order.cpp`, `lib/order`.

### Storage servers (`storage`)

- Receive append requests from clients and serve them, in total order, to subscribers.
- Code: `bin/storage.cpp`, `lib/storage`.

### Client library (`zip::client`)

- The main interface for appending records to the log.
- Aggregates append requests and self-tunes the rate it issues them to storage servers at, based on the fraction of Zipper-allocated log slots it consumes each epoch.
- Code: `lib/client`.

### Subscriber library (`zip::subscriber`)

- The main interface for consuming records from the log.
- Receives records from storage servers and interleaves them, across shards, according to the Zipper-assigned total order.
- Code: `lib/subscriber`.

This repository includes two example applications built on top of the client/subscriber libraries:

- **`apps/append`** — a standalone append benchmark (`client`) that appends records for a fixed duration and reports throughput/latency.
- **`apps/pc`** — a producer-consumer benchmark (`pc`) that runs a client and a subscriber in the same process to measure end-to-end append-to-delivery latency.

## Running Each Module

There are four binaries: `order`, `storage`, `client` (from `apps/append`), and `pc` (from `apps/pc`). Each takes a different set of arguments (CPU pinning, shard/replica IDs, addresses of the services it connects to, ...).

See **[docs/RUNNING.md](docs/RUNNING.md)** for the full argument reference for every binary, **[docs/EXAMPLE_DEPLOYMENT.md](docs/EXAMPLE_DEPLOYMENT.md)** for a worked, end-to-end single-machine deployment (the append benchmark and the producer-consumer benchmark, both running on top of a shared `order`/`storage` setup), and **[docs/SCRIPTED_DEPLOYMENT.md](docs/SCRIPTED_DEPLOYMENT.md)** for automating a multi-machine deployment of either benchmark over SSH.

# Acknowledgments

This work was co-funded by the European Union through the Lisboa 2030 Programme (ERDF) and by national funds through FCT, I.P., under projects no. 16539, UID/50021/2025, UID/PRR/50021/2025, and LISBOA2030-FEDER-00771200 and under grant UI/BD/153590/2022.