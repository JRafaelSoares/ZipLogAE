# ZipKVS

This repo is part of the artifact for ZipKVS, a replicated transactional key-value store (KVS) built on top of ZipLog.
This branch holds the implementation of ZipKVS and the implementation of ZipLog used to support it.
The client benchmark used to evaluate ZipKVS can be found in the `retwis` branch of the same git repository.

### Paper

**Linearizable Shared Log API Considered Harmful**

Yu-Ju Huang<sup>1</sup>, Shubham Chaudhary<sup>1</sup>, Rafael Soares<sup>2</sup>, Shir Cohen Gahtan<sup>1</sup>, Sabrina Johnson<sup>1</sup>, Arnav Kaul<sup>1</sup>, Lorenzo Alvisi<sup>1</sup>, Luis Rodrigues<sup>2</sup>, and Robbert van Renesse<sup>1</sup>

<sup>1</sup> Cornell University
<sup>2</sup> INESC-ID, Instituto Superior Técnico, Universidade de Lisboa

## Requirements

Before building or running ZipKVS, make sure you have:

### Software Requirements
- **Linux** — the networking layer is built directly on `libibverbs`/`rdma-core`, which are Linux-only.
- **Root/sudo access** — needed to configure huge pages (below) and, when using Docker, to run the container with elevated privileges for RDMA device access.

### ⚠️ Hardware Requirement: RDMA + mlx5

**This codebase requires a real RDMA-capable network device to run — specifically, it is written and tuned against Mellanox `mlx5` hardware.**

If you don't have access to hardware/a driver that supports XRC queue pairs (in practice, `mlx5`-class RDMA NICs), you will not be able to exercise this codebase's actual networking path.

> **Disclaimer:** the original evaluation in the paper was run on machines with two Intel Xeon Gold 6242 16-core CPUs and 192 GB of DDR4 memory each, equipped with Mellanox ConnectX-4 dual-port NICs connected via a 100 Gbps Mellanox SB7700 InfiniBand switch (4 μs median RTT, sub-1 μs clock sync). Other `mlx5`-class hardware should work, but has not been tested.

## Dependencies

- **Compiler**: `clang++-16` — the project relies on C++20 features and is built exclusively with Clang 16 (see `Makefile`).
- **Build tools**: `cmake`, `make`, `git` (with submodule support), `autoconf` (required to build the vendored `jemalloc` submodule).
- **RDMA**: `libibverbs` (part of `rdma-core`) — the entire networking layer (`zip::network::manager`) is built directly on top of `libibverbs`.
- **NUMA**: `libnuma-dev` — used for NUMA-aware memory allocation of RDMA buffers.
- **zlib**: `zlib1g-dev` — linked into the benchmark clients (`apps/append`, `apps/smr`) for histogram log encoding.
- **Vendored submodules** (fetched via `git submodule`, built automatically as part of the Makefile):
  - [`cxxopts`](https://github.com/jarro2783/cxxopts) — command-line argument parsing for every binary.
  - [`HdrHistogram_c`](https://github.com/HdrHistogram/HdrHistogram_c) — latency histograms in the benchmark clients.
  - [`jemalloc`](https://github.com/jemalloc/jemalloc) — allocator used by the storage server.

### Installing dependencies (Ubuntu/Debian)

```bash
sudo apt-get install -y  \
      cmake \
      clang-16 \
      build-essential \
      git \
      autoconf \
      libibverbs-dev \
      ibverbs-providers \
      ibverbs-utils \
      rdma-core \
      libnuma-dev \
      zlib1g-dev \
    libboost-dev \
    libboost-fiber-dev \
    libboost-context-dev \
    libboost-thread-dev \
    libboost-system-dev \
    cmake \
    libgtest-dev
```

Huge pages must also be configured, on the host, for RDMA buffer allocation:

```bash
echo 2048 | sudo tee /proc/sys/vm/nr_hugepages
```

This is a host-level kernel setting, so it applies whether you build natively or inside Docker.

## Docker

As an alternative to installing dependencies directly on the host, a `Dockerfile` is provided with every build dependency pre-installed.

**1. Build the image:**

```bash
docker build -t zipkvs .
```

**2. Run a container with RDMA access.** The container needs the host's RDMA devices and network stack passed through:

```bash
docker run --privileged --network host -d \
  -v "$(pwd)":/src/zipkvs \
  zipkvs sleep infinity
```

- `--privileged` exposes `/dev/infiniband/*` inside the container.
- `--network host` lets the container reach the RDMA NIC's IP/interface directly.
- `-v "$(pwd)":/src/zipkvs` bind-mounts the repo into the container, overlaying the copy baked into the image, so edits and `make` output are shared with the host.
- `-v /PATH/TO/RETWIS:/src/retwis` bind-mounts the repo including retwis into the container

**3. Enter the container**

```bash
docker exec -it $(docker ps -q -f ancestor=zipkvs) bash
```

**4. Verify RDMA visibility** inside the container before running any binary:

```bash
ibv_devices
```

Your device (e.g. `mlx5_0`) should be listed; if it isn't, double-check the huge pages configuration and step 2's flags.

## Building

```bash
make deps && make -j
```

This produces the following binaries directly under `build/`:
- `build/order` — the Zipper server.
- `build/storage` — the Storage server, which includes ZipKVS.
- `build/client` — the append benchmark client. For the purpose of ZipKVS, we can ignore

Every binary supports `-h`/`--help` to print its full argument list.

## Running Each Module

For ZipKVS, we are solely interested in deploying `order` and `storage` services. Each takes a different set of arguments (CPU pinning, shard/replica IDs, addresses of the services it connects to, ...).
Deployment is done automatically by our deployment scripts, which are described in the `retwis` branch,

# Acknowledgments

This work was co-funded by the European Union through the Lisboa 2030 Programme (ERDF) and by national funds through FCT, I.P., under projects no. 16539, UID/50021/2025, UID/PRR/50021/2025, and LISBOA2030-FEDER-00771200 and under grant UI/BD/153590/2022.