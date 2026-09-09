# Example Deployment

This walks through a complete ZipLog deployment on a single 8-core machine, running one of two example workloads on top of a shared `order` + `storage` setup:

- the **append benchmark**, or
- the **producer-consumer benchmark** (`pc`, appending to and consuming from the log in the same process, to measure end-to-end latency).

The two workloads are alternatives to each other — run one at a time on top of the same `order`/`storage` deployment, not both simultaneously.

> See [Running Each Module](RUNNING.md) for the full argument reference for every binary used below.

This example assumes a single machine with 8 CPU cores (0–7) and one RDMA device (`--address=127.0.0.1` throughout). Each process needs its own `--port` to avoid colliding on the same host. For a multi-machine deployment, replace `127.0.0.1` in each `--order`/`--servers` address with the reachable IP of the machine actually running that service, and adjust the `--cpu`/`--*_cpus` values to fit each machine's own core layout.

## 1. Shared setup: ordering + storage

Start the ordering server (CPU 0):

```bash
./build/out/bin/order --cpu=0 --port 31850
```

This binds to `127.0.0.1:6666` by default — that's the address every other process below points at via `--order`.

Then start two storage replicas for shard 0, each with its own client-facing and subscriber-facing CPU and its own `--port` (CPUs 1–4):

```bash
./build/out/bin/storage \
  --client_cpus=1 \
  --order=127.0.0.1:31850 \
  --port=31851 \
  --replica_id=0 \
  --shard_id=0 \
  --subscriber_cpus=2 \
  --timeout=100000
```

```bash
./build/out/bin/storage \
  --client_cpus=3 \
  --order=127.0.0.1:31850 \
  --port=31852 \
  --replica_id=1 \
  --shard_id=0 \
  --subscriber_cpus=4 \
  --timeout=100000
```

This leaves CPUs 5–7 free for whichever of the two workloads below you run next.

## 2. Append benchmark

Run the append benchmark client for 10 seconds (CPUs 5–6):

```bash
./build/out/apps/append/client \
  --bench_cpu=6 \
  --client_cpu=5 \
  --client_id=0 \
  --duration=10 \
  --order=127.0.0.1:31850 \
  --port=31853 \
  --servers=127.0.0.1:31851,127.0.0.1:31852 \
  --shard_id=0
```

The client runs for the requested duration, then prints its own throughput/latency statistics and exits — no separate subscriber process is needed for this workload.

## 3. Producer-consumer benchmark

As an alternative to the append benchmark above (on the same shared `order`/`storage` deployment), run `pc` instead, using CPUs 5–7 for its internal client and subscriber threads:

```bash
./build/out/apps/pc/pc \
  --client_cpu=5 \
  --client_id=0 \
  --duration=10 \
  --order=127.0.0.1:31850 \
  --port=31853 \
  --servers=127.0.0.1:31851,127.0.0.1:31852 \
  --shard_id=0 \
  --subscriber_cpus=6 \
  --subscriber_id=0
```

This appends at a fixed rate (`--rate`, default `50000` msgs/sec) for the requested duration while a subscriber thread consumes the log in the same process, then prints producer (append) and consumer (end-to-end delivery) latency statistics separately.

## Stopping

`order` and `storage` are long-running services: each installs a `SIGINT` handler and shuts down cleanly on `Ctrl+C`. Stop them in this order: `storage` first, then `order`.

`client` and `pc` are batch jobs: they run for `--duration` seconds and exit on their own, printing their statistics before returning; `Ctrl+C` also stops `pc` early if needed.
