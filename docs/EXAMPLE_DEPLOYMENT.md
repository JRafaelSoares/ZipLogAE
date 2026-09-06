# Example Deployment

This walks through a complete ZipLog deployment on a single 8-core machine, running one of two example workloads on top of a shared `order` + `storage` setup:

- an **append benchmark** (a `client` hammering the log, with a `subscriber` consuming it), or
- the **linearizable lock service** (a small application built on top of the shared log).

The two workloads are alternatives to each other — run one at a time on top of the same `order`/`storage` deployment, not both simultaneously.

> See [Running Each Module](RUNNING.md) for the full argument reference for every binary used below.

This example assumes a single machine with 8 CPU cores (0–7) and one RDMA device named `mlx5_0`. For a multi-machine deployment, replace `127.0.0.1` in each `--order`/`--servers` address with the reachable IP of the machine actually running that service, and adjust the `--cpu`/`--*_cpus` values to fit each machine's own core layout.

## 1. Shared setup: ordering + storage

Start the ordering server (CPU 0):

```bash
./build/order --device=mlx5_0 --gid=1 --cpu=0
```

Then start two storage replicas for shard 0, each with its own client-facing and subscriber-facing CPU and TCP port (CPUs 1–4):

```bash
./build/storage \
  --device=mlx5_0 --gid=1 \
  --client_cpus=1 \
  --order=127.0.0.1:6666 \
  --replica_id=0 \
  --server=7777 \
  --shard_id=0 \
  --subscriber_cpus=2

./build/storage \
  --device=mlx5_0 --gid=1 \
  --client_cpus=3 \
  --order=127.0.0.1:6666 \
  --replica_id=1 \
  --server=7778 \
  --shard_id=0 \
  --subscriber_cpus=4
```

This leaves CPUs 5–7 free for whichever of the two workloads below you run next.

## 2. Append benchmark

Start a subscriber to consume the log (CPUs 5–6):

```bash
./build/subscriber \
  --device=mlx5_0 --gid=1 \
  --application_cpus=5 \
  --order=127.0.0.1:6666 \
  --polling_cpus=6 \
  --subscriber_id=0
```

Then run the append benchmark client for 10 seconds (CPU 7):

```bash
./build/client \
  --device=mlx5_0 --gid=1 \
  --client_id=0 \
  --cpu=7 \
  --duration=10 \
  --order=127.0.0.1:6666 \
  --shard_id=0
```

When the client finishes, it prints final throughput/latency statistics to the log; the subscriber logs its own periodic throughput while the benchmark is running.

## 3. Linearizable lock service

As an alternative to the append benchmark above (on the same shared `order`/`storage` deployment), run the lock service instead, using CPUs 5–7 for its internal client and subscriber threads:

```bash
./build/lock_service \
  --device=mlx5_0 --gid=1 \
  --client_id=0 \
  --cpu=5 \
  --order=127.0.0.1:6666 \
  --shard_id=0 \
  --application_cpus=6 \
  --polling_cpus=7 \
  --subscriber_id=0 \
  --num_clients=1
```

This runs a single lock client (`--num_clients=1`) acquiring and releasing locks over the default 10 lock keys for the default 10 seconds; pass `--num_lock_keys`/`--time` to change either.

## Stopping

Every binary installs a `SIGINT` handler and shuts down cleanly on `Ctrl+C`; stop `client`/`subscriber`/`lock_service` first, then `storage`, then `order`.
