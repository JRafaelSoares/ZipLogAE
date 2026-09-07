# Running Each Module

There are four binaries: `order`, `storage`, `client` (the append benchmark, built from `apps/append`), and `pc` (the producer-consumer benchmark, built from `apps/pc`). A minimal deployment needs at least one `order`, one `storage`, and one `client` or `pc` instance; run each in its own terminal/host.

All four share the following network-related arguments. `--address`/`--port` together form the local eRPC endpoint this process listens on, so every process on the same host needs a unique `--port`:

| Argument | Description | Default |
|---|---|---|
| `--address` | Local IP address to bind this process's eRPC endpoint to. | `127.0.0.1` |
| `--numa` | NUMA node to use for memory allocation. `-1` disables NUMA-aware allocation. | `-1` |
| `--port` | Local eRPC port to bind to. The default of `1` for `storage`/`client`/`pc` is a leftover from an earlier RDMA-port-index scheme and is a privileged, easily-colliding port — always pass an explicit, unique `--port` for these three. | `6666` for `order`; `1` for `storage`, `client`, `pc` |

Pass `-h`/`--help` to any binary to see this same information from the tool itself.

### `order` — the ordering server (the Zipper)

Start this first; `storage`, `client`, and `pc` all connect to it via `--order=<address>:<port>` (its own `--address`/`--port`).

```bash
./build/out/bin/order --cpu=<cpu> [--address=<addr>] [--port=<port>] [--numa=<numa>]
```

| Argument | Description | Required? |
|---|---|---|
| `--cpu` | CPU core to pin the ordering server's processing thread to. | ✅ |

### `storage` — the storage server

Connects to `order`. Run one instance per replica of each shard.

```bash
./build/out/bin/storage \
  --client_cpus=<cpu>,<cpu> \
  --order=<order-host>:<order-port> \
  --replica_id=<replica_id> \
  --shard_id=<shard_id> \
  --timeout=<nanoseconds> \
  [--address=<addr>] [--port=<port>] [--numa=<numa>] \
  [--subscriber_cpus=<cpu>,<cpu>]
```

| Argument | Description | Required? |
|---|---|---|
| `--client_cpus` | Comma-separated list of CPU cores to run client-facing handler threads on. | ✅ |
| `--order` | Address (`host:port`) of the ordering server. | ✅ |
| `--replica_id` | ID of this replica within its shard. | ✅ |
| `--shard_id` | ID of the shard this storage server belongs to. | ✅ |
| `--timeout` | Timeout, in nanoseconds, for batching log entries before flushing them to subscribers. | ✅ |
| `--subscriber_cpus` | Comma-separated list of CPU cores to run subscriber-facing threads on. If omitted, this replica serves no subscribers. | ❌ |

### `client` — the append benchmark client

Connects to `order` and to the `storage` replicas of a shard, and runs an append benchmark for a fixed duration.

```bash
./build/out/apps/append/client \
  --bench_cpu=<cpu> \
  --client_cpu=<cpu> \
  --client_id=<client_id> \
  --order=<order-host>:<order-port> \
  --servers=<storage-host:port>,<storage-host:port> \
  --shard_id=<shard_id> \
  [--address=<addr>] [--port=<port>] [--numa=<numa>] \
  [--burst=<burst>] [--duration=<seconds>] [--failures=<failures>] [--size=<bytes>] \
  [--min_fraction=<fraction>] [--max_fraction=<fraction>]
```

| Argument | Description | Required? |
|---|---|---|
| `--bench_cpu` | CPU core to pin the benchmark's request-generation thread to. | ✅ |
| `--client_cpu` | CPU core to pin the client's own processing thread to. | ✅ |
| `--client_id` | Unique ID for this client. | ✅ |
| `--order` | Address (`host:port`) of the ordering server. | ✅ |
| `--servers` | Comma-separated addresses (`host:port`) of the storage replicas for this client's shard. | ✅ |
| `--shard_id` | ID of the shard this client sends requests to. | ✅ |
| `--burst` | Maximum number of concurrent in-flight requests. | ❌ (default `1`) |
| `--duration` | Duration to run the benchmark for, in seconds. | ❌ (default `10`) |
| `--failures` | Number of storage-server failures to tolerate (requires acks from `failures + 1` replicas). | ❌ (default `1`) |
| `--size` | Size of each request's payload, in bytes. | ❌ (default `8`) |
| `--min_fraction` | Fraction of pre-allocated log slots below which the client asks the Zipper for fewer slots next epoch. | ❌ (default `0.95`) |
| `--max_fraction` | Fraction of pre-allocated log slots above which the client asks the Zipper for more slots next epoch. | ❌ (default `0.99`) |

The client self-tunes its append rate every epoch via `--min_fraction`/`--max_fraction` — there's no manual rate flag.

### `pc` — the producer-consumer benchmark

Runs a client (producer) and a subscriber (consumer) in the same process, to measure end-to-end append-to-delivery latency. Connects to `order` and to the `storage` replicas of a shard just like `client` does.

```bash
./build/out/apps/pc/pc \
  --client_cpu=<cpu> \
  --client_id=<client_id> \
  --order=<order-host>:<order-port> \
  --servers=<storage-host:port>,<storage-host:port> \
  --shard_id=<shard_id> \
  --subscriber_cpus=<cpu>,<cpu> \
  --subscriber_id=<subscriber_id> \
  [--address=<addr>] [--port=<port>] [--numa=<numa>] \
  [--duration=<seconds>] [--rate=<msgs_per_sec>] [--size=<bytes>] \
  [--failures=<failures>] [--min_fraction=<fraction>] [--max_fraction=<fraction>]
```

| Argument | Description | Required? |
|---|---|---|
| `--client_cpu` | CPU core to pin the producer's client thread to. | ✅ |
| `--client_id` | Unique ID for the internal client. | ✅ |
| `--order` | Address (`host:port`) of the ordering server. | ✅ |
| `--servers` | Comma-separated addresses (`host:port`) of the storage replicas for this shard. | ✅ |
| `--shard_id` | ID of the shard to append to and subscribe from. | ✅ |
| `--subscriber_cpus` | Comma-separated list of CPU cores to run the consumer's subscriber thread(s) on. | ✅ |
| `--subscriber_id` | ID of the internal subscriber. | ✅ |
| `--duration` | Duration to run the benchmark for, in seconds. | ❌ (default `10`) |
| `--rate` | Fixed rate, in messages/sec, at which the producer issues requests. | ❌ (default `50000`) |
| `--size` | Size of each request's payload, in bytes. | ❌ (default `1024`) |
| `--failures` | Number of storage-server failures to tolerate. | ❌ (default `1`) |
| `--min_fraction`, `--max_fraction` | Slot-fraction thresholds for the internal client's rate estimation; same meaning as for `client` above. | ❌ (defaults `0.95`, `0.99`) |

`pc` prints separate producer and consumer statistics: the producer's append latency, and the consumer's end-to-end append-to-delivery latency.

For a worked, end-to-end deployment using these binaries together, see [Example Deployment](EXAMPLE_DEPLOYMENT.md).
