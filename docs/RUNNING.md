# Running Each Module

There are five binaries: `order`, `storage`, `client`, `subscriber`, and `lock_service`. A minimal deployment needs at least one `order`, one `storage`, and one `client` (or `subscriber`/`lock_service`) instance; run each in its own terminal/host.

All five share the following network-related arguments:

| Argument | Description | Default |
|---|---|---|
| `--device` | RDMA device name to use (e.g. `mlx5_0`). | `mlx5_0` |
| `--gid` | RDMA device port GID index. Required (must be `>= 0`) if the device's link layer is not native InfiniBand. | `-1` (disabled) |
| `--numa` | NUMA node to use for memory allocation. `-1` auto-detects the device's own NUMA node. | `-1` |
| `--port` | RDMA device port number. | `1` |

Pass `-h`/`--help` to any binary to see this same information from the tool itself.

### `order` — the ordering server

Start this first; `storage`, `client`, `subscriber`, and `lock_service` all connect to it.

```bash
./build/order --device=<device> --gid=<gid> --cpu=<cpu> --server=<port>
```

| Argument | Description | Required? |
|---|---|---|
| `--cpu` | CPU core to pin the ordering server's processing thread to. | ✅ |
| `--server` | TCP port to listen on for incoming client/storage connections. | ❌ (default `6666`) |

### `storage` — the storage server

Connects to `order`. Run one instance per replica of each shard.

```bash
./build/storage \
  --device=<device> --gid=<gid> \
  --client_cpus=<cpu>,<cpu> \
  --order=<order-host>:<order-port> \
  --replica_id=<replica_id> \
  --server=<port> \
  --shard_id=<shard_id> \
  [--subscriber_cpus=<cpu>,<cpu>] \
  [--subscriber_depth=<depth>]
```

| Argument | Description | Required? |
|---|---|---|
| `--client_cpus` | Comma-separated list of CPU cores to run client-facing handler threads on. | ✅ |
| `--order` | Address (`host:port`) of the ordering server to connect to. | ✅ |
| `--replica_id` | ID of this replica within its shard. | ✅ |
| `--server` | TCP port to listen on for incoming connections. | ❌ (default `6666`) |
| `--shard_id` | ID of the shard this storage server belongs to. | ✅ |
| `--subscriber_cpus` | Comma-separated list of CPU cores to run subscriber-facing threads on. | ❌ |
| `--subscriber_depth` | Maximum number of outstanding entries to subscribers. | ❌ (default `8`) |

### `client` — the append benchmark client

Connects to `order` and to the `storage` replicas of a shard, and runs an append benchmark for a fixed duration.

```bash
./build/client \
  --device=<device> --gid=<gid> \
  [--burst=<burst>] \
  --client_id=<client_id> \
  --cpu=<cpu> \
  --duration=<seconds> \
  [--failures=<failures>] \
  --order=<order-host>:<order-port> \
  --shard_id=<shard_id> \
  [--size=<bytes>] \
  [--rate=<iops> | --target=<t> --proportional=<p> --integral=<i> --derivative=<d>]
```

| Argument | Description | Required? |
|---|---|---|
| `--burst` | Maximum number of concurrent in-flight requests. | ❌ (default `1`) |
| `--client_id` | Unique ID for this client. | ✅ |
| `--cpu` | CPU core to pin the client's request-processing thread to. | ✅ |
| `--duration` | Duration to run the benchmark for, in seconds. | ✅ |
| `--failures` | Number of storage-server failures to tolerate (requires acks from `failures + 1` replicas). | ❌ (default `1`) |
| `--order` | Address (`host:port`) of the ordering server. | ✅ |
| `--shard_id` | ID of the shard this client sends requests to. | ✅ |
| `--size` | Size of each request's payload, in bytes. | ❌ (default `8`) |
| `--rate` | Fixed rate to run the client at, in IOPS. If omitted, the client instead uses a PID controller to adapt its rate. | ❌ |
| `--target` | Target fraction of used slots for PID rate estimation. | ❌ (default `0.9`) |
| `--proportional` | Proportional constant for PID rate estimation. | ❌ (default `500`) |
| `--integral` | Integral constant for PID rate estimation. | ❌ (default `40`) |
| `--derivative` | Derivative constant for PID rate estimation. | ❌ (default `30`) |

### `subscriber` — the log subscriber

Connects to `order` (and, transitively, the storage replicas) and streams delivered log entries to application threads, reporting periodic throughput/latency statistics.

```bash
./build/subscriber \
  --device=<device> --gid=<gid> \
  --application_cpus=<cpu>,<cpu> \
  [--failures=<failures>] \
  --order=<order-host>:<order-port> \
  --polling_cpus=<cpu>,<cpu> \
  [--sequential] \
  --subscriber_id=<subscriber_id>
```

| Argument | Description | Required? |
|---|---|---|
| `--application_cpus` | Comma-separated list of CPU cores to run subscriber application threads on. | ✅ |
| `--failures` | Number of failures to tolerate. | ❌ (default `1`) |
| `--order` | Address (`host:port`) of the ordering server. | ✅ |
| `--polling_cpus` | Comma-separated list of CPU cores to run subscriber polling threads on. | ✅ |
| `--sequential` | Whether to deliver entries in (global) order. | ❌ (default `true`) |
| `--subscriber_id` | ID of the subscriber. | ✅ |

### `lock_service` — the linearizable lock service

Combines a Ziplog client and subscriber to implement a linearizable distributed lock on top of the shared log; connects to `order` like `client`/`subscriber` do. Each instance drives one client acquiring/releasing locks over `--num_lock_keys` keys for `--time` seconds.

```bash
./build/lock_service \
  --device=<device> --gid=<gid> \
  --client_id=<client_id> \
  --cpu=<cpu> \
  [--failures=<failures>] \
  --order=<order-host>:<order-port> \
  --shard_id=<shard_id> \
  [--rate=<iops> | --target=<t> --proportional=<p> --integral=<i> --derivative=<d>] \
  --application_cpus=<cpu>,<cpu> \
  --polling_cpus=<cpu>,<cpu> \
  --subscriber_id=<subscriber_id> \
  --num_clients=<num_clients> \
  [--num_lock_keys=<num_keys>] [--time=<seconds>]
```

| Argument | Description | Required? |
|---|---|---|
| `--client_id` | Unique ID for this lock client. | ✅ |
| `--cpu` | CPU core to pin the internal Ziplog client thread to. | ✅ |
| `--failures` | Number of failures to tolerate. | ❌ (default `1`) |
| `--order` | Address (`host:port`) of the ordering server. | ✅ |
| `--shard_id` | ID of the shard the internal Ziplog client sends requests to. | ✅ |
| `--rate` | Fixed rate to run the underlying Ziplog client at, in IOPS. If omitted, uses PID rate estimation (see `client` above). | ❌ |
| `--target`, `--proportional`, `--integral`, `--derivative` | PID rate-estimation constants; same meaning as for `client`. | ❌ (defaults `0.9`, `500`, `40`, `30`) |
| `--application_cpus` | Comma-separated list of CPU cores for the internal subscriber's application threads. | ✅ |
| `--polling_cpus` | Comma-separated list of CPU cores for the internal subscriber's polling threads. | ✅ |
| `--subscriber_id` | ID of the internal subscriber. | ✅ |
| `--num_clients` | Total number of lock clients in the deployment. | ✅ |
| `--num_lock_keys` | Total number of lock keys to distribute clients over. | ❌ (default `10`) |
| `--time` | How long to run the lock-acquire/release loop for, in seconds. | ❌ (default `10`) |

For a worked, end-to-end deployment using these binaries together, see [Example Deployment](EXAMPLE_DEPLOYMENT.md).
