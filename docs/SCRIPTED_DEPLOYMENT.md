# Scripted Deployment

[`scripts/commands/commands.py`](../scripts/commands/commands.py) automates running the append or producer-consumer benchmark across a set of machines over SSH: it starts `order`, `storage`, and the benchmark processes on the right hosts, waits for the run to finish, then collects and (partially) post-processes their logs. This is an alternative to the fully-manual, single-machine walkthrough in [Example Deployment](EXAMPLE_DEPLOYMENT.md) — reach for it for multi-host runs, or repeated/parametrized ones (see [Sweeping parameters](#sweeping-parameters) below).

## Requirements

- Python 3 with the packages in [`scripts/grid5000/requirements.txt`](../scripts/grid5000/requirements.txt), plus `pandas` and a parquet engine (used by the log post-processing step, but not currently listed there):

  ```bash
  pip install -r scripts/grid5000/requirements.txt pandas pyarrow
  ```

- Passwordless SSH (key-based) as some `user` from the control machine (where you run `commands.py`) to every host in the deployment, and passwordless `sudo` for that user on each host — every ziplog binary is started as `sudo ./build/out/...` over a non-interactive SSH channel, so a `sudo` password prompt would just hang forever.
- ZipLog already built (see the top-level [README](../README.md#building)) at the same path on every host that runs a binary — the scripts `cd` into that path and run `./build/out/...` directly.
- Every host must satisfy the RDMA/`mlx5` hardware requirement from the main README — these scripts don't relax it, they just automate starting the same binaries documented in [Running Each Module](RUNNING.md) over SSH.

## The two config files

Every subcommand takes the same three positional arguments:

```bash
python3 scripts/commands/commands.py <append_bench|pc_bench> <deployment_config.json> <exp_config.json> <exp_name> [options...]
```

- `<deployment_config.json>` — control-plane details: who to SSH as, and where things live. Independent of which benchmark you run.
- `<exp_config.json>` — the actual topology for this run: which hosts run `order`/`storage`/`client`/`pc`, and on which ports/CPUs.
- `<exp_name>` — a label for this run; results land in `<script_dir>/results/<exp_name>/`.

### Deployment config

```json
{
  "user": "rasoares",
  "logdir": "/tmp/logdir",
  "script_dir": "/home/rasoares/ZipLogERPC/scripts",
  "zip_dir": "/home/rasoares/ZipLogERPC"
}
```

| Field | Description |
|---|---|
| `user` | SSH (and `sudo`) user on every host in the deployment. |
| `logdir` | Directory on **each remote host** where that host's process(es) write their `.log` file(s); wiped and recreated at the start of every run. |
| `script_dir` | Path to this repo's `scripts/` directory **on the control machine** — used only to compute `results/<exp_name>/`. |
| `zip_dir` | Path to the built ziplog repo **on each remote host** (binaries are run from `<zip_dir>/build/out/...`). |

### Experiment config

Shared by both benchmarks — `order`, `storage`, `failures` — plus a benchmark-specific `client` or `pc` section (see below). Field meanings mirror the binaries' own arguments in [Running Each Module](RUNNING.md).

| Field | Description |
|---|---|
| `order.addr` | One-element list; the ordering server's host (only one `order` process is supported). |
| `order.cpu` | `order`'s `--cpu`. |
| `order.port` | `order`'s `--port` — also what every other process's `--order` points at. |
| `storage.addr` | List of storage hosts, grouped into shards of `failures + 1` consecutive entries each (entries `0..failures` are shard 0's replicas, the next `failures + 1` are shard 1's, and so on). |
| `storage.port` | `--port` used by **every** storage replica (fine since each runs on a different host). |
| `storage.client_cpus` | `--client_cpus`, shared by every storage replica. |
| `storage.subscriber_cpus` | `--subscriber_cpus`, shared by every storage replica; leave `[]` to run without subscriber threads. |
| `storage.timeout` | `--timeout`, shared by every storage replica. |
| `failures` | Number of storage-replica failures to tolerate; `storage.addr` must have more entries than this. |

#### `client` (for `append_bench`)

| Field | Description |
|---|---|
| `client.addr` | List of hosts to run append clients on. |
| `client.port` | Base `--port`; host `i`'s `j`-th client process uses `port + (i * num_clients + j)`, so multiple clients on one host don't collide. |
| `client.bench_cpu`, `client.client_cpu` | Lists **indexed by global client ID** (`i * num_clients + j`, across all hosts) — must have at least `len(client.addr) * num_clients` entries. |

Clients are assigned shards round-robin (`client_id % num_shards`, where `num_shards = len(storage.addr) / (failures + 1)`), and each client's `--servers` is derived automatically from its shard's replicas — you never set `--servers` yourself.

#### `pc` (for `pc_bench`)

| Field | Description |
|---|---|
| `pc.addr` | List of hosts to run one `pc` process on each — unlike `client`, there's no per-host multiplexing. |
| `pc.port` | `--port`, shared by every `pc` instance. |
| `pc.client_cpu` | `--client_cpu`, shared by every `pc` instance (a single value, not a list). |
| `pc.subscriber_cpus` | `--subscriber_cpus`, shared by every `pc` instance. |

Each `pc` host's `--shard_id`/`--client_id`/`--subscriber_id` are all set to its index in `pc.addr`. **Caveat:** unlike `client`, `pc_bench` doesn't shard `--servers` per host — every `pc` instance is handed the *entire* `storage.addr` list regardless of its shard. This is harmless with a single shard (the common case, including the example below), but means multi-shard `pc_bench` configs need extra care.

## Running `append_bench`

Example experiment config, matching [`scripts/grid5000/config.json`](../scripts/grid5000/config.json):

```json
{
  "order": { "addr": ["10.0.0.1"], "cpu": 0, "port": 31850 },
  "storage": {
    "addr": ["10.0.0.2", "10.0.0.3"],
    "port": 31860,
    "client_cpus": [0],
    "subscriber_cpus": [],
    "timeout": 0
  },
  "client": {
    "addr": ["10.0.0.4"],
    "port": 31860,
    "bench_cpu": [0],
    "client_cpu": [1]
  },
  "failures": 1
}
```

```bash
python3 scripts/commands/commands.py append_bench deployment.json config.json my_run \
  --runtime 10 --burst 1 --size 8 --num_clients 1
```

| Flag | Description | Default |
|---|---|---|
| `--runtime` | Seconds to run the client for (`client`'s `--duration`). | `10` |
| `--burst` | `client`'s `--burst`. | `1` |
| `--size` | `client`'s `--size`. | `8` |
| `--num_clients` | Client processes to run **per host** in `client.addr`. | `1` |
| `--num_shards` | Currently unused — the shard count is always derived from `storage.addr`/`failures`. | `1` |

This starts `order`, waits 4s, starts every `storage` replica, waits 4s, starts all `client` processes, waits `2 * runtime` seconds, then stops everything and collects logs into `<script_dir>/results/my_run/` — including an aggregated `processed_logs.parquet` (throughput and p50/p95/p99/p99.9 latency, averaged across `append_client_*.log` files).

## Running `pc_bench`

Example experiment config, matching [`scripts/grid5000/pc_config.json`](../scripts/grid5000/pc_config.json):

```json
{
  "order": { "addr": ["10.0.0.1"], "cpu": 0, "port": 31850 },
  "storage": {
    "addr": ["10.0.0.2", "10.0.0.3"],
    "port": 31860,
    "client_cpus": [0, 1, 2, 3],
    "subscriber_cpus": [4, 5, 6, 7],
    "timeout": 0
  },
  "pc": {
    "addr": ["10.0.0.4"],
    "port": 31860,
    "client_cpu": 0,
    "subscriber_cpus": [1, 2, 3, 4]
  },
  "failures": 1
}
```

```bash
python3 scripts/commands/commands.py pc_bench deployment.json pc_config.json my_run \
  --runtime 10 --rate 50000 --size 512
```

| Flag | Description | Default |
|---|---|---|
| `--runtime` | Seconds to run the producer for (`pc`'s `--duration`). | `10` |
| `--rate` | `pc`'s `--rate`. | `50000` |
| `--size` | `pc`'s `--size`. | `512` |
| `--num_shards` | Currently unused. | `1` |
| `--num_clients` | Currently unused for `pc_bench` — there's no per-host multiplexing. | `1` |

Same start/stop/collect sequence as `append_bench`, but logs land as `pc_<i>.log` per host. **Note:** the built-in `process_logs` summary only parses `append_client_*.log` files, so the `processed_logs.parquet` it writes for a `pc_bench` run is not useful — read the raw `pc_<i>.log` files instead (they print separate producer/consumer throughput and p50/p95/p99/p99.9 latency).

## Sweeping parameters

[`scripts/experiments/append_bench.sh`](../scripts/experiments/append_bench.sh) and [`scripts/experiments/pc_bench.sh`](../scripts/experiments/pc_bench.sh) show how to loop the commands above over parameter grids (burst/size/client count, or size/rate/duration with repeated trials), naming each run's `<exp_name>` after its parameters so results don't overwrite each other.

## Troubleshooting

- **Run fails silently / just logs `Error: ...`**: `initialize_and_do_command` catches every exception and logs it rather than crashing — check the message for the failed assertion (usually a missing config field, or `storage.addr` too short for `failures`).
- **A remote command seems to hang**: commands run over non-interactive SSH channels; if `sudo` on a host actually prompts for a password, the process will block there indefinitely.
- **PTP-related output during setup**: `setup()` best-effort runs `/tmp/ptp_script.sh` as root on every host to resync clocks; failures here are swallowed and don't stop the run — set that script up if you care about tight clock sync, otherwise it's safe to ignore.
- **Stale processes from a previous run**: if a previous invocation of `commands.py` was killed before it could send its stop signal, its remote `order`/`storage`/`client`/`pc` processes may still be running and holding the ports your next run wants to reuse — check for and kill them manually.
