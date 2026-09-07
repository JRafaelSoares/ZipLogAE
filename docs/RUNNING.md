# Running Each Module

There are four binaries: `order`, `storage`, `client`, and `subscriber`. A minimal ZipKVS deployment needs one `order` instance and one `storage` instance per shard replica; `client` and `subscriber` are the original Ziplog append-log benchmark tools and are optional (see [Example Deployment](EXAMPLE_DEPLOYMENT.md) for a worked example using `client`). Run each instance in its own terminal/host.

> **Note:** this build is always compiled with the co-located ZipKVS key-value store (`-DCOLOCATED_ZIPKAT` in the `Makefile`). This adds the `--get_cpus`/`--num_keys` arguments to `storage` below, and also means a `storage` instance cannot accept `subscriber` connections — it asserts and aborts if one tries to connect.

All four share the following network-related arguments:

| Argument | Description | Default |
|---|---|---|
| `--device` | RDMA device name to use (e.g. `mlx5_0`). | `mlx5_0` |
| `--gid` | RDMA device port GID index. Required (must be `>= 0`) if the device's link layer is not native InfiniBand. | `-1` (disabled) |
| `--port` | RDMA device port number. | `1` |

Pass `-h`/`--help` to any binary to see this same information from the tool itself.

### `order` — the ordering server

Start this first; `storage` and `client` connect to it.

```bash
./build/order --device=<device> --gid=<gid> --cpu=<cpu> --server=<port>
```

| Argument | Description | Required? |
|---|---|---|
| `--cpu` | CPU core to pin the ordering server's processing thread to. | ✅ |
| `--server` | TCP port to listen on for incoming client/storage connections. | ❌ (default `6666`) |

### `storage` — the storage server

Connects to `order`. Run one instance per replica of each shard. Hosts the co-located ZipKVS key-value store.

```bash
./build/storage \
  --device=<device> --gid=<gid> \
  --order=<order-host>:<order-port> \
  --replica_id=<replica_id> \
  --server=<port> \
  --shard_id=<shard_id> \
  --num_keys=<num_keys> \
  [--client_cpus=<cpu>,<cpu>] \
  [--get_cpus=<cpu>,<cpu>] \
  [--subscriber_cpus=<cpu>,<cpu>]
```

| Argument | Description | Required? |
|---|---|---|
| `--client_cpus` | Comma-separated list of CPU cores to run client-facing handler threads on. | ❌ |
| `--get_cpus` | Comma-separated list of CPU cores to run ZipKVS GET-request handler threads on. | ❌ |
| `--num_keys` | Number of keys in the co-located ZipKVS key-value store. | ✅ |
| `--order` | Address (`host:port`) of the ordering server to connect to. | ✅ |
| `--replica_id` | ID of this replica within its shard (`0`–`9`). | ✅ |
| `--server` | TCP port to listen on for incoming connections. | ❌ (default `6666`) |
| `--shard_id` | ID of the shard this storage server belongs to (`0`, as this build is compiled with a single shard). | ✅ |
| `--subscriber_cpus` | Comma-separated list of CPU cores to run subscriber-facing threads on. Parsed but unusable in this build — see the note above. | ❌ |

For a worked, end-to-end deployment using these binaries together, see [Example Deployment](EXAMPLE_DEPLOYMENT.md).
