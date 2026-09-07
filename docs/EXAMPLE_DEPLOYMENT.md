# Example Deployment

This walks through a complete deployment on a single 8-core machine: a shared `order` + `storage` setup running the co-located Zipkat key-value store. ZipKVS client can be found in the `retwis` branch.
> See [Running Each Module](RUNNING.md) for the full argument reference for every binary used below.

This example assumes a single machine with 8 CPU cores (0–7) and one RDMA device named `mlx5_0`. For a multi-machine deployment, replace `127.0.0.1` in each `--order`/`--server` address with the reachable IP of the machine actually running that service, and adjust the `--cpu`/`--*_cpus` values to fit each machine's own core layout.

## 1. Shared setup: ordering + storage

Start the ordering server (CPU 0):

```bash
./build/order --device=mlx5_0 --gid=1 --cpu=0
```

Then start two storage replicas for shard 0, each with its own client-facing CPU and TCP port (CPUs 1–2), each holding 1,000,000 Zipkat keys:

```bash
./build/storage \
  --device=mlx5_0 --gid=1 \
  --client_cpus=1 \
  --order=127.0.0.1:6666 \
  --replica_id=0 \
  --server=7777 \
  --shard_id=0 \
  --num_keys=1000000

./build/storage \
  --device=mlx5_0 --gid=1 \
  --client_cpus=2 \
  --order=127.0.0.1:6666 \
  --replica_id=1 \
  --server=7778 \
  --shard_id=0 \
  --num_keys=1000000
```

