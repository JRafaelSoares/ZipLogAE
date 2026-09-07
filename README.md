# Running Experiment Suites (`tools/e1_e2.py`)

## Setup

Before running the experiment, some setup is required.
Bellow is a step-by-step list of the required setup before running your first experiment:

1. **Link the ZipLog repository.** Link the syslink in `third_party/ziplog` to ZipKVS's directory. Use the command:
```bash
ln -sfn /path/to/new/ziplog third_party/ziplog  
```
2. **Compile ZipLog code.**
3. **Update host lists with local cluster IP's.** `store/tools/meerkat_benchmarks.py` (lines 113-141) contain the functions holding each components host IP's (`clients()`, `ziplog_order_ips()`/`ziplog_order_servers()`, and `ziplog_storage_servers()`). Edit them according to your own deployment.
4. **Place the binaries at the hosts**
5. **Prepare the shard config file.** Copy `store/tools/f1.shard0.config` into a directory accessible by each machine (pointed by `--config_file_directory`), and edit its `replica` IPs/ports to match where your storage replicas actually listen (each `storage` instance's own `--server` port).
6. **Generate a keys file.** Experiments were done across 10,000,000 keys. You can generate them via:
   ```bash
   python store/tools/key_generator.py 10000000 > keys.txt
   ``` 
7. **Make the key file and shard config file accessible to all hosts.** They will later be accessed via `--config_file_directory` and `--key_file`.
8. **Insert all involved hosts in `tools/init_servers.txt`.** 
9. **Update the USER and PASS (the hosts `sudo` password at each involved host) in `tools/init.sh` and run the script.** This command sets up the `/mnt/log` path and other environment requirements such as `hugepages`.


## Command-line arguments

```bash
python store/tools/e1_e2.py \
  --ziplog_order_binary=<path> \
  --ziplog_order_port=<port> \
  --ziplog_order_cpus=<cpu> \
  --ziplog_storage_binary=<path> \
  --client_binary=<path> \
  [--config_file_directory=<dir>] \
  [--key_file=<path>] \
  [--suite_directory=<dir>]
```

| Argument | Description | Required? | Default |
|---|---|---|---|
| `--ziplog_order_binary` | Path to the `order` binary (same one from [RUNNING.md](RUNNING.md)), run on the order host. | ✅ | |
| `--ziplog_order_port` | TCP port the `order` binary listens on (its `--server`). | ✅ | |
| `--ziplog_order_cpus` | CPU core to pin the `order` binary to (its `--cpu`). | ✅ | |
| `--ziplog_storage_binary` | Path to the `storage` binary, run on each storage host. | ✅ | |
| `--client_binary` | Path to the client benchmark binary (`store/benchmark/retwisClient`). | ✅ | |
| `--config_file_directory` | Directory containing the shard config file (see below). | ❌ | `~/tapir_benchmarks` |
| `--key_file` | Path to the keys file (see below). | ❌ | `~/tapir_benchmarks/keys.txt` |
| `--suite_directory` | Directory results are written into, as a new timestamped subdirectory per run. | ❌ | `~/tmp` |

## Worked example

```bash
mkdir -p ~/tapir_benchmarks ~/tmp
cp store/tools/f1.shard0.config ~/tapir_benchmarks/   # then edit its replica IPs/ports
python store/tools/key_generator.py 10000000 > ~/tapir_benchmarks/keys.txt

python store/tools/e1_e2.py \
  --ziplog_order_binary=/path/to/build/order \
  --ziplog_order_port=6666 \
  --ziplog_order_cpus=0 \
  --ziplog_storage_binary=/path/to/build/storage \
  --client_binary=/path/to/build/retwisClient \
  --config_file_directory=$HOME/tapir_benchmarks \
  --key_file=$HOME/tapir_benchmarks/keys.txt \
  --suite_directory=$HOME/tmp
```

This runs the `e1_and_e2` scalability sweep defined in `e1_e2.py`'s `parameters_list` (fixed `zipf_coefficient=0`, 30s runs with 10s warmup, each combination repeated 3 times). For each run it kills any leftover `order`/`storage`/client processes, starts `order` and `storage` fresh, runs the clients, copies logs back, and appends a row to `<suite_directory>/<timestamp>_<random>_e1_and_e2/results.csv`.

