import glob
import os
import shutil
import stat

from itertools import chain
from shlex import quote
from concurrent.futures import ThreadPoolExecutor
import fnmatch


def _transport_backend(config):
    # Keep existing behavior unless explicitly configured.
    return config.get("transport", "erpc")

def start_order(deployment_config, config, connections):
    assert "order" in connections and len(connections["order"]) == 1, "Order is not initialized"
    zip_dir = deployment_config["zip_dir"]
    log_dir = deployment_config["logdir"]

    flags = {
        "--address": config["order"]["addr"][0],
        "--cpu" : config["order"]["cpu"],
        "--port" : config["order"]["port"]
    }
    command = f"cd {zip_dir} && echo 2048 | sudo tee /proc/sys/vm/nr_hugepages && sudo ./build/out/bin/order "
    command +=" ".join(f"{f} {v}" for f, v in flags.items()) + f" > {log_dir}/order.log 2>&1 "

    order_address = connections["order"][0].addr
    print(f"Starting Order on {order_address}")
    print(command)
    connections["order"][0].exec_command(command)

# TODO - No replicas right now
def start_storages(deployment_config, config, connections):
    assert "storage" in connections and len(connections["storage"]) > 0, "Storages are not initialized"
    assert "order" in connections and len(connections["order"]) == 1, "Order must be initialized"
    zip_dir = deployment_config["zip_dir"]
    log_dir = deployment_config["logdir"]

    order_ip = config["order"]["addr"][0]
    order_port = config["order"]["port"]
    flags = {
        "--port" : config["storage"]["port"],
        "--client_cpus" : ",".join(str(f) for f in config["storage"]["client_cpus"]),
        "--order" : f"{order_ip}:{order_port}",
        "--timeout" : config["storage"]["timeout"]
    }

    if len(config["storage"]["subscriber_cpus"]) > 0:
        flags["--subscriber_cpus"] = ",".join(str(f) for f in config["storage"]["subscriber_cpus"])

    command = f"cd {zip_dir} && echo 2048 | sudo tee /proc/sys/vm/nr_hugepages && sudo ./build/out/bin/storage "
    command +=" ".join(f"{f} {v}" for f, v in flags.items()) + " "

    failures = config["failures"]+1

    for i, c in enumerate(connections["storage"]):
        shard_id = i // (failures)
        replica_id = i % (failures)
        print(f"Starting storage {shard_id} replica {replica_id} on {c.addr}")
        storage_command = command + f"--shard_id {shard_id} --replica_id {replica_id} --address {c.addr} > {log_dir}/server_{shard_id}_{replica_id}.log 2>&1 "
        print(storage_command)
        c.exec_command(storage_command)

def start_append_client(deployment_config, config, exp_config, connections, num_clients):
    assert "client" in connections and len(connections["client"]) > 0, "Clients are not initialized"
    assert "order" in connections and len(connections["order"]) == 1, "Order must be initialized"

    zip_dir = deployment_config["zip_dir"]
    log_dir = deployment_config["logdir"]

    order_ip = config["order"]["addr"][0]
    order_port = config["order"]["port"]

    flags = {
        "--order" : f"{order_ip}:{order_port}",
    }

    command = f"cd {zip_dir} && echo 2048 | sudo tee /proc/sys/vm/nr_hugepages && sudo ./build/out/apps/append/client "

    for f, v in flags.items():
        command += f"{f} {v} "

    for f, v in exp_config.items():
        command += f"{f} {v} "

    srv_port = config["storage"]["port"]
    num_replicas = int(config["failures"])+1
    num_shards = len(config["storage"]["addr"]) // num_replicas

    for i, c in enumerate(connections["client"]):
        for j in range(num_clients):
            client_id = i*num_clients + j
            print(f"Starting client {client_id} on {c.addr}")
            shard_id = client_id % num_shards

            port = int(config["client"]["port"]) + client_id

            srv_replicas = config["storage"]["addr"][shard_id*num_replicas:(shard_id+1)*num_replicas]
            srv_addresses = ",".join(f"{srv}:{srv_port}" for srv in srv_replicas)
            bench_cpu = config["client"]["bench_cpu"][client_id]
            client_cpu = config["client"]["client_cpu"][client_id]
            client_command = command + f"--address {c.addr} --port {port} --bench_cpu {bench_cpu} --client_cpu {client_cpu} --client_id {client_id} --shard_id {shard_id} --servers {srv_addresses} > {log_dir}/append_client_{client_id}.log 2>&1 "

            print(client_command)
            c.exec_command(client_command)

def start_producer_consumer(deployment_config, config, exp_config, connections):
    assert "pc" in connections and len(connections["pc"]) > 0, "PC are not initialized"
    assert "order" in connections and len(connections["order"]) == 1, "Order must be initialized"

    zip_dir = deployment_config["zip_dir"]
    log_dir = deployment_config["logdir"]

    order_ip = config["order"]["addr"][0]
    order_port = config["order"]["port"]
    srv_port = config["storage"]["port"]

    flags = {
        "--order" : f"{order_ip}:{order_port}",
        "--port" : config["pc"]["port"],
        "--client_cpu" : config["pc"]["client_cpu"],
        "--subscriber_cpus" : ",".join(str(f) for f in config["pc"]["subscriber_cpus"]),
        "--servers" : ",".join(f"{srv_ip}:{srv_port}" for srv_ip in config["storage"]["addr"]),
        "--failures" : config["failures"]
    }

    command = f"cd {zip_dir} && echo 2048 | sudo tee /proc/sys/vm/nr_hugepages && sudo ./build/out/apps/pc/pc "

    for f, v in flags.items():
        command += f"{f} {v} "

    for f, v in exp_config.items():
        command += f"{f} {v} "

    for i, c in enumerate(connections["pc"]):
        print(f"Starting pc on {c.addr}")
        client_command = command + f"--address {c.addr} --client_id {i} --shard_id {i} --subscriber_id {i} > {log_dir}/pc_{i}.log 2>&1 "

        print(client_command)
        c.exec_command(client_command)

def start_subscriber(deployment_config, config, connections):
    assert "subscriber" in connections and len(connections["subscriber"]) > 0, "Subscribers are not initialized"
    assert "order" in connections and len(connections["order"]) == 1, "Order must be initialized"

    zip_dir = deployment_config["zip_dir"]
    log_dir = deployment_config["logdir"]
    order_con = connections["order"][0]
    order_ip = config["order"]["addr"][0]
    order_port = config["order"]["port"]

    flags = {
        "--port" : config["subscriber"]["port"],
        "--order" : f"{order_ip}:{order_port}",
        "--application_cpus" : ",".join(str(f) for f in config["subscriber"]["application_cpus"]),
        "--polling_cpus" : ",".join(str(f) for f in config["subscriber"]["polling_cpus"]),
        "--failures" : config["failures"],
        "--ports" : ",".join(str(p) for p in config["subscriber"]["ports"]),
        "--transport": _transport_backend(config),
    }

    command = f"cd {zip_dir} && echo 2048 | sudo tee /proc/sys/vm/nr_hugepages && sudo ./build/subscriber "

    for f, v in flags.items():
        command += f"{f} {v} "

    for i, c in enumerate(connections["subscriber"]):
        print(f"Starting subscriber {i} on {c.addr}")
        subscriber_command = command + f"--address {c.addr} --subscriber_id {i} > {log_dir}/subscriber_{i}.log 2>&1 "
        print(subscriber_command)
        c.exec_command(subscriber_command)

def clear_server_logs(logdir, connections):
    for c in chain(connections["order"], connections["storage"]):
        safe_logdir = quote(logdir)
        assert safe_logdir != "" and safe_logdir != "/", "LOGDIR MUST BE SET TO SOMETHING"
        c.exec_command(f"rm -rf {safe_logdir} && mkdir {safe_logdir}")
    print("Cleared Order and Storage Nodes")

def clear_client_logs(logdir, connections):
    nodes = ["client", "subscriber"]

    for n in nodes:
        if n not in connections:
            continue

        for i, c in enumerate(connections[n]):
            safe_logdir = quote(logdir)
            assert safe_logdir != "" and safe_logdir != "/", "LOGDIR MUST BE SET TO SOMETHING"
            c.exec_command(f"rm -rf {safe_logdir} && mkdir {safe_logdir}", True)

        print(f"Cleared {n} Nodes")


def collect_logs(data_logdir, server_data_path, connections):
    os.makedirs(server_data_path, exist_ok=True)

    def obtain_log(c):
        try:
            attr = c.sftp_client.stat(data_logdir)
            if stat.S_ISDIR(attr.st_mode):
                for filename in c.sftp_client.listdir(data_logdir):
                    print(f"{filename}")

                    if fnmatch.fnmatch(filename, "*.log"):
                        print(f"{data_logdir}/{filename}")

                        local_path = os.path.join(server_data_path, filename)
                        c.sftp_client.get(
                            f"{data_logdir}/{filename}",
                            local_path
                        )
            else:
                print(f"{data_logdir} is not a dir")
        except FileNotFoundError:
            # Directory does not exist on remote
            pass


    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(obtain_log, connections))


def process_logs(server_data_path, exp_configs):
    # Process logs to extract metrics
    os.makedirs(server_data_path, exist_ok=True)

    avg_tput = 0.0
    avg_latency = 0.0
    p50 = 0.0
    p95 = 0.0
    p99 = 0.0
    p999 = 0.0
    num_files = 0
    for filename in os.listdir(server_data_path):
        if filename.startswith("append_client") and filename.endswith(".log"):
            num_files += 1
            with open(os.path.join(server_data_path, filename), 'r') as file:
                for line in file:
                    if "ops/sec" in line:
                        avg_tput += float(line.split()[-2])
                    if "#[Mean" in line:
                        avg_latency += float(line.split()[2].split(',')[0]) / 1000
                    if "p50:" in line:
                        p50 += float(line.split()[1]) / 1000
                    if "p95:" in line:
                        p95 += float(line.split()[1]) / 1000
                    if "p99:" in line:
                        p99 += float(line.split()[1]) / 1000
                    if "p99.9" in line:
                        p999 += float(line.split()[1]) / 1000

    if num_files > 0:
        avg_latency /= num_files
        p50 /= num_files
        p95 /= num_files
        p99 /= num_files
        p999 /= num_files

    row = {
        "num_files": num_files,
        "avg_tput": avg_tput,
        "avg_latency": avg_latency,
        "p50": p50,
        "p95": p95,
        "p99": p99,
        "p999": p999,
    }

    if isinstance(exp_configs, dict):
        row.update({f"{key}": value for key, value in exp_configs.items()})

    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("process_logs requires pandas to write parquet output") from exc

    output_path = os.path.join(server_data_path, "processed_logs.parquet")
    pd.DataFrame([row]).to_parquet(output_path, index=False)


def wait_client_execution(connections):
    print("Waiting for Clients to complete")

    def wait_result(c):
        c.wait_command()
        print(f"Client {c.addr} is finished")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(wait_result, connections["clients"]))
