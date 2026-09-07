import time
from time import sleep
import logging
from datetime import datetime

from common import *
import json
from connection import Connection
from itertools import chain
import subprocess

LOG = logging.getLogger("admin")
LOG.setLevel(logging.INFO)  # Set the minimum level to INFO
# Create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # Also set level on the handler

# Create a simple formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
LOG.addHandler(ch)

import argparse


# Generic Command class
class Command:
    """Base class for a command"""

    NAME = "<not_implemented>"
    HELP = ""
    DESCRIPTION = ""

    def create_subparser(self, subparsers):
        parser = subparsers.add_parser(
            self.NAME, description=self.DESCRIPTION, help=self.HELP
        )
        parser.set_defaults(run=self.initialize_and_do_command)
        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser):
        pass

    def initialize_and_do_command(self, args):
        pass


def initialize_and_run_commands(description, commands, args=None):
    parser = argparse.ArgumentParser(description=description)
    subparsers = parser.add_subparsers(dest="command name")
    subparsers.required = True

    for command in commands:
        command().create_subparser(subparsers)

    parsed_args = parser.parse_args(args)
    parsed_args.run(parsed_args)


class AdminCommand(Command):

    def __init__(self):
        self.connections = {}
        self.config = None
        self.config_name = ""
        self.deployment_config = None
        self.user = ""
        self.logdir = ""
        self.script_dir = ""
        self.result_path = ""
        self.data_log_dir = ""

    def add_arguments(self, parser):
        parser.add_argument(
            "deployment_config",
            nargs="?",
            default="",
            metavar="dep_config_file",
            help="Path to a config file",
        )

        parser.add_argument(
            "exp_config",
            nargs="?",
            default="",
            metavar="exp_config_file",
            help="Path to a config file",
        )

        parser.add_argument(
            "exp_name",
            help="Experiment name for result directory"
        )

    def validate_deployment_config(self):
        assert self.deployment_config is not None, "Error: No deployment config available to validate"
        required_files = [
            "user",
            "logdir",
            "zip_dir",
            "script_dir"
        ]

        for f in required_files:
            assert f in self.deployment_config, f"Error: no {f} in deployment config"

        self.user = self.deployment_config["user"]
        self.logdir = self.deployment_config["logdir"]
        self.script_dir = self.deployment_config["script_dir"]

    def validate_config(self):
        assert self.config is not None, "Error: No config available to validate"

        required_nodes = [
            "order",
            "storage",
            "failures"
        ]

        for n in required_nodes:
            assert n in self.config, f"Error: no {n} nodes in config"

        assert len(self.config["storage"]["addr"]) > self.config[
            "failures"], "Number of server addresses is not enough to fulfill fault tolerance requirement"

        order_fields = [
            "addr",
            "cpu",
            "port"
        ]

        storage_fields = [
            "addr",
            "port",
            "client_cpus",
            "subscriber_cpus",
            "timeout"
        ]

        field_mapping = {
            "order" : order_fields,
            "storage": storage_fields,
        }

        for node, fields in field_mapping.items():
            # For the case of subscribers, which are not required
            if node not in self.config:
                continue

            for f in fields:
                assert f in self.config[node], f"Error: {f} not in {node} config"

        assert len(self.config["storage"]["client_cpus"]) > 0, "Storage nodes must have at least 1 client cpu"

    def initialize_and_do_command(self, args):
        try:
            self.load_config(args)
            self.initialize(args)
            self.setup(args)
            self.do_command(args)
        except Exception as e:
            LOG.error(f"Error: {e}")
        except KeyboardInterrupt:
            LOG.warning("Process interrupted by user")
        finally:
            self.clean_connections()

    def load_config(self, args):
        with open(args.deployment_config, "r") as f:
            self.deployment_config = json.load(f)

        self.validate_deployment_config()

        self.config_name = os.path.basename(args.exp_config)
        with open(args.exp_config, "r") as f:
            self.config = json.load(f)

        self.validate_config()

        self.result_path = f"{self.script_dir}/results/{args.exp_name}/"
        os.makedirs(self.result_path, exist_ok=True)

        LOG.info("Successfully Loaded Configs")

    def initialize(self, args):
        LOG.info("Begin initializiation of Connections")

        required_nodes = [
            "order",
            "storage",
            "client",
            "pc"
        ]

        for n in required_nodes:
            if n in self.config:
                self.connections[n] = [Connection(ip, self.user) for ip in self.config[n]["addr"]]

        LOG.info("Successfully Connected to all machines")

    def setup(self, args):
        LOG.info("Setting Up required paths")

        paths = [
            self.deployment_config["logdir"],
        ]

        def create_dir(c):
            for p in paths:
                dirs = p.split('/')
                path = ""
                for dir_ in dirs:
                    if dir_ == "":
                        continue
                    path += "/" + dir_
                    try:
                        c.sftp_client.mkdir(path)
                    except IOError:
                        # Directory already exists
                        pass

        def setup_huge_pages(c):
            # TODO - make correct clean command
            c.ssh_client.exec_command("echo 2048 | tee /proc/sys/vm/nr_hugepages", True)

        connections = list(chain.from_iterable(
            self.connections[conn_type] for conn_type in self.connections
        ))

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(create_dir, connections))

        # Restart PTP

        commands = []
        command = f"/tmp/ptp_script.sh"
        user = "root"
        for c in connections:
            commands.append(
                f'(ssh {user}@{c.addr} "{command}") & '
            )
        subprocess.run("".join(commands) + " wait",
                       shell=True,
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)

        sleep(10)

        LOG.info("Successfully Set Up required paths")

    def do_command(self, args):
        raise NotImplemented

    def clean_connections(self):
        LOG.info("Cleaning Connections")

        def clean(c):
            c.close()

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(clean, list(chain.from_iterable(
                self.connections[conn_type] for conn_type in self.connections
            ))))

        LOG.info("Successfully Cleaned connections")

    def stop_connection_execution(self):
        def stop(c):
            c.stop()

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(stop, list(chain.from_iterable(
                self.connections[conn_type] for conn_type in self.connections
            ))))


class AppendBench(AdminCommand):
    NAME = "append_bench"
    HELP = "Simple Append Benchmark"

    def add_arguments(self, parser):
        super().add_arguments(parser)

        parser.add_argument(
            "--runtime", default=10, help="Runtime of experiment"
        )

        parser.add_argument(
            "--burst", default=1, help="Number of outstanding appends"
        )

        parser.add_argument(
            "--size", default=8, help="Operation Size"
        )

        parser.add_argument(
            "--num_shards", default=1, help="Number of shards"
        )

        parser.add_argument(
            "--num_clients", default=1, help="Number of clients per machine"
        )

    def validate_config(self):
        super().validate_config()

        client_fields = [
            "addr",
            "port",
            "bench_cpu",
            "client_cpu"
        ]

        field_mapping = {
            "client" : client_fields,
        }

        for node, fields in field_mapping.items():
            assert node in self.config, f"Error: no {node} nodes in config"
            for f in fields:
                assert f in self.config[node], f"Error: {f} not in {node} config"

        #assert len(self.config["client"]["bench_cpu"]) >=  num_clients, " The number of bench_cpus must be higher or equal to the number of client threads"
        #assert len(self.config["client"]["client_cpu"]) >=  num_clients, " The number of bench_cpus must be higher or equal to the number of client threads"

    def do_command(self, args):
        exp_config = {
            "--duration": args.runtime,
            "--failures": self.config["failures"],
            "--burst": args.burst,
            "--size": args.size
        }

        clear_server_logs(self.logdir, self.connections)
        clear_client_logs(self.logdir, self.connections)

        start_order(self.deployment_config, self.config, self.connections)
        sleep(4)

        start_storages(self.deployment_config, self.config, self.connections)
        sleep(4)

        start_append_client(self.deployment_config, self.config, exp_config, self.connections, int(args.num_clients))
        sleep(args.runtime*2)

        # Stop all executions
        self.stop_connection_execution()
        collect_logs(self.logdir, self.result_path, chain(self.connections["order"], self.connections["storage"], self.connections["client"]))
        process_logs(self.result_path, {})

class PCBench(AdminCommand):
    NAME = "pc_bench"
    HELP = "Simple Append Benchmark"

    def add_arguments(self, parser):
        super().add_arguments(parser)

        parser.add_argument(
            "--runtime", default=10, type=int, help="Runtime of experiment"
        )

        parser.add_argument(
            "--rate", default=50000, help="Number of outstanding appends"
        )

        parser.add_argument(
            "--size", default=512, help="Operation Size"
        )

        parser.add_argument(
            "--num_shards", default=1, help="Number of shards"
        )

        parser.add_argument(
            "--num_clients", default=1, help="Number of clients per machine"
        )

    def validate_config(self):
        super().validate_config()

        client_fields = [
            "addr",
            "port",
            "client_cpu",
            "subscriber_cpus"
        ]

        field_mapping = {
            "pc" : client_fields,
        }

        for node, fields in field_mapping.items():
            assert node in self.config, f"Error: no {node} nodes in config"
            for f in fields:
                assert f in self.config[node], f"Error: {f} not in {node} config"

        #assert len(self.config["client"]["bench_cpu"]) >=  num_clients, " The number of bench_cpus must be higher or equal to the number of client threads"
        #assert len(self.config["client"]["client_cpu"]) >=  num_clients, " The number of bench_cpus must be higher or equal to the number of client threads"

    def do_command(self, args):
        exp_config = {
            "--duration": args.runtime,
            "--rate": args.rate,
            "--size": args.size
        }

        clear_server_logs(self.logdir, self.connections)
        clear_client_logs(self.logdir, self.connections)

        start_order(self.deployment_config, self.config, self.connections)
        sleep(4)

        start_storages(self.deployment_config, self.config, self.connections)
        sleep(4)

        start_producer_consumer(self.deployment_config, self.config, exp_config, self.connections)
        sleep(args.runtime*2)

        # Stop all executions
        self.stop_connection_execution()
        collect_logs(self.logdir, self.result_path, chain(self.connections["order"], self.connections["storage"], self.connections["pc"]))
        process_logs(self.result_path, {})

def main(args):
    start_time = time.time()
    initialize_and_run_commands(
        "Controls deployment and experiment for SpecLog",
        [
            AppendBench,
            PCBench
        ],
        args,
    )
    LOG.info("Elapsed time: %.1f sec", time.time() - start_time)


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
