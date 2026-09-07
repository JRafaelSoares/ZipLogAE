import json
import argparse
import subprocess
import sys


def run_cmd(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize machines in grid5000 cluster")

    parser.add_argument(
        "user", help="Username in Grid5000"
    )

    parser.add_argument(
        "dev", help="Dev to be used for clock sync"
    )

    parser.add_argument(
        "--no-ptp", action="store_true", help="Skip PTP setup"
    )

    parser.add_argument("--job_id", help="Optional job id to specify which to get the information from")

    args = parser.parse_args()

    # Get Hosts

    if args.job_id is not None:
        job_info_txt = subprocess.run(f"oarstat -j {args.job_id} -J", shell=True, capture_output=True, text=True)

    else:
        job_info_txt = subprocess.run(f"oarstat -u {args.user} -J", shell=True, capture_output=True, text=True)

    # Parse output as JSON
    try:
        job_info_json = json.loads(job_info_txt.stdout)
    except json.JSONDecodeError as e:
        print("Failed to parse JSON:", e)
        sys.exit(-1)

    assert len(job_info_json) == 1, "Assuming only a single job exists in the json"
    job_id = list(job_info_json.keys())[0]

    host_machines = job_info_json[job_id]["assigned_network_address"]

    ip_addresses = []
    for host in host_machines:
        ip_text = subprocess.run(f"nslookup {host}", shell=True, capture_output=True, text=True)

        # Parse the output
        lines = ip_text.stdout.splitlines()

        address_after_name = None
        found_name = False

        for line in lines:
            if line.startswith('Name:'):
                found_name = True
            elif found_name and line.strip().startswith('Address:'):
                address_after_name = line.split('Address:')[1].strip()
                break

        assert address_after_name is not None, f"Could not find the address for host {host}"

        ip_addresses.append(address_after_name)

    # For each host, copy the start_up script and execute it

    for i, host in enumerate(ip_addresses):
        remote = f"root@{host}"

        # Validate connectivity/auth with OpenSSH using the user's normal SSH config.
        probe = run_cmd(["ssh", "-o", "BatchMode=yes", remote, "true"])
        if probe.returncode != 0:
            print(f"Failed to connect to {host}: {probe.stderr.strip()}")
            sys.exit(1)
        print(f"Connected to {host}")

        # Copy startup file to each one

        scp_to_startup = run_cmd(["scp", "./scripts/grid5000/start_up.sh", f"{remote}:start_up.sh"])
        if scp_to_startup.returncode != 0:
            print(f"Failed to copy start_up.sh to {host}: {scp_to_startup.stderr.strip()}")
            sys.exit(1)

        if args.no_ptp:
            config_type = "none"
        else:
            config_type = "slave"
    
            if i == len(ip_addresses)-1:
                config_type = "master"

        command = f"~/start_up.sh {args.user} {args.dev} {config_type}"

        with open("ptp_script.sh", "w") as f:
            f.write("#!/bin/bash\n")
            f.write(command + "\n")

        scp_to_script = run_cmd(["scp", "ptp_script.sh", f"{remote}:/tmp/ptp_script.sh"])
        if scp_to_script.returncode != 0:
            print(f"Failed to copy ptp_script.sh to {host}: {scp_to_script.stderr.strip()}")
            sys.exit(1)

        remote_cmd = "chmod +x /tmp/ptp_script.sh && /tmp/ptp_script.sh"
        ssh_run = run_cmd(["ssh", remote, remote_cmd])
        output = ssh_run.stdout.strip()
        error = ssh_run.stderr.strip()
        exit_status = ssh_run.returncode

        print(f"STATUS: {exit_status}")
        if exit_status == 1:
            print(f"Output: {output}")
            print(f"Error on host {host}: {error}")

