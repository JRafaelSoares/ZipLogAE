#!/bin/bash

burst=(1)
size=(8 4096)
num_clients=(1)
for burst_count in "${burst[@]}"; do
    for size_count in "${size[@]}"; do
      for client_count in "${num_clients[@]}"; do
        echo "Running append_bench with burst=$burst_count and size=$size_count"
        python3 scripts/commands/commands.py append_bench scripts/grid5000/deployment.json scripts/grid5000/config.json "s_${size_count}_b_${burst_count}_c${client_count}" --size "$size_count" --burst "$burst_count" --num_clients "${client_count}"
      done
    done
done
