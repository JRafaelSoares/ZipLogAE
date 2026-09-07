#!/bin/bash

duration=(10)
size=(8 16 32 64 128 256 512 1024 2048 4096)
rate=(50000)
trials=3

for duration_count in "${duration[@]}"; do
    for size_count in "${size[@]}"; do
      for rate_count in "${rate[@]}"; do
        for trial_count in $(seq 1 "$trials"); do
          echo "Running pc_bench with duration=$duration_count size=$size_count rate=$rate_count trial=$trial_count"
          python3 scripts/commands/commands.py pc_bench scripts/grid5000/deployment.json scripts/grid5000/pc_config.json "s_${size_count}_r_${rate_count}_d_${duration_count}_t_${trial_count}" --size "$size_count" --rate "$rate_count" --runtime "${duration_count}"
        done
      done
    done
done
