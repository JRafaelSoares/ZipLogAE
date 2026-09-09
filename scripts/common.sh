#!/usr/bin/env bash
# Shared order+storage bring-up/teardown, mirroring the "Shared setup" section
# of docs/EXAMPLE_DEPLOYMENT.md. Sourced by run_append_benchmark.sh and
# run_lock_service.sh — not meant to be run directly.

DEVICE="${DEVICE:-mlx5_0}"
GID="${GID:-1}"
BUILD_DIR="${BUILD_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/build}"

ORDER_PID=""
STORAGE_PIDS=()

start_shared() {
  "$BUILD_DIR/order" --device="$DEVICE" --gid="$GID" --cpu=0 &
  ORDER_PID=$!
  sleep 2

  "$BUILD_DIR/storage" \
    --device="$DEVICE" --gid="$GID" \
    --client_cpus=1 \
    --order=127.0.0.1:6666 \
    --replica_id=0 \
    --server=7777 \
    --shard_id=0 \
    --subscriber_cpus=2 &
  STORAGE_PIDS+=("$!")

  sleep 2

  "$BUILD_DIR/storage" \
    --device="$DEVICE" --gid="$GID" \
    --client_cpus=3 \
    --order=127.0.0.1:6666 \
    --replica_id=1 \
    --server=7778 \
    --shard_id=0 \
    --subscriber_cpus=4 &
  STORAGE_PIDS+=("$!")

  sleep 2
}

# Stops storage before order, per the "Stopping" section of
# docs/EXAMPLE_DEPLOYMENT.md. Callers must stop their own workload
# processes (client/subscriber/lock_service) before calling this.
stop_shared() {
  for pid in "${STORAGE_PIDS[@]}"; do
    kill -INT "$pid" 2>/dev/null
  done
  wait "${STORAGE_PIDS[@]}" 2>/dev/null

  kill -INT "$ORDER_PID" 2>/dev/null
  wait "$ORDER_PID" 2>/dev/null
}
