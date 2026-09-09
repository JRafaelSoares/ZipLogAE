#!/usr/bin/env bash
# Runs the append-benchmark deployment described in
# docs/EXAMPLE_DEPLOYMENT.md: shared order+storage, a subscriber, and the
# append-benchmark client. Assumes binaries are already built in ./build.
#
# Env overrides: DEVICE (default mlx5_0), GID (default 1), DURATION (default 10).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

DURATION="${DURATION:-10}"
SUBSCRIBER_PID=""

cleanup() {
  if [[ -n "$SUBSCRIBER_PID" ]]; then
    kill -INT "$SUBSCRIBER_PID" 2>/dev/null
    wait "$SUBSCRIBER_PID" 2>/dev/null
  fi
  stop_shared
}
trap cleanup EXIT INT TERM

start_shared

"$BUILD_DIR/subscriber" \
  --device="$DEVICE" --gid="$GID" \
  --application_cpus=5 \
  --order=127.0.0.1:6666 \
  --polling_cpus=6 \
  --subscriber_id=0 &
SUBSCRIBER_PID=$!
sleep 2

"$BUILD_DIR/client" \
  --device="$DEVICE" --gid="$GID" \
  --client_id=0 \
  --cpu=7 \
  --duration="$DURATION" \
  --order=127.0.0.1:6666 \
  --shard_id=0
