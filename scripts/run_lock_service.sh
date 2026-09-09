#!/usr/bin/env bash
# Runs the linearizable lock-service deployment described in
# docs/EXAMPLE_DEPLOYMENT.md, as an alternative to the append benchmark on
# top of the same shared order+storage setup. Assumes binaries are already
# built in ./build.
#
# Env overrides: DEVICE (default mlx5_0), GID (default 1), NUM_CLIENTS (default 1).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

NUM_CLIENTS="${NUM_CLIENTS:-1}"

cleanup() {
  stop_shared
}
trap cleanup EXIT INT TERM

start_shared

"$BUILD_DIR/lock_service" \
  --device="$DEVICE" --gid="$GID" \
  --client_id=0 \
  --cpu=5 \
  --order=127.0.0.1:6666 \
  --shard_id=0 \
  --application_cpus=6 \
  --polling_cpus=7 \
  --subscriber_id=0 \
  --num_clients="$NUM_CLIENTS"
