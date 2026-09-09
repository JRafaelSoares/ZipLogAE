#!/bin/bash
# Local single-machine deployment for the producer-consumer benchmark,
# following docs/EXAMPLE_DEPLOYMENT.md sections 1 and 3: one order server,
# two storage replicas (shard 0), and the pc client — all on 127.0.0.1.
#
# Usage: scripts/experiments/local_pc.sh [duration_seconds] [rate]

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

DURATION="${1:-10}"
RATE="${2:-50000}"
ORDER_ADDR=127.0.0.1:31850
STORAGE0_ADDR=127.0.0.1:31851
STORAGE1_ADDR=127.0.0.1:31852

BIN_ORDER=./build/out/bin/order
BIN_STORAGE=./build/out/bin/storage
BIN_PC=./build/out/apps/pc/pc

for bin in "$BIN_ORDER" "$BIN_STORAGE" "$BIN_PC"; do
  if [[ ! -x "$bin" ]]; then
    echo "error: $bin not found or not executable - build the project first" >&2
    exit 1
  fi
done

LOG_DIR="$(mktemp -d /tmp/ziplog-local-pc.XXXXXX)"
echo "Logs: $LOG_DIR"

ORDER_PID=
STORAGE0_PID=
STORAGE1_PID=

cleanup() {
  # Stop storage before order, as documented, and let each shut down
  # gracefully (SIGINT) rather than killing them outright.
  for pid in "$STORAGE0_PID" "$STORAGE1_PID"; do
    [[ -n "$pid" ]] && kill -INT "$pid" 2>/dev/null || true
  done
  for pid in "$STORAGE0_PID" "$STORAGE1_PID"; do
    [[ -n "$pid" ]] && wait "$pid" 2>/dev/null || true
  done
  [[ -n "$ORDER_PID" ]] && kill -INT "$ORDER_PID" 2>/dev/null || true
  [[ -n "$ORDER_PID" ]] && wait "$ORDER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Starting order server on $ORDER_ADDR (cpu=0)"
"$BIN_ORDER" --cpu=0 --port 31850 > "$LOG_DIR/order.log" 2>&1 &
ORDER_PID=$!

sleep 2

echo "Starting storage replica 0 on $STORAGE0_ADDR (client_cpus=1, subscriber_cpus=2)"
"$BIN_STORAGE" \
  --client_cpus=1 \
  --order="$ORDER_ADDR" \
  --port=31851 \
  --replica_id=0 \
  --shard_id=0 \
  --subscriber_cpus=2 \
  --timeout=100000 > "$LOG_DIR/storage0.log" 2>&1 &
STORAGE0_PID=$!

sleep 2

echo "Starting storage replica 1 on $STORAGE1_ADDR (client_cpus=3, subscriber_cpus=4)"
"$BIN_STORAGE" \
  --client_cpus=3 \
  --order="$ORDER_ADDR" \
  --port=31852 \
  --replica_id=1 \
  --shard_id=0 \
  --subscriber_cpus=4 \
  --timeout=100000 > "$LOG_DIR/storage1.log" 2>&1 &
STORAGE1_PID=$!

# Give order/storage a moment to come up before pointing pc at them.
sleep 2

echo "Running pc for ${DURATION}s at rate=${RATE} (client_cpu=5, subscriber_cpus=6)"
"$BIN_PC" \
  --client_cpu=5 \
  --client_id=0 \
  --duration="$DURATION" \
  --order="$ORDER_ADDR" \
  --port=31853 \
  --rate="$RATE" \
  --servers="$STORAGE0_ADDR,$STORAGE1_ADDR" \
  --shard_id=0 \
  --subscriber_cpus=6 \
  --subscriber_id=0 \
  2>&1 | tee "$LOG_DIR/pc.log"

echo "pc log: $LOG_DIR/pc.log"
echo "Done. Stopping storage and order servers."
