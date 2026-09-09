#!/usr/bin/env bash
#
# actual_run_local_experiment.sh
#
# Runs one local experiment as these four exact commands, in order:
#   1. ziplog order server
#   2. ziplog storage replica 0
#   3. ziplog storage replica 1
#   4. retwisClient
# Prints /tmp/client.0.log (written by retwisClient itself, via --logPath
# /tmp with a single client thread) at the end.
#
# NOTE: ./store/tools/f1.shard0.config (used as --configFile below) is the
# checked-in config, which lists remote replica addresses
# (192.168.99.28/29/30:51736) rather than the local storage servers started
# here (127.0.0.1:7777 and 127.0.0.1:7778). If the client can't connect to
# the storage replicas, update that config (or point --configFile at a
# different file) to match.

set -euo pipefail

ORDER_LOG=/tmp/order.log
STORAGE0_LOG=/tmp/storage0.log
STORAGE1_LOG=/tmp/storage1.log
CLIENT_LOG=/tmp/client.0.log

ORDER_PID=""
STORAGE0_PID=""
STORAGE1_PID=""

cleanup() {
    [ -n "$STORAGE1_PID" ] && kill -9 "$STORAGE1_PID" 2>/dev/null || true
    [ -n "$STORAGE0_PID" ] && kill -9 "$STORAGE0_PID" 2>/dev/null || true
    [ -n "$ORDER_PID" ] && kill -9 "$ORDER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "Starting ziplog order server (log: $ORDER_LOG)."
./third_party/ziplog/build/order --device=mlx5_0 --gid=1 --server 5813 --cpu 0 \
    > "$ORDER_LOG" 2>&1 &
ORDER_PID=$!

sleep 2

echo "Starting ziplog storage replica 0 (log: $STORAGE0_LOG)."
./third_party/ziplog/build/storage --device=mlx5_0 --gid=1 --client_cpus=1 --subscriber_cpus=2 --get_cpus=3 --order=127.0.0.1:5813 --replica_id=0 --server=7777 --shard_id=0 --num_keys=1000 \
    > "$STORAGE0_LOG" 2>&1 &
STORAGE0_PID=$!

sleep 2

echo "Starting ziplog storage replica 1 (log: $STORAGE1_LOG)."
./third_party/ziplog/build/storage --device=mlx5_0 --gid=1 --client_cpus=4 --subscriber_cpus=5 --get_cpus=6 --order=127.0.0.1:5813 --replica_id=1 --server=7778 --shard_id=0 --num_keys=1000 \
    > "$STORAGE1_LOG" 2>&1 &
STORAGE1_PID=$!

sleep 2

echo "Running retwisClient."
./store/benchmark/retwisClient \
    --configFile ./store/tools/f1.shard0.config \
    --keysFile /src/keys \
    --numKeys 1000 \
    --numShards 1 \
    --duration 10 \
    --warmup 1 \
    --tLen 2 \
    --wPer 50 \
    --closestReplica -1 \
    --mode meerkatstore \
    --numServerThreads 1 \
    --zipf 1.0 \
    --ziplogClientRate 51200 \
    --numClientThreads 1 \
    --numClientFibers 1 \
    --secondsFromEpoch "$(date +%s)" \
    --ip 127.0.0.1 \
    --physPort 0 \
    --logPath /tmp

cleanup
trap - EXIT

echo
echo "===== $CLIENT_LOG ====="
cat "$CLIENT_LOG"
