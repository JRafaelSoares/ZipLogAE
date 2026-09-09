#!/bin/bash
# Same setup as benchmark-f0.sh, but every node runs locally instead of over ssh.

# ─── config ───────────────────────────────────────────────────────────────────
DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ZIPLOG=$DIR/ziplog
CONFIG=$DIR/config/setup1.json
NUM_COMMANDS=${1:-1000}
NUM_CLIENTS=${2:-10}

LOGDIR=$DIR/logs
mkdir -p "$LOGDIR"

# ─── helpers ──────────────────────────────────────────────────────────────────
kill_all() {
    echo "[*] killing all ziplog processes..."
    pkill -SIGTERM -f "$ZIPLOG" 2>/dev/null
    sleep 1
}

wait_for_port() {
    local port=$1
    local retries=20
    echo "[*] waiting for 127.0.0.1:$port..."
    for i in $(seq 1 $retries); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            exec 3<&- 3>&-
            echo "[+] 127.0.0.1:$port ready"
            return 0
        fi
        sleep 0.3
    done
    echo "[!] timeout waiting for 127.0.0.1:$port"
    return 1
}

trap kill_all EXIT

# ─── start infrastructure ─────────────────────────────────────────────────────
kill_all

echo "[*] starting zipper..."
nohup "$ZIPLOG" zipper "$CONFIG" < /dev/null > "$LOGDIR/zipper.log" 2>&1 &
wait_for_port 8000 || exit 1

echo "[*] starting subscriber..."
nohup "$ZIPLOG" subscriber "$CONFIG" 0 < /dev/null > "$LOGDIR/subscriber0.log" 2>&1 &
wait_for_port 8020 || exit 1

echo "[*] starting server..."
nohup "$ZIPLOG" server "$CONFIG" 0 < /dev/null > "$LOGDIR/server0.log" 2>&1 &
wait_for_port 8010 || exit 1

echo "[*] starting proxy..."
nohup "$ZIPLOG" proxy "$CONFIG" 0 < /dev/null > "$LOGDIR/proxy0.log" 2>&1 &
wait_for_port 8001 || exit 1

echo "[*] sleeping 1s for connections to stabilize..."
sleep 1

# ─── run benchmark ────────────────────────────────────────────────────────────
echo "[*] running benchmark: $NUM_CLIENTS clients x $NUM_COMMANDS commands each..."
START=$(date +%s%N)

# launch all clients in parallel, each logging to its own file
PIDS=()
for i in $(seq 0 $(( NUM_CLIENTS - 1 ))); do
    "$ZIPLOG" benchmark "$CONFIG" 0 "$NUM_COMMANDS" > "$LOGDIR/client${i}.log" 2>&1 &
    PIDS+=($!)
done

# wait for all clients to finish
for pid in "${PIDS[@]}"; do
    wait $pid
done

END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
TOTAL_COMMANDS=$(( NUM_CLIENTS * NUM_COMMANDS ))

# ─── collect results ──────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════"
echo " benchmark complete"
echo " clients:      $NUM_CLIENTS"
echo " commands:     $TOTAL_COMMANDS ($NUM_COMMANDS per client)"
echo " total time:   ${ELAPSED_MS}ms"
echo " throughput:   $(( TOTAL_COMMANDS * 1000 / ELAPSED_MS )) cmd/s"
echo "════════════════════════════════════════"

# show per-client success counts
echo ""
echo "[*] per-client results:"
for i in $(seq 0 $(( NUM_CLIENTS - 1 ))); do
    cat "$LOGDIR/client${i}.log" 2>/dev/null
done

# subscriber stdout is fully buffered once redirected to a file, so [latency]
# lines may still be sitting in its buffer — kill it now to force a flush
# via its destructor before we read the log (harmless: benchmark is done).
kill_all

echo ""
echo "[*] latency stats (µs):"
grep '\[latency\]' "$LOGDIR/subscriber0.log" \
    | grep -oP 'latency=\K[0-9]+' \
    | sort -n \
    | awk '
        BEGIN { count=0; sum=0 }
        { vals[count++]=$1; sum+=$1 }
        END {
            if (count==0) { print "  no latency data"; exit }
            print "  count: " count
            print "  min:   " vals[0]
            print "  p50:   " vals[int(count*0.50)]
            print "  p99:   " vals[int(count*0.99)]
            print "  max:   " vals[count-1]
            print "  mean:  " int(sum/count)
        }
    '
