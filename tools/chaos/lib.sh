#!/usr/bin/env bash
# Shared harness for the chaos testbed (M4.S7).
#
# Usage: source tools/chaos/lib.sh, then start_chaos_cluster <budget_bytes>
# <num_workers>; gives back COORD_PID, COORD_HTTP_PORT, WORKER_PIDS[],
# WORKER_GRPC_PORTS[]; call cleanup_chaos on exit.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[1]}")/../.." && pwd)"
BIN="$REPO_ROOT/target/debug"

COORD_HTTP_PORT=""
COORD_GRPC_PORT=""
COORD_PID=""
WORKER_PIDS=()
WORKER_GRPC_PORTS=()

start_chaos_cluster() {
    local budget_bytes="$1"
    local num_workers="$2"

    BASE_PORT=$(( (RANDOM % 20000) + 20000 ))
    COORD_HTTP_PORT=$BASE_PORT
    COORD_GRPC_PORT=$((BASE_PORT + 1))

    pkill -f 'pylon-coord' 2>/dev/null || true
    pkill -f 'pylon-worker' 2>/dev/null || true
    sleep 1

    echo "[chaos] starting coord (HTTP :$COORD_HTTP_PORT, gRPC :$COORD_GRPC_PORT, budget=${budget_bytes}B)"
    # Force info-level logs: the harness greps for INFO ack markers
    # and the ambient RUST_LOG (often `warn`) would hide them.
    RUST_LOG=pylon=info \
    PYLON_HTTP_PORT=$COORD_HTTP_PORT PYLON_GRPC_PORT=$COORD_GRPC_PORT \
        PYLON_TASK_MEMORY_BUDGET_BYTES="$budget_bytes" \
        "$BIN/pylon-coord" >"/tmp/pylon-chaos-coord.log" 2>&1 &
    COORD_PID=$!

    WORKER_PIDS=()
    WORKER_GRPC_PORTS=()
    for i in $(seq 1 "$num_workers"); do
        local flight_port grpc_port
        flight_port=$((BASE_PORT + 10 + i))
        # Dev machines have other tenants: skip ports already held.
        while lsof -iTCP:"$flight_port" -sTCP:LISTEN >/dev/null 2>&1; do
            flight_port=$((flight_port + 1))
        done
        grpc_port=$((BASE_PORT + 100 + i))
        while lsof -iTCP:"$grpc_port" -sTCP:LISTEN >/dev/null 2>&1; do
            grpc_port=$((grpc_port + 1))
        done
        WORKER_GRPC_PORTS+=("$grpc_port")
        echo "[chaos] starting worker-$i (flight :$flight_port, gRPC :$grpc_port)"
        RUST_LOG=pylon=info \
        PYLON_FLIGHT_ADDR="127.0.0.1:$flight_port" PYLON_GRPC_ADDR="127.0.0.1:$grpc_port" \
            "$BIN/pylon-worker" "http://127.0.0.1:$COORD_GRPC_PORT" \
            --flight-addr "127.0.0.1:$flight_port"             --grpc-addr "127.0.0.1:$grpc_port" \
            >"/tmp/pylon-chaos-worker$i.log" 2>&1 &
        WORKER_PIDS+=($!)
    done

    # Wait until every worker is registered.
    for _ in $(seq 1 30); do
        local count
        count=$(curl -s "http://127.0.0.1:$COORD_HTTP_PORT/v1/workers" \
            | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('workers', [])))" \
            2>/dev/null || echo 0)
        if [ "$count" -eq "$num_workers" ]; then
            echo "[chaos] $count workers registered"
            return 0
        fi
        sleep 0.5
    done
    echo "[chaos] FAIL: workers did not register in time" >&2
    cleanup_chaos
    exit 1
}

cleanup_chaos() {
    echo "[chaos] cleaning up..."
    [ -n "$COORD_PID" ] && kill "$COORD_PID" 2>/dev/null || true
    for pid in "${WORKER_PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
