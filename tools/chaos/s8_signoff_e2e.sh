#!/usr/bin/env bash
# M4.S8 — the headline sign-off E2E (RFC 0007 §5).
#
# Double-run comparison at scale:
#   run A (baseline) : uninterrupted aggregate → rows_total BASELINE
#   run B (chaos)    : same query, SIGKILL a worker mid-task after the
#                      first spill checkpoint → input-log re-dispatch →
#                      rows_total KILL_ROWS
#   sign-off         : KILL_ROWS == BASELINE (RFC: "结果与
#                      uninterrupted run 字节级一致"; count-exact is
#                      the practical proxy at this result size)
#
# Scale knobs (defaults prove all mechanics at 20M×1M in ~15 min;
# the RFC's literal 1B is the same script, hours):
#   S8_ROWS=1000000000 S8_GROUPS=1000000 tools/chaos/s8_signoff_e2e.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"
trap cleanup_chaos EXIT

S8_ROWS="${S8_ROWS:-20000000}"
S8_GROUPS="${S8_GROUPS:-1000000}"
# 48 MiB: enough for ~500k in-flight groups per task, small enough to
# force periodic spill → TASK_STALLED checkpoints.
BUDGET=50331648

echo "[s8] generating sample: $S8_ROWS rows / $S8_GROUPS groups..."
cargo run --quiet -p gen-sample-data -- --rows "$S8_ROWS" --groups "$S8_GROUPS"

QUERY="SELECT name, COUNT(*) FROM sample GROUP BY name"

run_query() {
    local mode="$1"
    # Harness chatter goes to stderr — stdout must carry ONLY the
    # final rows_total (the caller captures this in a variable).
    start_chaos_cluster "$BUDGET" 2 >&2
    local qid
    qid=$(curl -s -X POST "http://127.0.0.1:$COORD_HTTP_PORT/v1/query" \
        -H "Content-Type: application/json" -d "{\"sql\": \"$QUERY\"}" \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['query_id'])")
    echo "[s8] $mode: query_id=$qid" >&2

    if [ "$mode" = "kill" ]; then
        # Kill the moment the first spill checkpoint lands.
        for _ in $(seq 1 240); do
            if grep -q "QSM ack: stalled" /tmp/pylon-chaos-coord.log 2>/dev/null; then
                break
            fi
            sleep 0.25
        done
        local idx=$(( (RANDOM % ${#WORKER_PIDS[@]}) ))
        echo "[s8] kill: SIGKILL worker-$((idx+1)) (pid=${WORKER_PIDS[$idx]})" >&2
        kill -9 "${WORKER_PIDS[$idx]}" 2>/dev/null || true
        unset "WORKER_PIDS[$idx]"
    fi

    local terminal=""
    for _ in $(seq 1 720); do
        terminal=$(curl -s "http://127.0.0.1:$COORD_HTTP_PORT/v1/query/$qid" \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
        case "$terminal" in
            done|failed) break ;;
        esac
        sleep 1
    done
    if [ "$terminal" != "done" ]; then
        echo "[s8] FAIL ($mode): terminal='$terminal' (expected done)" >&2
        tail -25 /tmp/pylon-chaos-coord.log >&2
        exit 1
    fi
    curl -s "http://127.0.0.1:$COORD_HTTP_PORT/v1/query/$qid" \
        | python3 -c "import json,sys; print(json.load(sys.stdin).get('rows_total', 0))"
    cleanup_chaos >&2
    sleep 1
}

BASELINE=$(run_query baseline)
echo "[s8] baseline: $BASELINE rows"
if [ "$BASELINE" -ne "$S8_GROUPS" ]; then
    echo "[s8] FAIL: baseline rows_total=$BASELINE (expected $S8_GROUPS groups)" >&2
    exit 1
fi

KILL_ROWS=$(run_query kill)
echo "[s8] kill-run: $KILL_ROWS rows"
if ! grep -q "re-dispatched task with persisted input log" /tmp/pylon-chaos-coord.log 2>/dev/null; then
    echo "[s8] FAIL: kill run did not exercise input-log re-dispatch" >&2
    exit 1
fi

if [ "$KILL_ROWS" != "$BASELINE" ]; then
    echo "[s8] FAIL: kill run $KILL_ROWS != baseline $BASELINE (replay must be exact)" >&2
    exit 1
fi

echo ""
echo "[s8] M4 SIGN-OFF PASS — mid-task worker kill at ${S8_ROWS}×${S8_GROUPS}: "
echo "[s8]   kill-run result ($KILL_ROWS rows) == baseline ($BASELINE rows)"
