#!/usr/bin/env bash
# FTE source E2E (RFC 0007 §5 M4 "FTE source" row).
#
# 1 coord + 2 workers, tiny per-task budget. Wait for the first
# TASK_STALLED checkpoint, then SIGKILL one worker mid-task. The
# coord re-dispatches the dead worker's task with its persisted
# input log; the survivor replays the FULL input and the query must
# finish DONE with the exact 100k-row correct result — the assertion
# that was impossible before input replay existed.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"
trap cleanup_chaos EXIT

ROUNDS="${1:-3}"

for round in $(seq 1 "$ROUNDS"); do
    echo ""
    echo "[fte] === round $round/$ROUNDS ==="
    start_chaos_cluster 300000 2

    QUERY="SELECT name, COUNT(*) FROM sample GROUP BY name"
    QID=$(curl -s -X POST "http://127.0.0.1:$COORD_HTTP_PORT/v1/query" \
        -H "Content-Type: application/json" -d "{\"sql\": \"$QUERY\"}" \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['query_id'])")

    # Kill the moment the first checkpoint lands (mid-task window).
    for _ in $(seq 1 60); do
        if grep -q "QSM ack: stalled" /tmp/pylon-chaos-coord.log 2>/dev/null; then
            break
        fi
        sleep 0.05
    done
    VICTIM_IDX=$(( (RANDOM % ${#WORKER_PIDS[@]}) ))
    VICTIM_PID="${WORKER_PIDS[$VICTIM_IDX]}"
    echo "[fte] round $round: SIGKILL worker-$((VICTIM_IDX+1)) mid-task (pid=$VICTIM_PID)"
    kill -9 "$VICTIM_PID" 2>/dev/null || true
    unset "WORKER_PIDS[$VICTIM_IDX]"

    TERMINAL=""
    for _ in $(seq 1 80); do
        STATE=$(curl -s "http://127.0.0.1:$COORD_HTTP_PORT/v1/query/$QID" \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
        case "$STATE" in
            done|failed) TERMINAL="$STATE"; break ;;
        esac
        sleep 0.25
    done

    if [ "$TERMINAL" != "done" ]; then
        echo "[fte] FAIL round $round: terminal='$TERMINAL' (expected done via input replay)" >&2
        tail -25 /tmp/pylon-chaos-coord.log >&2
        exit 1
    fi
    if ! grep -q "re-dispatched task with persisted input log" /tmp/pylon-chaos-coord.log 2>/dev/null; then
        echo "[fte] FAIL round $round: no input-log re-dispatch in log (kill landed post-completion)" >&2
        exit 1
    fi
    ROWS=$(curl -s "http://127.0.0.1:$COORD_HTTP_PORT/v1/query/$QID" \
        | python3 -c "import json,sys; print(json.load(sys.stdin).get('rows_total', 0))")
    if [ "$ROWS" != "100000" ]; then
        echo "[fte] FAIL round $round: rows_total=$ROWS (expected 100000 — replay must be exact)" >&2
        exit 1
    fi
    echo "[fte] round $round: PASS — mid-task kill, input replay via persisted log, DONE with $ROWS exact rows"

    cleanup_chaos
    sleep 1
done
echo "[fte] $ROUNDS rounds complete"
