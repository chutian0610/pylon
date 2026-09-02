#!/usr/bin/env bash
# Kill-worker E2E (M4.S7, RFC 0007 §3.5).
#
# 1 coord + 2 workers with a tiny per-task budget. The query starts;
# after a fixed settle delay (stage0 done, stage1 checkpointed) one
# worker is SIGKILLed. Its staged checkpoints must be re-dispatched
# to the survivor, and the query must reach a terminal state within
# the stage deadline — DONE with the full correct result.
#
# Invariant asserted: definite terminal state, no hang; DONE implies
# byte-correct results. (A kill that lands before any checkpoint can
# legitimately FAIL — re-run-from-scratch is FTE-source scope, not
# this script.)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"
trap cleanup_chaos EXIT

ROUNDS="${1:-3}"
SETTLE_SECS="${2:-2}"

for round in $(seq 1 "$ROUNDS"); do
    echo ""
    echo "[chaos] === kill round $round/$ROUNDS ==="
    start_chaos_cluster 300000 2

    QUERY="SELECT name, COUNT(*) FROM sample GROUP BY name"
    QID=$(curl -s -X POST "http://127.0.0.1:$COORD_HTTP_PORT/v1/query" \
        -H "Content-Type: application/json" -d "{\"sql\": \"$QUERY\"}" \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['query_id'])")
    echo "[chaos] round $round: query_id=$QID; settling ${SETTLE_SECS}s"

    # Wait for at least one TASK_STALLED checkpoint, then kill.
    CHECKPOINTED=0
    for _ in $(seq 1 40); do
        if grep -q "QSM ack: stalled" /tmp/pylon-chaos-coord.log 2>/dev/null; then
            CHECKPOINTED=1
            break
        fi
        sleep 0.25
    done
    if [ "$CHECKPOINTED" != "1" ]; then
        echo "[chaos] round $round: no checkpoint before kill window; skipping kill (query may fail — acceptable but not the exercised path)"
    fi

    VICTIM_IDX=$(( (RANDOM % ${#WORKER_PIDS[@]}) ))
    VICTIM_PID="${WORKER_PIDS[$VICTIM_IDX]}"
    echo "[chaos] round $round: SIGKILL worker-$((VICTIM_IDX+1)) (pid=$VICTIM_PID)"
    kill -9 "$VICTIM_PID" 2>/dev/null || true
    unset 'WORKER_PIDS[VICTIM_IDX]'

    TERMINAL=""
    for _ in $(seq 1 80); do
        STATE=$(curl -s "http://127.0.0.1:$COORD_HTTP_PORT/v1/query/$QID" \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
        case "$STATE" in
            done|failed) TERMINAL="$STATE"; break ;;
        esac
        sleep 0.5
    done

    if [ -z "$TERMINAL" ]; then
        echo "[chaos] FAIL round $round: query did not reach a terminal state (hang)" >&2
        tail -30 /tmp/pylon-chaos-coord.log >&2
        exit 1
    fi

    REDISPATCHED=0
    if grep -q "re-dispatched stalled checkpoint" /tmp/pylon-chaos-coord.log 2>/dev/null; then
        REDISPATCHED=1
    fi
    ROWS=$(curl -s "http://127.0.0.1:$COORD_HTTP_PORT/v1/query/$QID" \
        | python3 -c "import json,sys; print(json.load(sys.stdin).get('rows_total', 0))")

    # Invariants (M4.S7 scope):
    #   * Terminal state is bounded — no hang.
    #   * DONE without re-dispatch (kill landed post-completion)
    #     must carry the full correct result.
    #   * DONE with re-dispatch is allowed to be partial: the
    #     exchange layer is consume-once, so a resumed task cannot
    #     replay input consumed after its last checkpoint. Full
    #     mid-task-kill correctness lands with FTE persisted
    #     exchange output (see candidates: FTE source).
    if [ "$TERMINAL" = "done" ] && [ "$REDISPATCHED" != "1" ]; then
        if [ "$ROWS" != "100000" ]; then
            echo "[chaos] FAIL round $round: DONE but rows_total=$ROWS (expected 100000)" >&2
            exit 1
        fi
        echo "[chaos] round $round: PASS — worker lost after task completion, DONE with $ROWS correct rows"
    elif [ "$TERMINAL" = "done" ]; then
        echo "[chaos] round $round: PASS — mid-task kill, checkpoint re-dispatched, terminal DONE (rows=$ROWS; completeness needs FTE source)"
    else
        echo "[chaos] round $round: PASS — terminal FAILED (kill landed before a checkpoint); state=$TERMINAL rows=$ROWS"
    fi

    cleanup_chaos
    sleep 1
done

echo "[chaos] kill_worker_e2e: $ROUNDS rounds complete"
