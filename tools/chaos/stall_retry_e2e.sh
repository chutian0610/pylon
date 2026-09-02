#!/usr/bin/env bash
# Deterministic stall→checkpoint E2E (M4.S7, RFC 0007 §3.5).
#
# 1 coord + 2 workers, TINY per-task memory budget. The stage-1
# aggregate hits the budget on its second batch, spills, and acks
# TASK_STALLED (emit-and-continue). The worker survives; the coord
# holds the checkpoint and the query must complete normally with the
# correct 100k-row result — proving the checkpoint path does not
# perturb the happy path.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"
trap cleanup_chaos EXIT

# ~300 KiB: the first ~8k-row batch fits (256 KiB estimate), the
# second triggers a spill + TASK_STALLED checkpoint.
start_chaos_cluster 300000 2

QUERY="SELECT name, COUNT(*) FROM sample GROUP BY name"
QID=$(curl -s -X POST "http://127.0.0.1:$COORD_HTTP_PORT/v1/query" \
    -H "Content-Type: application/json" -d "{\"sql\": \"$QUERY\"}" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['query_id'])")
echo "[chaos] query_id=$QID"

# Bounded wait for a terminal state.
TERMINAL=""
for _ in $(seq 1 60); do
    STATE=$(curl -s "http://127.0.0.1:$COORD_HTTP_PORT/v1/query/$QID" \
        | python3 -c "import json,sys; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
    case "$STATE" in
        done|failed) TERMINAL="$STATE"; break ;;
    esac
    sleep 0.5
done
if [ "$TERMINAL" != "done" ]; then
    echo "FAIL: expected DONE, got '${TERMINAL:-timeout}'" >&2
    tail -30 /tmp/pylon-chaos-coord.log >&2
    exit 1
fi

ROWS=$(curl -s "http://127.0.0.1:$COORD_HTTP_PORT/v1/query/$QID" \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('rows_total', 0))")
if [ "$ROWS" != "100000" ]; then
    echo "FAIL: rows_total=$ROWS (expected 100000 — a full correct result after checkpoints)" >&2
    tail -30 /tmp/pylon-chaos-coord.log >&2
    exit 1
fi

# The coord log must contain at least one TASK_STALLED checkpoint ack.
if ! grep -q "QSM ack: stalled" /tmp/pylon-chaos-coord.log; then
    echo "FAIL: no TASK_STALLED checkpoint ack observed (spill path did not fire)" >&2
    tail -30 /tmp/pylon-chaos-coord.log >&2
    exit 1
fi

echo "[chaos] PASS: stall checkpoint observed; query DONE with $ROWS correct rows"
