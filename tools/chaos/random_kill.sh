#!/usr/bin/env bash
# RFC 0007 §5 M4.S7 named entry point: N rounds of random-delay
# worker kills over the deterministic chaos cluster.
#
# Usage: tools/chaos/random_kill.sh [rounds] [max_kill_delay_secs]
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

ROUNDS="${1:-5}"
MAX_DELAY="${2:-5}"

for round in $(seq 1 "$ROUNDS"); do
    DELAY=$(python3 -c "import random; print(round(random.uniform(0.5, $MAX_DELAY), 2))")
    echo "[random_kill] round $round/$ROUNDS — kill after ${DELAY}s settle"
    "$HERE/kill_worker_e2e.sh" 1 "$DELAY"
done
echo "[random_kill] $ROUNDS rounds complete"
