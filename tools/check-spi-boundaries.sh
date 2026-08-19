#!/usr/bin/env bash
# Boundary axiom enforcement for the pylon-connector-spi crate.
#
# Verifies RFC 0005 § 3 rule #1: the SPI depends only on
# `pylon-types` + Arrow. Any engine crate appearing in the SPI's
# dependency graph (Cargo.toml or `use` imports) fails the check.
#
# Run from the repo root:
#   bash tools/check-spi-boundaries.sh
#
# Wire into CI / pre-push by adding `bash tools/check-spi-boundaries.sh`
# before `cargo test`. Exit code 0 = pass, 1 = violation.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

SPI_DIR="crates/pylon-connector-spi"

if [ ! -d "$SPI_DIR" ]; then
    echo "FAIL: $SPI_DIR does not exist. R0 has not been scaffolded."
    exit 1
fi

# Engine crates the SPI must NOT depend on (RFC 0005 § 3 rule #1).
FORBIDDEN_DEPS=(
    "pylon-plan"
    "pylon-runtime"
    "pylon-coord"
    "pylon-worker"
    "pylon-exchange"
    "pylon-protocol"
    "pylon-proto"
)

violations=0

# 1. Cargo.toml dep check.
echo "[1/2] Checking $SPI_DIR/Cargo.toml for forbidden engine deps..."
for dep in "${FORBIDDEN_DEPS[@]}"; do
    if grep -nE "^[[:space:]]*$dep[[:space:]]*=" "$SPI_DIR/Cargo.toml"; then
        echo "  FAIL: forbidden dep '$dep' declared in $SPI_DIR/Cargo.toml"
        violations=$((violations + 1))
    fi
done

# 2. Use-import check. The SPI must not import any engine crate.
echo "[2/2] Checking $SPI_DIR/src/** for forbidden \"use pylon_…\" imports..."
for src in $(find "$SPI_DIR/src" -name '*.rs'); do
    for dep in "${FORBIDDEN_DEPS[@]}"; do
        if grep -nE "^[[:space:]]*use[[:space:]]+$dep(::|;|\\{|\$)" "$src" \
            || grep -nE "^use crate::${dep#pylon-}" "$src"; then
            echo "  FAIL: $src imports forbidden crate '$dep'"
            violations=$((violations + 1))
        fi
    done
done

if [ "$violations" -ne 0 ]; then
    echo
    echo "AXIOM VIOLATION: $SPI_DIR must only depend on pylon-types + Arrow."
    echo "See docs/rfcs/0005-pipeline-trait-surface.md § 3 rule #1."
    exit 1
fi

echo
echo "OK: $SPI_DIR dependency boundary holds."
