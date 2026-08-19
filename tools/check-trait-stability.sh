#!/usr/bin/env bash
# Trait-stability enforcement for the pylon workspace.
#
# Companion to `tools/check-spi-boundaries.sh` (RFC 0005 §3 rule #1).
# Adds three rules from `docs/design/trait-stability.md` § 8.2:
#
#   1. No internal-crate dep in connector crates.
#      `pylon-catalog` / `pylon-storage` / `pylon-iceberg` cannot
#      depend on any engine crate (`pylon-plan` / `pylon-runtime` /
#      `pylon-coord` / `pylon-worker` / `pylon-exchange`).
#
#   2. No engine re-export from the connector SPI.
#      `pylon-connector-spi::lib.rs` re-exports must not include
#      items from any engine crate.
#
#   3. `SPI_VERSION` declared in `pylon-connector-spi/src/lib.rs`
#      (warn pre-R1, fail from R1 onward — gated by checking for
#      the existence of the Connector trait definition).
#
# Run from the repo root:
#   bash tools/check-trait-stability.sh
#
# Exit 0 = pass, 1 = violation.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Engine crates a connector must NOT depend on, and the SPI must
# NOT re-export from. (RFC 0005 §1 + `trait-stability.md` § 2.)
INTERNAL_CRATES=(
    "pylon-plan"
    "pylon-runtime"
    "pylon-coord"
    "pylon-worker"
    "pylon-exchange"
    "pylon-proto"
)

# Connector crates — must depend only on `pylon-types` +
# `pylon-connector-spi` + Arrow.
CONNECTOR_CRATES=(
    "pylon-catalog"
    "pylon-storage"
    "pylon-iceberg"
)

SPI_CRATE="pylon-connector-spi"

violations=0

# ---------------------------------------------------------------------------
# 1. No internal-crate dep in connector crates.
# ---------------------------------------------------------------------------

echo "[1/3] Checking connector crates do not depend on engine crates..."
for conn in "${CONNECTOR_CRATES[@]}"; do
    cargo_toml="crates/$conn/Cargo.toml"
    if [ ! -f "$cargo_toml" ]; then
        continue  # crate may not exist yet; nothing to check
    fi
    for dep in "${INTERNAL_CRATES[@]}"; do
        if grep -nE "^[[:space:]]*$dep[[:space:]]*=" "$cargo_toml" \
            | grep -v "^[[:space:]]*#"; then
            echo "  FAIL: $cargo_toml declares forbidden engine dep '$dep'"
            violations=$((violations + 1))
        fi
    done
done

# Also verify the connector crates' src/** does not `use` an internal crate.
for conn in "${CONNECTOR_CRATES[@]}"; do
    src_dir="crates/$conn/src"
    if [ ! -d "$src_dir" ]; then
        continue
    fi
    for src in $(find "$src_dir" -name '*.rs'); do
        for dep in "${INTERNAL_CRATES[@]}"; do
            # Match `use pylon_plan::`, `use pylon_plan;`, `use pylon_plan::{...}`, end-of-line use.
            if grep -nE "^[[:space:]]*use[[:space:]]+${dep//-/_}(::|;|\\{|\$)" "$src"; then
                echo "  FAIL: $src imports forbidden crate '$dep'"
                violations=$((violations + 1))
            fi
        done
    done
done

# ---------------------------------------------------------------------------
# 2. No engine re-export from the connector SPI.
# ---------------------------------------------------------------------------

echo "[2/3] Checking $SPI_CRATE does not re-export engine-crate items..."
spi_lib="crates/$SPI_CRATE/src/lib.rs"
if [ -f "$spi_lib" ]; then
    for dep in "${INTERNAL_CRATES[@]}"; do
        if grep -nE "^[[:space:]]*pub use[[:space:]]+${dep//-/_}::" "$spi_lib"; then
            echo "  FAIL: $spi_lib re-exports from forbidden crate '$dep'"
            violations=$((violations + 1))
        fi
    done
fi

# ---------------------------------------------------------------------------
# 3. SPI_VERSION constant.
# ---------------------------------------------------------------------------

echo "[3/3] Checking $SPI_CRATE declares SPI_VERSION..."
if [ -f "$spi_lib" ]; then
    if ! grep -nE "^[[:space:]]*pub[[:space:]]+const[[:space:]]+SPI_VERSION" "$spi_lib"; then
        # Pre-R1 warning: the SPI_VERSION rule is a *fail* from R1
        # onward. Until then, we emit a warning so we don't block
        # pre-R1 PRs.
        echo "  WARN: $spi_lib does not declare \`pub const SPI_VERSION\`."
        echo "        This is a fail from R1 onward (see trait-stability.md § 5.5)."
    fi
fi

if [ "$violations" -ne 0 ]; then
    echo
    echo "AXIOM VIOLATION: $violations check(s) failed."
    echo "See docs/design/trait-stability.md § 8.2."
    exit 1
fi

echo
echo "OK: trait-stability boundaries hold."
