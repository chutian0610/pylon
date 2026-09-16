# Pylon

Pylon is an Apache Arrow–native, pipeline-first SQL query engine
written in Rust. It targets the Presto/Trino workload family — SQL in,
distributed GROUP BY aggregates out — with a Velox-inspired
Operator/Driver/Task runtime instead of a JVM.

## Features

- **SQL front-end** — sqlparser-based parsing and planning; `GROUP BY`
  with `COUNT` / `SUM` / `MIN` / `MAX` over Parquet
- **Distributed execution** — coordinator + worker binaries; stages are
  hash-partitioned across workers and shuffled over Arrow Flight
  `DoExchange`
- **Fault-tolerant execution** — per-task memory budgets,
  spill-to-disk/S3 aggregates with `TASK_STALLED` checkpoints,
  persisted exchange input, and automatic worker-loss re-dispatch.
  Verified by chaos sign-off: killing a worker mid-query still
  produces the exact baseline result
- **Arrow-native** — columnar end to end; no JVM, no DataFusion runtime

## Status

M1–M4 complete (single worker → distributed exchange → fault
tolerance). Up next: M5 hardening (auth, HA, JDBC) and M3.5+
(Iceberg catalog, HashJoin). See
[docs/roadmap/milestones.md](docs/roadmap/milestones.md) and the
[sign-off packet](docs/notes/m4-status.md).

## Quickstart

### Single worker

```bash
cargo build --workspace

# Generate a 100k-row sample table
cargo run -p gen-sample-data

# Run a filtered scan
cd crates/pylon-worker
RUST_LOG=pylon=info ../../target/debug/pylon \
  --sql "SELECT id, name FROM sample WHERE amount > 100000" \
  --table sample \
  --path ../../data/sample.parquet \
  --out /tmp/result.parquet

# Inspect the result
cargo run -p verify-output --quiet -- /tmp/result.parquet
```

### Two-worker cluster

```bash
cargo build --workspace --bin pylon-coord --bin pylon-worker
bash tools/e2e/two_worker_smoke.sh
```

Starts a coordinator and two workers, runs a distributed
`GROUP BY` across a real Arrow Flight shuffle, and checks the result.

## Tooling

| Script | Purpose |
|---|---|
| `tools/e2e/two_worker_smoke.sh` | 2-worker cross-process Flight shuffle E2E |
| `tools/chaos/stall_retry_e2e.sh` | spill → `TASK_STALLED` checkpoint → DONE |
| `tools/chaos/kill_worker_e2e.sh` | SIGKILL a worker mid-task; asserts bounded terminal state |
| `tools/chaos/fte_kill_e2e.sh` | mid-task kill with input replay (exact result) |
| `tools/chaos/s8_signoff_e2e.sh` | M4 sign-off: baseline vs mid-task-kill run at scale |

Tuning knobs and the sample-data generator are documented in
[docs/operations.md](docs/operations.md).

## Workspace layout

```
crates/
├── pylon-types/        Shared types, errors, MemoryPool, IPC codec
├── pylon-plan/         SQL → LogicalPlan → PhysicalPlan
├── pylon-runtime/      PipelineOp + Driver + operators (scan, filter,
│                       project, hash aggregate with spill, exchange)
├── pylon-exchange/     Arrow Flight transport + IPC codec + FTE logs
├── pylon-proto/        gRPC stubs (Worker service)
├── pylon-coord/        Coordinator binary (HTTP + gRPC + Fragmenter)
├── pylon-worker/       Worker binary (gRPC + Flight + pipelines)
└── pylon-catalog / -iceberg / -storage   Connector SPIs (M3.5+)

tools/
├── gen-sample-data/    Sample Parquet generator (--rows/--groups)
├── verify-output/      Parquet reader/printer
├── e2e/                Cross-process smoke E2E
└── chaos/              Fault-injection + sign-off testbed
```

## Architecture

Two binaries, two-stage execution: the coordinator parses SQL, plans a
physical DAG, cuts it into hash-partitioned stages, and dispatches
tasks to workers; workers run Velox-style operator pipelines and
shuffle partitions over Arrow Flight. Aggregates spill to disk under
per-task byte budgets and checkpoint their way to fault tolerance.

- Full design: [docs/architecture.md](docs/architecture.md)
- Key ADRs and RFCs: [docs/rfcs/](docs/rfcs/)

## Documentation

See [docs/README.md](docs/README.md) for the full map — architecture
and ADRs for contributors, operations for running/testing, roadmap for
what's next, and `docs/notes/` for the append-only development record.

## License

Apache-2.0
