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

## Architecture

Two binaries, two-stage execution: the coordinator parses SQL, plans a
physical DAG, cuts it into hash-partitioned stages, and dispatches
tasks to workers; workers run Velox-style operator pipelines and
shuffle partitions over Arrow Flight. Aggregates spill to disk under
per-task byte budgets and checkpoint their way to fault tolerance.