# Pylon — Pipeline-first Rust SQL query engine

**Status**: M1 milestone complete (single-worker pipeline). See [docs/roadmap/milestones.md](docs/roadmap/milestones.md).

Pylon is an Apache Arrow-native Rust query engine targeting the Presto/Trino use cases with reduced JVM/serialization overhead and a pipeline-driven execution model inspired by Velox.

## What's working in M1

- ✅ `pylon-types`: shared base types and error model
- ✅ `pylon-plan`: SQL → LogicalPlan → PhysicalPlan (sqlparser 0.55)
- ✅ `pylon-runtime`: PipelineOp trait (Velox-style 7-method contract), per-task tokio driver
- ✅ 3 ops: `SeqScanOp` (Parquet), `FilterOp` (>, <, =, ≠, ≥, ≤), `ProjectOp` (column subset)
- ✅ `pylon-worker` binary: end-to-end `SELECT ... FROM ... WHERE ...` running 100K rows in <100ms
- ⏳ 5 other crates (catalog, exchange, iceberg, storage, coord): stubs only, filled in later milestones

## Quickstart

```bash
# Build everything
cargo build --workspace

# Generate a 100K-row Parquet sample table
cargo run -p gen-sample-data

# Run a query
cd crates/pylon-worker
RUST_LOG=pylon=info ../../target/debug/pylon \
  --sql "SELECT id, name FROM sample WHERE amount > 100000" \
  --table sample \
  --path ../../data/sample.parquet \
  --out /tmp/result.parquet

# Inspect the output
cargo run -p verify-output --quiet -- /tmp/result.parquet
```

Expected output for the example above:

```
rows: 33333
schema (2 columns):
  - id : Int64
  - name : Utf8
sample rows:
  id=66667 name=name_66667
  ...
```

## Supported SQL (M1)

```sql
SELECT [* | col1, col2, ...] FROM <table> [WHERE <col> <op> <literal>]

ops:    >  <  >=  <=  =  <>
ints:   5, 1000
floats: 0.01, 1.5e10
strs:   'foo', 'name_00042'

LIMITATIONS (planned for M2+):
  - AND/OR in WHERE (M1 only accepts one predicate)
  - JOIN
  - Aggregates
  - distributed execution
```

## Architecture

See [docs/rfcs/0001-architecture.md](docs/rfcs/0001-architecture.md) for the full RFC and [docs/research/findings.md](docs/research/findings.md) for the research that backs every architectural decision.

**Key ADRs**:

1. **Two-binary split** (coordinator / worker) — planned, single binary M1
2. **arrow-rs directly, no DataFusion runtime** — DataFusion's pull-stream ExecutionPlan is fundamentally incompatible with pipeline MPP; we borrow the kernels and type system only
3. **Velox Operator / Driver / Task** as the runtime reference
4. **Doris "fixed thread pool = CPU core count"** as a hard scheduler constraint
5. **HashJoinBridge** (Velox + Trino) for build/probe state sharing
6. **Arrow Flight + FTE** for shuffle + fault tolerance (M4)
7. **Iceberg REST Catalog** as the only catalog (Lakekeeper default; Polaris alt)
8. **No Substrait in v1** — same engine, no cross-engine requirement yet

## Workspace layout

```
crates/
├── pylon-types/      Shared types
├── pylon-plan/       SQL → LogicalPlan → PhysicalPlan
├── pylon-runtime/    PipelineOp + Driver + 3 ops
├── pylon-exchange/   (M2) Arrow Flight server/client
├── pylon-catalog/    (M3) Iceberg REST Catalog client
├── pylon-iceberg/    (M3) Iceberg table reader
├── pylon-storage/    (M3) object_store abstraction (S3/GCS/ADLS)
├── pylon-coord/      (M2) Coordinator binary
└── pylon-worker/     CLI binary; M1 runs queries

tools/
├── gen-sample-data/  Generates a 100K-row test Parquet
└── verify-output/    Reads a Parquet and prints row count + sample
```

## License

Apache-2.0
