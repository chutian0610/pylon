# Pylon — Pipeline-first Rust SQL query engine

**Status**: M1, M2, M3 milestones complete (single worker → multi-worker gRPC + in-process exchange → cross-worker Arrow Flight shuffle). Next: M4 (FTE + spill to object storage). See [docs/roadmap/milestones.md](docs/roadmap/milestones.md) and [docs/notes/m3-status.md](docs/notes/m3-status.md).

Pylon is an Apache Arrow-native Rust query engine targeting the Presto/Trino use cases with reduced JVM/serialization overhead and a pipeline-driven execution model inspired by Velox.

## What's working today

- ✅ `pylon-types`: shared base types and error model
- ✅ `pylon-plan`: SQL → LogicalPlan → PhysicalPlan (sqlparser 0.55), incl. `Aggregate` with `COUNT` / `SUM` / `MIN` / `MAX` and `GROUP BY`
- ✅ `pylon-runtime`: PipelineOp trait (Velox-style 7-method contract); `Driver::run` with true single-thread poll loop; ops:
  - `SeqScanOp` (Parquet) · `FilterOp` (>, <, =, ≠, ≥, ≤) · `ProjectOp` (column subset)
  - `PartitionFilterOp` (`id % n == p`) · `HashAggregateOp` (per-row hash group aggregate) · `ExchangeSinkOp` / `ExchangeSourceOp` (in-process partitioned via `PylonFlightService`)
  - `ExchangeSinkRpc` (cross-process via real Arrow Flight `DoExchange`) · `ExchangeSourceOp` reads from local Flight server
- ✅ `pylon-exchange`: in-process `PylonFlightService` (descriptor → `Vec<RecordBatch>` map) + `PylonFlightClient` (real Arrow IPC streaming encode/decode) + `FlightServerImpl` (tonic `arrow_flight::flight_service_server::FlightService` impl)
- ✅ `pylon-proto`: gRPC `Worker` service with `RegisterWorker(flight_addr, grpc_addr) -> worker_id` and `OpenSession` (bidi `stream<TaskRequest, TaskResponse>`)
- ✅ `pylon-coord`: HTTP API (`POST /v1/query`, `GET /v1/query/{id}`, `GET /v1/workers`); `pylon_coord::Discovery` registry; `Fragmenter` with post-order walk + `HashPartitionExchange` injection (per-row FNV-1a hash routing)
- ✅ `pylon-worker` binary: gRPC + Arrow Flight server in one process; `--flight-addr` / `--grpc-addr` flags; calls `RegisterWorker` then `OpenSession` with `x-pylon-worker-id` metadata
- ✅ Cross-process 2-worker E2E: `tools/e2e/two_worker_smoke.sh` (1 coord + 2 workers, `SELECT name, COUNT(*) FROM sample GROUP BY name` runs with real Arrow Flight `DoExchange` between workers)

## Quickstart — single worker (M1)

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

Expected output for the example above: `rows: 33333`.

## Quickstart — 2-worker cross-process (M3)

```bash
# Build the binaries
cargo build --workspace --bin pylon-coord --bin pylon-worker

# Run the smoke E2E (starts 1 coord + 2 workers, runs a query, checks result)
bash tools/e2e/two_worker_smoke.sh
```

What this exercises:
- Each worker calls `RegisterWorker` on the coord with its `flight_addr`
- The coord uses `Fragmenter::fragment_with_workers(plan, qid, &[flight_addr_0, flight_addr_1])` to build the DAG
- Stage 0 is dispatched to worker 0; stage 1 partition `p` is dispatched to worker `p % n_workers`
- Stage 0's `ExchangeSinkRpc` opens a tonic `DoExchange` to each worker's Flight server
- Each stage 1 worker pulls via `ExchangeSource` from its local `PylonFlightService` (fed by the local Flight server)

See `docs/notes/m3-status.md` for the full accept criteria + numbers.

## Architecture

See [docs/rfcs/0001-architecture.md](docs/rfcs/0001-architecture.md) for the full RFC and [docs/research/findings.md](docs/research/findings.md) for the research that backs every architectural decision.

**Key ADRs**:

1. **Two-binary split** (coordinator / worker)
2. **arrow-rs directly, no DataFusion runtime** — DataFusion's pull-stream `ExecutionPlan` is fundamentally incompatible with pipeline MPP; we borrow the kernels and type system only
3. **Velox Operator / Driver / Task** as the runtime reference
4. **Doris "fixed thread pool = CPU core count"** as a hard scheduler constraint
5. **HashJoinBridge** (Velox + Trino) for build/probe state sharing
6. **Arrow Flight + FTE** for shuffle + fault tolerance (M4: FTE pending)
7. **Iceberg REST Catalog** as the only catalog (Lakekeeper default; Polaris alt) — M3.5+
8. **No Substrait in v1** — same engine, no cross-engine requirement yet

## Workspace layout

```
crates/
├── pylon-types/        Shared types
├── pylon-plan/         SQL → LogicalPlan → PhysicalPlan
├── pylon-runtime/      PipelineOp + Driver + 8 ops (incl. Exchange + HashAggregate)
├── pylon-exchange/     Arrow IPC + PylonFlightClient + FlightServerImpl (tonic)
├── pylon-proto/        gRPC stubs (Worker service)
├── pylon-catalog/      (M3.5+) Iceberg REST Catalog client
├── pylon-iceberg/      (M3.5+) Iceberg table reader
├── pylon-storage/      (M3.5+) object_store abstraction (S3/GCS/ADLS)
├── pylon-coord/        Coordinator binary (HTTP + gRPC + Fragmenter + Discovery)
└── pylon-worker/       Worker binary (gRPC + Flight server + Pipeline runner)

tools/
├── gen-sample-data/    Generates a 100K-row test Parquet
├── verify-output/      Reads a Parquet and prints row count + sample
└── e2e/
    └── two_worker_smoke.sh    2-worker cross-process Flight shuffle E2E
```

## License

Apache-2.0
