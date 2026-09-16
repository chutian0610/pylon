# Architecture

Pylon executes SQL as a pipeline of columnar operators, coordinated
across a small cluster of worker processes. It is Arrow-native end to
end (no JVM, no DataFusion runtime) and borrows its runtime vocabulary
from Velox: **Operator / Driver / Task / Stage**.

## Query flow

```
SQL
  → parse (sqlparser)
  → LogicalPlan
  → PhysicalPlan (arrow-rs backed operators)
  → Fragmenter (cuts stage boundaries, injects hash-partition exchanges)
  → StageDag → TaskSpecs
  → workers run Stage pipelines; stages shuffle via Arrow Flight DoExchange
```

A GROUP BY query becomes two stages: stage 0 scans and hash-partitions
rows to per-partition workers; stage 1 aggregates each partition and
the coordinator unions the results.

## Components

| Crate | Role |
|---|---|
| `pylon-types` | Shared value types, `PylonError`, `MemoryPool` trait, IPC codec |
| `pylon-plan` | sqlparser → `LogicalPlan` → `PhysicalPlan` (`Aggregate` with `COUNT/SUM/MIN/MAX` + `GROUP BY`) |
| `pylon-runtime` | `PipelineOp` trait (Velox-style 7-method contract), single-thread `Driver`, ops: `SeqScanOp` (Parquet), `FilterOp`, `ProjectOp`, `PartitionFilterOp`, `HashAggregateOp` (spillable), `ExchangeSinkRpc` / `ExchangeSourceOp` |
| `pylon-exchange` | Arrow Flight transport: `FlightServerImpl`, `PylonFlightClient`, IPC codec, FTE input-log persistence |
| `pylon-proto` | gRPC `Worker` service: `RegisterWorker`, bidi `OpenSession` |
| `pylon-coord` | HTTP API (`POST /v1/query`, `GET /v1/query/{id}`, `GET /v1/workers`), `Discovery` registry, `Fragmenter`, `QueryStateMachine` |
| `pylon-worker` | One process hosting the gRPC + Arrow Flight servers and the pipeline runner |
| `pylon-catalog` / `pylon-iceberg` / `pylon-storage` | Connector SPIs (Iceberg REST catalog, object storage) — stubs, M3.5+ |

## Fault tolerance (M4)

- Every task runs under a byte budget (`PerTaskPool`). When
  `HashAggregateOp` exceeds it, state spills to Arrow IPC files and a
  `TASK_STALLED` checkpoint (spill handle) is acked upstream
  (emit-and-continue).
- Stage-0's exchange persists every batch to the partition owner's
  **input log** (`<spill_root>/pylon-input/<descriptor>.arrow`,
  write-ahead before the drain-once queue).
- Stage-1 tasks receive their `input_log` path at dispatch; on worker
  loss the coord re-dispatches the spec verbatim to a survivor, which
  replays the log from offset 0 — a full, correct recompute.
- The sign-off (`tools/chaos/s8_signoff_e2e.sh`) kills a worker
  mid-task and requires the result to equal the uninterrupted baseline
  exactly (verified at 20M rows × 1M groups).

## Key ADRs

1. **Two-binary split** (coordinator / worker)
2. **arrow-rs directly, no DataFusion runtime** — DataFusion's
   pull-stream `ExecutionPlan` is fundamentally incompatible with
   pipeline MPP; we borrow the kernels and type system only
3. **Velox Operator / Driver / Task** as the runtime reference
4. **Doris "fixed thread pool = CPU core count"** as a hard scheduler
   constraint
5. **HashJoinBridge** (Velox + Trino) for build/probe state sharing
6. **Arrow Flight + FTE** for shuffle + fault tolerance
7. **Iceberg REST Catalog** as the only catalog (Lakekeeper default;
   Polaris alt) — M3.5+
8. **No Substrait in v1** — same engine, no cross-engine requirement

## Two-stage walkthrough

What `tools/e2e/two_worker_smoke.sh` exercises:

- Each worker calls `RegisterWorker` with its `flight_addr`
- The coord runs `Fragmenter::fragment_with_workers` to build the DAG
- Stage 0 is dispatched to worker 0; stage 1 partition `p` goes to
  worker `p % n_workers`
- Stage 0's `ExchangeSinkRpc` opens a tonic `DoExchange` to each
  worker's Flight server
- Each stage-1 worker pulls via `ExchangeSourceOp` from its local
  `PylonFlightService` (or, in FTE mode, replays the persisted input
  log)

## Further reading

- Design decisions: [rfcs/](rfcs/)
- Stability boundaries: [design/trait-stability.md](design/trait-stability.md)
- Development history / sign-off packets: [notes/](notes/)
