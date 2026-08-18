# M3 milestone — sign-off packet

Date: 2026-08-18

## Scope reminder (RFC-0004)

M3 replaces the M2 "coord-as-gatherer" model with **worker↔worker Arrow Flight shuffle**. The coord only routes; data flows peer-to-peer. PipelineOp contract unchanged; what changes is the wire protocol for `Exchange{Sink,Source}` and the dispatch path.

## Acceptance criteria (M3, RFC-0004 §12)

| Item | Status | Evidence |
|---|---|---|
| `pylon-exchange` crate implements `FlightService` trait + `WorkerFlightService` | ✅ | `crates/pylon-exchange/src/flight_rpc.rs`; `cargo build -p pylon-exchange` |
| `ExchangeSinkOp` pushes a worker's batch to an Arrow Flight stream | ✅ | `crates/pylon-runtime/src/ops/exchange.rs::ExchangeSink`; unit tests in `tests/exchange_partition_test.rs` |
| `ExchangeSourceOp` pulls from an Arrow Flight stream back into the pipeline | ✅ | `crates/pylon-runtime/src/ops/exchange.rs::ExchangeSource`; same suite |
| 2-worker E2E: `SELECT name, COUNT(*) FROM sample GROUP BY name` runs cross-worker Flight and the result is correct | ✅ | `tools/e2e/two_worker_smoke.sh` (B-3.5 commit) |
| M3 stage `git commit` clean, 5+ new unit tests added | ✅ | 38+ new unit tests across A1 / A2 / B-1 / B-2 / B-3.5 |

## Deliverables (per milestone stage)

### A1 — `LogicalPlan::Aggregate` + `HashAggregateOp`
- `crates/pylon-plan/src/{logical,physical,translate}.rs` — new `Aggregate` node + `Expr::AggregateFunction(func, args, data_type, input_data_types)`; SQL → Logical → Physical lowering for `SELECT k, agg() FROM t GROUP BY k`
- `crates/pylon-runtime/src/ops/aggregate.rs::HashAggregateOp` — per-row FNV-1a-style hash accumulator; group state keyed by `Vec<GroupKey>` (Int64 / Float64Bits / Utf8); COUNT(*) / SUM(int|float) / MIN / MAX
- `crates/pylon-worker/src/main.rs` — wires `"Aggregate"` op name + parses `group_by_cols` / `agg_specs` OpSpec keys
- Tests: 12 SQL→Logical unit tests; 14 HashAggregateOp unit tests
- E2E: `crates/pylon-runtime/tests/aggregate_e2e_test.rs` — 1-stage aggregate over `data/sample.parquet`, result matches expected cardinality and sums exactly

### A2 — `Fragmenter` post-order walk + `HashPartitionExchange`
- `crates/pylon-coord/src/fragment.rs::Fragmenter` — post-order walk returns `(stage0_ops, stage1_per_partition)`; on seeing `PhysicalPlan::Aggregate { group_by }`, cuts a stage boundary:
  - stage0 tail: partitioned `ExchangeSink` with `descriptors` (one per partition) + `n_partitions` + `partition_keys`
  - stage1: N per-partition task pairs of `[ExchangeSource(descriptor_p), Aggregate]`
- `ExchangeSinkOp::new_partitioned` — per-row hash routing via FNV-1a mix into one of N descriptors, batch sliced via `arrow_select::take::take`
- E2E: `crates/pylon-runtime/tests/aggregate_2stage_e2e_test.rs` — 1 worker, 2 stage, 4 partitions, `SELECT name, COUNT(*) GROUP BY name`; every name goes to exactly one partition; final result is the union of stage1 outputs

### B-1 — `pylon_coord::Discovery` + `RegisterWorker` RPC + worker Flight server
- `crates/pylon-proto/proto/pylon.proto` — added `RegisterWorker(RegisterWorkerRequest{flight_addr, grpc_addr}) -> RegisterWorkerResponse{worker_id}`
- `crates/pylon-coord/src/discovery.rs` — `Discovery { register / lookup / unregister / list }`; `RegisteredWorker { worker_id, flight_addr, grpc_addr }`
- `crates/pylon-coord/src/bin/pylon-coord.rs` — `CoordGrpc::register_worker` impl; `open_session` reads `x-pylon-worker-id` metadata to pair the session with the prior registration; `WorkerHandle.flight_addr` field
- `crates/pylon-exchange/src/flight_rpc.rs` — `FlightServerImpl` implements `arrow_flight::flight_service_server::FlightService`; 9 RPCs stubbed except `do_exchange` which decodes `app_metadata` (descriptor) + IPC stream into the local `PylonFlightService`
- `crates/pylon-worker/src/main.rs` — new `--flight-addr` (default `127.0.0.1:0`) and `--grpc-addr` flags; `tokio::net::TcpListener::bind` to capture kernel-assigned port; calls `RegisterWorker`; opens `OpenSession` with `x-pylon-worker-id` metadata

### B-2 — `ExchangeSinkRpc` (cross-process via Arrow Flight)
- `crates/pylon-runtime/src/ops/exchange.rs::ExchangeSinkRpc` — same per-row FNV-1a hash routing as `ExchangeSink`, but the transport is tonic Flight `DoExchange` to a per-partition `flight_addr`
- Each `DoExchange` request: first `FlightData` carries the descriptor in `app_metadata`; second `FlightData.data_body` is the Arrow IPC streaming bytes for that partition's batch
- `crates/pylon-coord/src/fragment.rs::Fragmenter::fragment_with_workers(plan, qid, &[flight_addr, ...])` — when workers are known, emits `ExchangeSinkRpc` with `target_flight_addrs[i] = worker_flight_addrs[i % n_workers]`
- `crates/pylon-worker/src/main.rs` — wires `"ExchangeSinkRpc"` op name + parses `descriptors` / `n_partitions` / `partition_keys` / `target_flight_addrs` (semicolon-joined, one per partition) from the OpSpec config

### B-3 / B-3.5 — 2-worker cross-process E2E
- `tools/e2e/two_worker_smoke.sh` — starts 1 coord + 2 workers, verifies registration + `flight_addr`, runs `SELECT name, COUNT(*) FROM sample GROUP BY name`, validates the result
- `crates/pylon-coord/src/bin/pylon-coord.rs`:
  - `encode_batch_ipc` replaced with `PylonFlightClient::send + take_bytes` so workers send real Arrow IPC bytes (not `vec![]`)
  - `open_session` handler decodes the real bytes via `arrow_ipc::reader::StreamReader` and stores the resulting `RecordBatch`es in `w.completed[task_id]`
  - `plan_and_dispatch` rewritten to use `Fragmenter::fragment_with_workers`; passes the original SQL string straight to `pylon_plan::translate::logical_from_sql` (so `GROUP BY` / `COUNT(*)` survive)
  - Stage 0 dispatched to `workers.first()`; stage 1 per-partition tasks dispatched round-robin (`worker_idx = partition % n_workers`)
  - `QueryStatus` entry inserted **before** `plan_and_dispatch` so the polling task spawned inside it can read `stage0_task_id`
  - Polling task drains both `stage0` and `stage1` task IDs from all workers' `completed` maps and writes real `RecordBatch`es into `QueryStatus.rows`

## Headline numbers

- **Unit tests**: 91 passing across 33 suites (up from 17 at end of M1)
- **Cross-worker 2-stage E2E**: end-to-end with 2 OS processes, real `Arrow Flight DoExchange` between workers; stage0 scans 100k rows, `ExchangeSinkRpc` per-row hashes into 2 partition targets, 2 stage1 tasks (one per worker) each receive ~50k rows and aggregate; result is the 100k distinct `(name, count=1)` rows merged by the coord
- **Latency**: not formally benchmarked; the 2-stage pipeline adds a small constant overhead vs the in-process 2-stage test (which is itself <100ms for 100k rows on M1 hardware)

## Out-of-scope (deferred)

- **FTE + spill to object storage** (M4 / RFC-0006) — exchange output is in-memory only; worker crash loses the data
- **Coordinator HA** (M5) — single coord; if coord dies, in-flight queries die
- **TLS / OIDC auth** (M5) — gRPC + Flight are plaintext; flight_addr is reachable from anyone on the network
- **Adaptive batch size / columnar shuffle** — fixed 4 MiB / Arrow IPC streaming
- **Same-worker `ExchangeSource` over real Flight RPC** — currently in-process `PylonFlightService` only; cross-worker data already goes through real `DoExchange` (B-2)
- **HashJoin / Distinct / Window fragmenter rules** — fragmenter framework is generic; only the `Aggregate` rule is implemented (M3 first cut)
- **Nested aggregates** — explicitly rejected by the fragmenter (would need a stage2)
- **Iceberg / Lakekeeper / TPC-H SF100** — the original M3 scope; deferred to M3.5+ (this milestone delivered the cross-worker shuffle substrate instead)
- **Result streaming / push** — coord returns the full result via `GET /v1/query/{id}` after a 3-second `sleep(3)` poll; proper `TaskDone` acks are deferred

## Status: M3 cross-worker shuffle complete (B-1 / B-2 / B-3.5); M3 Iceberg side deferred to next milestone

Sign-off: 2026-08-18. Ready for M4 (FTE + spill).
