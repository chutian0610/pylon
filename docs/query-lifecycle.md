# Query lifecycle — following one query through Pylon

A guided tour for new contributors: what happens, in order, when

```sql
SELECT name, COUNT(*) FROM sample GROUP BY name
```

is submitted to a 1-coordinator + 2-worker cluster. File paths point
at the code to read next; the fastest way to see all of it live is
`bash tools/e2e/two_worker_smoke.sh`.

## The cast

| Actor | Runs | Does |
|---|---|---|
| **Coordinator** | `pylon-coord` binary | accepts SQL, plans, cuts stages, dispatches tasks, unions results |
| **Worker ×N** | `pylon-worker` binary | hosts a gRPC + Arrow Flight server, runs operator pipelines |
| **Driver / Pipeline** | inside each worker task | single-thread poll loop over one stage's operators |

## 1. Submit — HTTP in

`crates/pylon-coord/src/bin/pylon-coord.rs`

- `POST /v1/query` → `submit_query`: stores a `QueryStatus` entry
  (state = Running) keyed by a fresh `QueryId`, then spawns
  `plan_and_dispatch`.
- The HTTP request returns immediately with `{"query_id": ...}`;
  the client polls `GET /v1/query/{id}` for state + rows.

## 2. Plan — SQL to physical DAG

`crates/pylon-plan/src/translate.rs`

- `parse_sql` (sqlparser) → `Statement`
- `logical_from_sql(sql, &catalog)` → `LogicalPlan` — an
  `Aggregate { group_by: [name], aggs: [COUNT, SUM] }` over a
  `Scan`
- `physical_from_logical(logical)` → `Arc<dyn ExecutionPlan>` — the
  arrow-rs backed operator tree (`SeqScanExec` → `AggregateExec`)

## 3. Fragment — cut the stage boundary

`crates/pylon-coord/src/fragment.rs`

- `Fragmenter::fragment_with_workers(plan, qid, &[flight_addrs])`
  walks the plan; on `Aggregate` it cuts a stage boundary and
  injects a hash-partition exchange:
  - **stage 0**: `SeqScan` → `ExchangeSinkRpc` (per-row FNV-1a hash
    of `name` → partition `p`, slice the batch, send slice `p` to
    worker `p`'s Flight server)
  - **stage 1**: one task per partition — `ExchangeSource` →
    `HashAggregate`

## 4. Dispatch — TaskSpecs over gRPC

`crates/pylon-coord/src/bin/pylon-coord.rs`

- `split_dag_for_dispatch` flattens the DAG into stage-0 +
  per-partition stage-1 op lists
- stage-1 `ExchangeSource` ops get their `input_log` path injected
  (FTE: the persisted input log on the partition owner's spill root)
- Each `TaskSpec` ships over the worker's `OpenSession` stream;
  `QueryStateMachine::register_stage` arms the per-stage barrier —
  the coord advances only when every dispatched task acks `DONE`

## 5. Worker — ops registry to pipeline

`crates/pylon-worker/src/main.rs`

- `run_task` builds ops via `op_registry` (`TaskContext` carries the
  task memory budget + spill-checkpoint callback), then
  `Driver::new(pipeline).run()`

## 6. Stage 0 — scan, partition, ship

`crates/pylon-runtime/src/driver.rs` + `ops/exchange.rs`

- `SeqScanOp` streams 8192-row batches out of the Parquet file
- `ExchangeSinkRpc::add_input` hash-partitions each batch, encodes
  one slice per target, and spawns a DoExchange job (handles join in
  `no_more_input`)
- target side (`crates/pylon-exchange/src/flight_rpc.rs`):
  `do_exchange` **appends each batch to the partition's input log**
  (write-ahead) **before** queueing it for stage 1

## 7. Stage boundary — join, ack, dispatch stage 1

- `ExchangeSinkRpc::no_more_input` drains the ack stream to the end
  ⇒ the input log is complete ⇒ the worker acks `TASK_DONE`
- coord's session handler → `QueryStateMachine` stage-0 done → the
  waiting dispatcher injects `input_log` paths and dispatches stage 1

## 8. Stage 1 — replay, aggregate (spill + checkpoints)

`crates/pylon-runtime/src/ops/aggregate.rs`

- `ExchangeSourceOp` drains its queue (or replays the persisted
  input log) feeding `HashAggregateOp`
- `add_input`: `PerTaskPool.try_grow` per batch; over budget →
  **spill** state to Arrow IPC (`SpillManager`) + ack a
  `TASK_STALLED` checkpoint, then retry the batch
- `no_more_input`: reload every spill file, merge, emit ONE final
  batch of groups

## 9. Collect — results back over HTTP

`crates/pylon-coord/src/bin/pylon-coord.rs`

- each `TaskResponse.batch` (IPC stream) is decoded into the coord's
  per-task `completed` map; `TASK_DONE` acks advance the stage
  barrier
- after the last stage: completed batches are unioned, `rows_total`
  is set, and `GET /v1/query/{id}` returns the result

## Where fault tolerance engages

| Hook | Where | What happens |
|---|---|---|
| memory budget | `aggregate.rs` `add_input` | over-budget batch triggers spill + checkpoint |
| spill checkpoint | `Spillable::spill` → `on_spill` | worker acks `TASK_STALLED` (emit-and-continue) |
| worker loss | coord `open_session` stream end | stalled checkpoints re-dispatched to a survivor |
| replay | `ExchangeSourceOp::from_log` | survivor replays the full persisted input log |
| ordering | `ExchangeSinkRpc::no_more_input` | ack stream drained ⇒ input logs complete before stage 1 starts |

## See it in code

- unit level: `crates/pylon-runtime/tests/` (hash aggregate, exchange
  partitioning, spill roundtrip)
- end-to-end: `tools/e2e/two_worker_smoke.sh`, then
  `tools/chaos/stall_retry_e2e.sh` and
  `tools/chaos/fte_kill_e2e.sh` for the fault-tolerance paths
