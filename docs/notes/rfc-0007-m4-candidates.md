# RFC 0007 (M4) — open candidate list

Date: 2026-08-20
Author: Codex

Open work remaining under the M4 umbrella of `docs/roadmap/milestones.md`.
Captured at end-of-session before PR #16 lands. Each item is a
single-PR-shaped unit; pick when ready.

## Direct M4 chain (RFC 0007 §5)

| ID | Phase | What | Why | Est | Pre-req |
|---|---|---|---|---|---|
| ~~**C3**~~ | ~~M4.S3~~ | ~~`ConnectorCapabilities.fault_tolerant` flag + `pylon-storage` impl~~ **DONE** — SPI adds `ConnectorCapabilities` + `Connector::capabilities()`; `pylon-storage` implements local-FS Arrow IPC `DataSink`/`DataSource`; `SpillManager` routes all I/O through connector trait objects per RFC 0007 §2 rule [b]. | ~~Move spill bytes from local-FS to `DataSink::append` (RFC 0007 §2 rule `[b]`). Production-grade spill path.~~ | ~~~5 dev days~~ | ~~PR #16 (M4.S2)~~ |
| ~~**C4**~~ | ~~M4.S4~~ | ~~`pylon-storage` adds `object_store` dep + `s3://` config~~ **DONE** — `S3SpillStore` wraps `object_store::AmazonS3` behind sync `put`/`get`/`delete`; `S3DataSink`/`S3DataSource` implement Arrow IPC over S3; `SpillManager::with_s3` routes spill to S3; MinIO integration tests pass. | ~~Real S3-compatible ObjectStorage. Without this, the `Connector::supports_fte` flag has no working backend.~~ | ~~~6 dev days~~ | ~~C3~~ |
| ~~**C5**~~ | ~~M4.S5~~ | ~~`QueryStateMachine::TaskAck::Stalled` + coord-side spill-handle bookkeeping~~ **DONE** — `TaskAck::Stalled { spill_handle }` variant; attempt counter + handle registry per (query, stage); proto adds `TASK_STALLED` + `spill_handle` field; coord bin wires the ack. **2 `aggregate_2stage_e2e_test` tests un-ignored** — flaky 500 ms sleep replaced with a deterministic `pending_rows` drain barrier. | ~~Retries use the spill handle instead of restarting from scratch.~~ | ~~~3 dev days~~ | ~~C3 + R7 (DONE)~~ |
| (deferred) | M4.S6 | Spillable `SortOp` (new op) | **Deferred until sort support exists (decision 2026-09-02).** The engine has no `SortOp` and no `ORDER BY` lowering yet — building the spillable variant first would be infrastructure with no caller to validate it. Revisit when any of: `ORDER BY` lands in the planner/executor, TPC-H beyond Q1/Q3 is targeted, or a workload needs over-memory sort. Cost of deferral is low: the spill stack (`Spillable` trait, `SpillManager` local-FS + S3 multipart, retry resume) is complete, so a future SortOp only adds the operator + a `Spillable` impl (sorted-run serialization + k-way merge) + a fragmenter rule. | — | — |
| ~~**M4.S7**~~ | ~~Chaos testbed~~ **DONE (2026-09-02, scoped)** — `tools/chaos/`: `stall_retry_e2e.sh` (deterministic tiny-budget spill → TASK_STALLED checkpoints → clean DONE with the full 100k result) + `kill_worker_e2e.sh`/`random_kill.sh` (SIGKILL a worker around the query; asserts bounded terminal state, per-round outcome reported). Enablers landed with it: worker honors `TaskSpec.memory_budget_bytes` and emits `TASK_STALLED` checkpoints on spill (emit-and-continue); coord re-dispatches a worker's stalled checkpoints **on session loss** (the C5.5 immediate re-dispatch would have double-executed live tasks); coord env `PYLON_TASK_MEMORY_BUDGET_BYTES` + the long-dormant `PYLON_HTTP_PORT`/`PYLON_GRPC_PORT` env vars now actually parse. **Honest scope note**: mid-task kill → the re-dispatched task cannot replay exchange input (consume-once queues), so its result may be partial — full mid-task-kill correctness is gated on FTE persisted exchange output (see new FTE-source row) and is S8's correctness gate. Also fixed en route: the result-drain task could launder a stage-failed query into Done over a partial result; the wait loop resolved a fully-stalled stage prematurely (raw acked count) — both caught by this testbed. | ~~Worker kill mid-query, retry path exercised.~~ | ~~~5 dev days~~ | ~~C5~~ |
| **FTE source** | Persisted exchange output for input replay: ExchangeSink writes its stream to the spill store alongside the checkpoint; re-dispatched tasks re-read consumed input from the persisted log instead of the drain-once queue. Blocks S8's mid-flight-kill correctness assertion. | The S7 chaos run demonstrated the exact gap (re-dispatched task ran to DONE on checkpoint-only state with 0 rows when input was unreplayable). | ~4 dev days | C5.6 (S3 spill store) |
| (skip) | M4.S8 | 1B-row mid-flight-worker-kill E2E | The headline M4 sign-off test. | ~3 dev days | C5 + S7 |

## Adjacent cleanup work

| ID | What | Why | Est |
|---|---|---|---|
| **C5.5** | **DONE** — Stalled→retry chain closed end-to-end: `SpillManager` gains `*_async` (spawn_blocking, RFC 0007 §2 rule [c]) + per-op S3 timeouts; coord dispatches stash `TaskSpec`s and a per-stage retry watcher consumes `stalled_handles` (clear → re-dispatch → on failure put-back); retried tasks carry a `spill_handle` OpSpec key and `HashAggregateOp::with_pending_resume` folds the spilled state at `no_more_input`. | Closed the P1 findings from the M4.S3–S5 code review (missing re-dispatch, unbounded S3 blocking). | — |
| ~~**C5.6**~~ | ~~P2 follow-ups~~ **DONE** — S3 sink streams multipart parts (8 MiB default, 5 MiB protocol floor enforced; steady-state memory = one part, no 5 GB cap); `read_concatenated_ipc` reads the per-batch-stream format (single-stream files still readable); QSM `remove_query` wired at all three query-terminal points; mutex locks poison-tolerant (`into_inner`) across QSM + coord; `S3SpillStore::list`/`delete_prefix` give the store-level orphan-GC API (deployment: MinIO bucket lifecycle rule; coord-side sweep wiring deferred). Fix en route: `tokio::time::timeout` now constructed inside the blocking runtime (C5.5's wrapper panicked outside a reactor context — caught by the MinIO suite). | ~~Production hardening before S7/S8 chaos + 1B-row E2E.~~ | ~~~4 dev days~~ |
| ~~**B**~~ | ~~Mechanical autofix PR: 276 files of `cargo fmt` drift + 22 `clippy` errors~~ **DONE** — `cargo fmt --all` + clippy fixes landed; `fmt-check` + `clippy -D warnings` jobs enabled in `ci.yml`. | ~~Unblocks enabling `fmt-check` + `clippy -D warnings` jobs in `ci.yml` (PR #12 deferred these).~~ | ~~~9 dev days~~ |
| ~~**F**~~ | ~~RFC 0007 §3.1 doc alignment: text says "release at Drop" but the trait surface (PR #15) added `release(bytes: usize)` as an explicit method~~ **DONE** — trait definition and conformance rule updated to match. | ~~Small doc-only follow-up so the RFC and the canonical trait match.~~ | ~~~10 min~~ |
| ~~G~~ | ~~RFC 0005 §3 rule updates — `M4.S1 release` was added as additive to the trait; RFC text should mention it~~ **DONE** — additive note added to `DataSource::next` doc. | ~~Same scope as F. Doc-only.~~ | ~~~10 min~~ |

## Out-of-scope (do not pick up here)

- M1 / M2 DoD deeper sweep (PR #14 closed the obvious `[ ]` rows;
  anything deeper requires real test-coverage tooling — separate effort).
- M3 deferred items (Lakekeeper / Iceberg / TPC-H SF100 / Broadcast
  exchange / HashJoin) — M3.5+ scope per `docs/notes/m3-status.md`.
- Coordinator HA / TLS / OIDC — M5.
- Connector / SortOp / chaos / 1B-row E2E — deferred above.

## When to revisit

After each M4 PR merges, sweep this file: strike the closed item,
bump the dates if any item's "pre-req" chain shifts. The file is
deliberately at the same indent level as the other
`docs/notes/rfc-0005-{r7,r9}-status.md` audit notes — same audit
trail pattern.
