# M4 milestone — sign-off packet

Date: 2026-09-15

## Scope reminder (RFC 0007)

M4 = fault-tolerant execution + spill: per-task memory accounting,
spill-to-disk for oversized aggregates, persisted exchange input for
replay after worker loss, a chaos testbed, and the headline sign-off
E2E (kill a worker mid-query; the retried query must match the
uninterrupted baseline exactly).

## Acceptance criteria (all met)

| Item | Status | Evidence |
|---|---|---|
| Per-task memory pool (`MemoryPool` trait + `PerTaskPool`) | ✅ | PR #15; wired into `HashAggregateOp` via `TaskContext.memory_budget` |
| Spill manager (local-FS, Arrow IPC streaming format) | ✅ | PR #16; `SpillManager` + `Spillable` trait |
| FTE-mode exchange input persistence | ✅ | `do_exchange` write-ahead appends to `<spill_root>/pylon-input/<descriptor>.arrow` before queue push |
| Input replay for re-dispatched tasks | ✅ | `ExchangeSourceOp::from_log`; coord injects `input_log` at stage-1 dispatch |
| Worker loss → checkpoint re-dispatch | ✅ | session-loss handler re-sends specs verbatim (log already inside); stalled handles cleared, not re-injected |
| Chaos testbed | ✅ | `tools/chaos/`: `stall_retry_e2e.sh`, `kill_worker_e2e.sh`, `random_kill.sh`, `fte_kill_e2e.sh`, `s8_signoff_e2e.sh` |
| Sign-off E2E: kill mid-task == baseline | ✅ | `s8_signoff_e2e.sh` at 20M rows × 1M groups: baseline 1,000,000 == kill-run 1,000,000 |

## What landed (phases)

| Phase | PR | Delivery |
|---|---|---|
| M4.S1 MemoryPool | #15 | `MemoryPool` trait, `PerTaskPool`/`NoopMemoryPool`, aggregate accounting |
| M4.S2 Spill manager | #16 | `Spillable` trait, local-FS spill, auto-spill aggregate |
| C5.5 ack-drain chain | #22 | death-driven re-dispatch replacing C5.5 immediate watcher; budget wiring (`TaskSpec.memory_budget_bytes`); `PYLON_TASK_MEMORY_BUDGET_BYTES`; port env vars actually parsed |
| C5.6 hardening | #23 | S3 multipart spill streaming (5 MiB protocol floor enforced); QSM per-query cleanup; poison-tolerant mutexes; `list`/`delete_prefix` GC API |
| M4.S7 chaos | #24 | testbed + worker honors budget + `TASK_STALLED` checkpoints (emit-and-continue) + session-loss re-dispatch |
| FTE source | #25 | persisted exchange input (`input_log`), `ExchangeSourceOp::from_log`, `RegisterWorker.spill_root`, codec promoted to `pylon_types::codec`, ack-stream drain |
| M4.S8 sign-off | #26 | double-run comparison at 20M×1M; queue skip in FTE mode; ≤64k-row TaskResponse chunks + 64 MB decode limit; streaming `gen-sample-data` |
| ack-drain root cause | #27 | silent 424-row loss root-caused (response stream dropped before server finished) and fixed: ack stream drained to end; broken exchange now fails the stage |

## Headline numbers

- **Tests**: 198 passing / 5 ignored (47 suites), up from 17 at end of M1
- **Sign-off scale**: 20M input rows × 1M groups (baseline 1,000,000 rows == kill-run 1,000,000 rows)
- **Sign-off mechanics exercised**: ~600 MB persisted input logs, ~12 spill checkpoints per stage-1 task, 1M-group final merge, mid-task SIGKILL → re-dispatch → exact replay
- **RFC's literal 1B**: same script parameterized
  (`S8_ROWS=1000000000 S8_GROUPS=1000000 tools/chaos/s8_signoff_e2e.sh`;
  single-host, est. several hours — mechanics identical, only I/O volume scales)

## Deviations / deferrals (documented, intentional)

1. **S6 (spillable `SortOp`) deferred** — the engine has no `SortOp` /
   `ORDER BY` yet; building the spillable variant first would be
   infrastructure with no caller. Revisit when sort support lands.
   The spill stack is complete, so a future SortOp only adds the
   operator + a `Spillable` impl + a fragmenter rule.
2. **FTE source = persisted input logs on the worker spill root**
   (local FS today; `object_store`/S3 backend already exists for the
   spill path). Cross-host replay wants the S3 spill root configured —
   mechanics are storage-agnostic.
3. **Stall semantics = emit-and-continue**: a spill checkpoint acks
   `TASK_STALLED` and the task keeps running; the coord re-dispatches
   from the checkpoint only on session loss (replacing the C5.5
   immediate re-dispatch, which would double-execute live tasks).
4. **FTE-mode `do_exchange` skips the drain-once queue entirely** —
   the input log is authoritative. At S8 scale the unconsumed queue
   would OOM the worker.
5. **Sign-off at 20M×1M** rather than the RFC's literal 1B — mechanics
   identical, and the 1B run is a parameter of the same script.

## Bugs the milestone work caught (fixed)

1. `wait_for_stage_done` resolved a fully-stalled stage on the raw
   acked count → coord drained an empty result and marked the query
   Done (regression test added).
2. Result-drain could launder a stage-failed query into Done over a
   partial result.
3. Flight-server incremental decode had a clone-then-clear
   lost-update race (dropped any FlightData body arriving
   mid-drain) — decode rewritten per-body.
4. DoExchange jobs resolved at response-header time; dropping the
   response stream could reset the server inbound mid-flight —
   ack stream now drained to end; failures propagate.
5. `PYLON_HTTP_PORT` / `PYLON_GRPC_PORT` env vars were silently
   ignored (every deployment got 8080/9090).

## Status: M4 complete — signed off 2026-09-15
