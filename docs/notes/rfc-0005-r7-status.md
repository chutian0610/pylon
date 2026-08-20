# RFC 0005 — R7 status (carry-over note)

Date: 2026-08-20
Conclusion: R7 is live on `main`. No code change required.

## R7 per RFC 0005 §7

> **R7** — `QueryStateMachine` + `TaskDone` ack (M3-tail #1).
> Coord-side `sleep` removed. Low (mechanical). Depends on: none.

Goal: replace the `tokio::time::sleep(2/3 sec)` polling heuristic in
`bin/pylon-coord.rs` with a per-`(query, stage)` state machine that
fires the stage-done barrier on the actual `TaskResponse::TASK_DONE`
ack from workers, instead of at the next polling tick.

## Evidence R7 is on `main`

Anchor: `d610c8e24b0ed0ab76b323f26d2968e548030294` (R7 commit).
`main` tip at the time of writing: `5d693d5...`.

Reproducible checks:

```bash
# d610c8e is a linear ancestor of main
git merge-base d610c8e main          # → d610c8e
git log d610c8e ^main                # → empty

# The two R7-introduced files are byte-identical on main
git diff d610c8e main -- crates/pylon-coord/src/query_state.rs   # → 0 lines
git diff d610c8e main -- crates/pylon-coord/src/lib.rs           # → 0 lines

# The dispatch path is sleep-free
git grep -n 'tokio::time::sleep' crates/pylon-coord/src/bin/pylon-coord.rs  # → only doc references
```

What this means:

- `d610c8e` is a linear ancestor of `main`, so its changes are part
  of `main`'s history verbatim.
- The two R7-introduced files (`query_state.rs` and the `pub use`
  line in `lib.rs`) are byte-identical on `main` vs. the R7 commit.
- In `bin/pylon-coord.rs`, `state_machine` is held in `CoordState`
  (init at construction), `register_stage` is called by the
  dispatcher after each stage's tasks are emitted,
  `wait_for_stage_done` parks the polling task until the last
  `TASK_DONE` ack arrives (instead of `sleep`), and `ack_task` is
  called from the inbound `OpenSession` handler for every
  `TASK_DONE` / `TASK_FAILED` response.
- The 8 unit tests in `crates/pylon-coord/src/query_state.rs::tests`
  are present (registration, single- and multi-ack paths,
  Failed-ack fail-fast, deadline timeout, zero-task stage,
  multi-`(query, stage)` isolation, multi-waiter unblock).
- `tools/e2e/two_worker_smoke.sh` is the cross-process integration
  harness used to validate R7; intact at `main` HEAD.

If a future regression reintroduces a `sleep` in the dispatch path,
the recipe above will surface it (the `git grep` row will hit a real
call site instead of a doc reference, and the diff against
`d610c8e` will no longer be empty).

## Why no GitHub PR number

R7 was merged into `main` directly before the team adopted the
stacked-squash PR workflow that produced the visible R6 PR series
(PRs `#1`–`#5` against `chutian0610/pylon`). There is no PR #N
recorded on GitHub for R7; the commit is its own evidence on
`main`.

## What this PR does

None, code-wise. This file exists to:

1. Mark the R7 row of RFC 0005 §7 as completed instead of leaving
   readers to chase the older `docs/notes/m3-status.md` "deferred"
   line.
2. Leave a one-shot audit recipe (the diff/grep block above) that
   anyone can re-run on any future `main` tip.

## RFC 0005 §7 status snapshot (2026-08-20)

| PR | Status |
|---|---|
| R0  | landed; squashed into R6 PR #1 (schema-provider seam) |
| R1  | landed; squashed into R6 PR #1 / PR #2 (value types + types/tests) |
| R2  | merged via `codex/r2-execution-plan-trait`; legacy `enum PhysicalPlan` / `PhysicalExpr` deleted (R2.3) |
| R3  | merged via `codex/r3-fragmenter-rule`; Aggregate cut extracted, HashJoin/Distinct/Window plug-in ready |
| R4  | re-scoped during implementation to LogicalOptimizer / RepartitionRule; merged via `codex/r4-logical-optimizer` |
| R5-pre | merged via `codex/ci-boundary-checks` (driver.rs cleanup, zero trait changes) |
| R5  | landed as part of the R8 trail (OpRegistry) |
| R6  | PR #1 + #2 squash-merged into `main`; PR #3-5 stacked awaiting base retarget |
| **R7** | **landed via `d610c8e`; this PR marks it** |
| R8  | merged via `codex/r8-trait-stability` (trait-stability doc + CI script) |
| R9  | pending — `docs/design/trait-stability.md` not yet authored |
