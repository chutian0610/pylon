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
| (skip) | M4.S6 | Spillable `SortOp` (new op) | Sort doesn't exist in M3 cut. Lower priority — wait for a real Sort use case. | ~6 dev days | C3 |
| (skip) | M4.S7 | Chaos testbed | Worker kill mid-query, retry path exercised. | ~5 dev days | C5 |
| (skip) | M4.S8 | 1B-row mid-flight-worker-kill E2E | The headline M4 sign-off test. | ~3 dev days | C5 + S7 |

## Adjacent cleanup work

| ID | What | Why | Est |
|---|---|---|---|
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
