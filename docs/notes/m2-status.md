# M2 milestone — sign-off packet (reconstruction)

> **Note**: written 2026-09-16 from repository evidence. The M2
> implementation predates the visible commit history (the repo was
> re-initialized around the RFC 0005 drafting period), so this packet
> is a **reconstruction** from `docs/roadmap/milestones.md` (M2 DoD,
> fact-checked by the `75f4ca2` hygiene pass), the surviving code, and
> cross-references in `m1-status.md` / `m3-status.md`.

Date of original work: pre-RFC-0005 (reconstructed 2026-09-16)

## Goal

Multi-process coordinator/worker: a two-stage query
(partitioned exchange) split across separate coord and worker
processes.

## DoD results (fact-checked)

| Item | Status | Evidence |
|---|---|---|
| `pylon-coord` binary: HTTP API → parse → plan → fragment → schedule | ✅ | axum `Router` + `POST /v1/query`; `plan_and_dispatch` in `crates/pylon-coord/src/bin/pylon-coord.rs` |
| `pylon-worker` binary: accept tasks, drive pipeline | ✅ | tonic gRPC `OpenSession` bidi stream; `Driver` loop in `crates/pylon-runtime/src/driver.rs`. (The DoD's literal "heartbeat to coord" was superseded by design: M3 chose one-shot `RegisterWorker` + a persistent `OpenSession` stream instead of periodic heartbeats — a semantic replacement, not a defect) |
| `pylon-exchange`: Arrow Flight server + client | ✅ | `FlightServerImpl` (server) + `ExchangeSinkRpc` tonic `DoExchange` (client); verified in the M3 B-2 sign-off |
| Partitioned `HashPartitionExchange` (N→N) | ✅ | `Fragmenter` cuts at `Aggregate`, injects `ExchangeSink` + per-partition `[ExchangeSource, Aggregate]` task pairs; verified end-to-end |
| Broadcast exchange (1→N) | ❌ deferred | deferred with HashJoin/Window to later milestones (see m3-status) |
| Two-stage `JOIN` on 1 coord + 2 workers | ❌ deferred | requires `HashJoin` (M4+ fragmenter rules) |
| TPC-H Q1/Q3 at SF10 | ❌ deferred | requires catalog/Iceberg infra (M3.5+) |

## Status: M2 substantially complete

Delivered: the coord/worker split, the fragmenter + scheduler, and
the partitioned exchange path that M3's cross-worker shuffle later
unified onto Arrow Flight. Deferred: broadcast, join, TPC-H
(recorded in the M3 out-of-scope list).
