# M3 Tail — #2: Same-worker Exchange goes through real Flight RPC

> Status: PR1 + PR2 shipped. `target_flight_addrs` is dispatcher-owned;
> `ExchangeSinkOp` is gone; the fragmenter emits `ExchangeSinkRpc` only.
> Scope: collapse the in-process `ExchangeSinkOp` short-circuit so every
> shuffle (loopback or remote) uses Arrow Flight `DoExchange`.
> Goal: one code path, realistic semantics, easier reasoning.

## 0. Why this needs to land

Two producer shapes exist today, both feeding the same
`PylonFlightService` queue on the receiver side:

| Path        | Producer op        | Wire                 | Receiver                |
|-------------|--------------------|----------------------|-------------------------|
| Same-worker | `ExchangeSinkOp`   | in-process `push()`  | same `PylonFlightService` |
| Cross-worker| `ExchangeSinkRpc`  | gRPC `DoExchange`    | same `PylonFlightService` (via `flight_rpc.rs`) |

Source-of-truth decisions live in `fragment.rs::visit_plan`:
the `worker_flight_addrs.is_empty()` branch decides between the two
emissions. That branch plus the round-robin `p % n_workers` target
firing **even when source-worker == target-worker** means the
fragmenter can't express "same worker, just loopback".

Receiver code (`flight_rpc.rs`, `PylonFlightService`) already abstracts
IPC, so the unification cost is concentrated on the producer side and
in dispatch.

## 1. Design choice — adopt Option I

**Always go through Arrow Flight, even loopback.** Delete `ExchangeSinkOp`.

Rejected: Option II (`SinkTransport` trait with two impls) — keeps two
code paths, doesn't simplify dispatch reasoning, and the user framing
("拉齐") rules it out.

Consequence: same-worker exchanges pay IPC encode/decode. That
overhead exists today on the cross-worker path and tests tolerate it
already. We add an A/B micro in §4 to confirm the loopback regression
is bounded.

## 2. Concrete subtasks — *shipped*

PR1 (B3) + PR2 (B1+B2+B8) landed. The remaining M3-tail items (#1
result streaming, #3 fragmenter rules, #4 adaptive batches) are
unchanged; see `docs/rfcs/0004-m3-flight-shuffle.md` § 15 for the
post-sign-off cleanup narrative.


### B1. Delete `ExchangeSinkOp`
- File: `crates/pylon-runtime/src/ops/exchange.rs`
- Remove `ExchangeSinkOp`, its tests, and the `new_partitioned`/`new`
  constructors.
- File: `crates/pylon-runtime/src/ops/mod.rs` — drop re-export.
- File: `crates/pylon-worker/src/main.rs` — replace the `"ExchangeSink"`
  factory branch with a `"ExchangeSinkRpc"` branch that always emits
  `target_flight_addrs[i] = bound_flight_addr.clone()` (loopback).

### B2. Fragmenter single-mode emission
- File: `crates/pylon-coord/src/fragment.rs`
- Make `fragment_with_workers` the only public entry point; make
  `worker_flight_addrs` required (return `InvalidPlan` when empty —
  i.e. when no workers registered, reject query submission rather
  than silently falling through to a code path we're deleting).
- Delete `fragment_multi_stage` and the in-process branch in
  `visit_plan`. The `(op_name, sink_config) = if ctx...is_empty()` if
  collapses to a single arm emitting `ExchangeSinkRpc`.
- Update the comment block that flags "M3 first cut" / "B-2 routing".

### B3. Dispatch source-worker index  ← **linchpin**
- Today's `ExchangeSinkRpc.target_flight_addrs` is set by the
  fragmenter with `ctx.worker_flight_addrs[p % n_workers]`. Once we
  require loopback for same-worker, the fragmenter no longer has
  enough info: it doesn't know **which** worker runs stage0 task that
  produces row p.
- Move target-flight-addr computation to the coord's dispatch step
  (the place that pins a task to a `WorkerHandle`):
  1. Stage0 task is pinned to worker W_s.
  2. Stage1 partition p is pinned to worker W_t(p) (the
     `Discovery` snapshot already knows registered flight_addrs).
  3. Coord rewrites the OpSpec config in the dispatched `TaskRequest`
     so `target_flight_addrs[p] = if W_s == W_t(p) { W_s.flight_addr }
     else { W_t(p).flight_addr }`.
- File: `crates/pylon-coord/src/bin/pylon-coord.rs` (`plan_and_dispatch`),
  and `crates/pylon-coord/src/scheduler.rs` if assignment lives there.
- Verify `pylon_coord::discovery` already exposes `flight_addr(worker_id)`
  — needed for both arms of the `if`.

### B4. Verify `PylonFlightService` invariants still hold
- `pending()`, `pop()`, `push()` semantics don't change, but the
  receiver now sees only IPC-decoded batches. Spot-check:
  - empty-batch handling (`num_rows() == 0` skipped — symmetrical on
    both paths already).
  - ordering guarantee: `StreamReader` preserves insertion order on a
    single `DoExchange` stream; matches the `Vec<RecordBatch>` model.
  - EOS / "stream closed" signal: the B-1 `make_ack` placeholder is
    still the only signal that the producer finished. Acceptable for
    M3 tail; tightening is item #1's problem.

### B5. Tests
- `crates/pylon-runtime/tests/exchange_test.rs` and
  `exchange_partition_test.rs` — most of these touch `ExchangeSinkOp`
  directly. Migrate to `ExchangeSinkRpc` against a local
  `PylonFlightService` bound on `127.0.0.1:0`.
- `crates/pylon-runtime/tests/aggregate_2stage_e2e_test.rs` — the
  existing 2-stage path almost certainly exercises the in-process
  branch; restate it to require a Flight port.
- Add `crates/pylon-runtime/tests/exchange_loopback_test.rs`:
  - spin up `FlightServerImpl` on `127.0.0.1:0`,
  - drive one `ExchangeSinkRpc` against it (endpoint = local),
  - assert one `ExchangeSourceOp` reads back what was sent,
  - compare row/sum against the previous in-process expectation.
- Add an e2e that runs 2 workers in the same process, runs a 2-stage
  aggregate, asserts results are bit-identical whether all partitions
  end up on worker A (loopback), worker B (remote), or split.

### B6. Observability
- Add a `tracing::span` field on `ExchangeSinkRpc::do_exchange`:
  `exchange_path = "loopback" | "remote"` derived from `local
  flight_addr == target.flight_addr`.
- Counter `pylon_exchange_send_total{path=…}` in
  `flight_rpc.rs`/`exchange.rs` once metrics are wired (low priority).

### B7. Risk register
- **R1 — perf regression on same-worker**: IPC encode/decode vs. an
  in-process `push`. Bound expected ≤ ~5%; confirm with `B5` A/B.
  Mitigation: short-term, no override. Long-term, allow a
  `disable_loopback_flight` debug flag if the regression turns out
  larger.
- **R2 — placeholder URL cleanup**: `PylonFlightClient::connect
  ("in-process://worker")` in `pylon-worker::encode_batch_ipc` is
  shim — once loopback is real, replace with the actual bound
  flight_addr so a stack trace shows a real endpoint.
- **R3 — `flight_rpc.rs` first-message handling**:
  `if data.data_body.is_empty() && data.app_metadata.is_empty() { continue }`
  silently drops frames. With a single producer shape it's easier to
  reason about, but document the contract in the file header.
- **R4 — coord rejects query when no workers registered**: now that
  fragmenter errors on `worker_flight_addrs.is_empty()`, the existing
  in-process smoke path (single-process query) breaks. Tests in
  `crates/pylon-runtime` are fine (they bypass the coord), but
  any local end-to-end runner needs at least one `RegisterWorker`
  before submit.

### B8. Cleanup
- Remove the `// M3 first cut` / `// B-2 routing` markers in
  `fragment.rs` once they no longer describe reality.
- Update `docs/rfcs/0004-m3-flight-shuffle.md` with a closing
  paragraph: "M3 tail unified producer path; M3 B-1/B-2 split is
  internal detail only".
- Update `crates/pylon-coord/src/query.rs` if it cites the
  `fragment_multi_stage` entrypoint.

## 3. Sequencing

Order matters because B3 is the gate:

1. **PR1**: B3 (dispatch-time flight_addr rewrite). Lands first; the
   fragmenter still has both branches but the new path is reachable.
2. **PR2**: B1 + B2 + B8 (delete the in-process branch + clean docs).
3. **PR3**: B5 (tests), B6 (metrics), B7 follow-ups.

B4 is folded into PR2 review; B6/B7 can ship independently.

## 4. Verification

- CI: full suite; the new `exchange_loopback_test` must pass.
- Local micro (manual, not blocking): `cargo bench --bench
  exchange_overhead` if added — measure loopback vs pre-PR in-process
  for a 1M-row partition.
- E2E (manual, blocking for sign-off): 2-worker mode on a synthetic
  aggregated query, results identical to single-worker baseline.

## 5. Out of scope (recorded for next pass)

- Adapting `#3 Fragmenter` rules (HashJoin / Distinct / Window) so
  they too emit `ExchangeSinkRpc`. Trivial once B1+B2 land, but
  folded into that RFC.
- Replacing the polling `sleep(3)` ack protocol in
  `pylon-coord/src/bin/pylon-coord.rs` (`#1` — result streaming).
- Adaptive batch sizing / columnar shuffle (`#4`).
