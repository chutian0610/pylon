# RFC 0006 — status (carry-over note)

Date: 2026-08-20
Conclusion: RFC 0006's scope was absorbed into RFC 0004 (which is
fully implemented). No separate RFC 0006 will be authored; the
`milestones.md` M0 row is being retired as part of this PR.

## What RFC 0006 was supposed to be

`docs/roadmap/milestones.md` M0 row 21 (verbatim, before this PR):

```
| RFC 0006 Exchange 协议 spec | ⏳ |
```

The intended scope: a stable spec for the data-plane exchange
protocol — i.e. the wire-level contract for how workers transfer
`RecordBatch` between pipeline stages.

## Why RFC 0006 was never authored as a separate RFC

RFC 0004 — [`M3 Arrow Flight Shuffle Protocol`](../rfcs/0004-m3-flight-shuffle.md),
Status **Implemented (2026-08-18)** — covers the full exchange-protocol spec:

| Topic | RFC 0004 section |
|---|---|
| Flight descriptor protocol | §5 |
| Arrow IPC data format | §6 |
| OpSpec extension for exchange (proto) | §7 |
| Op implementation contract | §8 |
| Coordinator routing information | §9 |
| Failure handling (M3 simplified) | §10 |

§11 of RFC 0004 explicitly defers what it does *not* cover (FTE +
spill) to M4; §14 records the 2026-08-18 sign-off and ends with
the literal line:

> 下一步 (M4)：FTE (写 Arrow IPC stream 到 S3) + Spill + 容错。
> **RFC-0006 待写。**

So the team's running acknowledgement was that the original
milestones entry should be retired, not authored separately. The
phrase "RFC-0006 待写" in the 2026-08-18 sign-off packet is a
process note, not a roadmap commitment.

## Evidence (reproducible on current main)

```bash
# 1. No 0006 file ever existed.
ls docs/rfcs/0006-* 2>/dev/null   # → no output

# 2. RFC 0004's status header was set during its 2026-08-18 implementation.
grep -E 'Status.*Implemented' docs/rfcs/0004-m3-flight-shuffle.md
                                 # → - **Status**: Implemented (2026-08-18)

# 3. The Flight descriptor + data format commitments from RFC 0004
#    are observable on current main.
grep -nE 'ExchangeSink|ExchangeSource|FlightService|DoExchange' \
    crates/pylon-runtime/src/ops/exchange.rs
                                    # → op definitions line up with §8 contract

# 4. Coordinator Flight routing (RFC 0004 §9) is in pylon-coord.
grep -nE 'fragment_with_workers|target_flight_addrs' \
    crates/pylon-coord/src/fragment.rs
                                    # → wires FlightRPC sinks/coord routing
```

## What this PR does

1. Adds `docs/notes/rfc-0006-status.md` (this file).
2. Updates `docs/roadmap/milestones.md` M0 row 21 from `⏳`
   to `✅ 合入 RFC 0004 (见 docs/notes/rfc-0006-status.md)`.

No Rust source change.

## Why a separate RFC 0006 would be wrong

RFC 0004 covers every section a "data-plane exchange protocol spec"
would need (descriptor + format + op contract + failure + sign-off).
Authoring RFC 0006 today would either (a) duplicate RFC 0004 in
different wording, or (b) commit to a divergent, looser spec that
contradicts what's already shipping. Neither is useful.

If the team later revisits the Exchange design (e.g. for TLS —
M5, or for FTE-aware exchange — RFC 0007 §5 M4.S3-4), the right
move is to amend RFC 0004 (or write its successor RFC) at that
point — not retrospectively backfill 0006.

## Out of scope for this PR (intentional)

The M0 status table has other `⏳` rows (RFC 0002 / 0003 / 0004 /
0005) that are also outdated — most of those RFCs were created and
their scope has since been superseded by follow-on RFCs (e.g.
RFC 0002's "Crate 结构" intent is reflected in RFC 0005 §1's
"Module layout"). Cleaning those up is a separate hygiene pass.
