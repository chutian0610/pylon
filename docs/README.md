# Documentation map

What lives where, and who should read it.

| Path | Audience | Content |
|---|---|---|
| [`README.md`](../README.md) | everyone | Project intro, features, quickstart, tooling, workspace layout |
| [`query-lifecycle.md`](query-lifecycle.md) | **new contributors (start here)** | One query's end-to-end journey with file pointers: submit → plan → fragment → dispatch → two stages → collect |
| [`architecture.md`](architecture.md) | contributors | Components, fault-tolerance design, key ADRs |
| [`operations.md`](operations.md) | operators / testers | Binaries, environment variables, sample-data generator, chaos/FTE scripts |
| [`rfcs/`](rfcs/) | contributors | Design decisions: architecture (0001), execution hierarchy (0002), control/data plane (0003), Flight shuffle (0004), pipeline trait surface (0005), M4 FTE + spill (0007) |
| [`design/trait-stability.md`](design/trait-stability.md) | contributors | SPI vs internal crate boundaries, stability enforcement |
| [`roadmap/milestones.md`](roadmap/milestones.md) | everyone | Forward-looking roadmap with per-milestone status |
| [`notes/`](notes/) | historians / reviewers | Append-only development records: milestone sign-off packets (m1, m3, m4), RFC carry-over audits (r7, r9, 0006), the M4 candidates ledger |
| [`research/findings.md`](research/findings.md) | contributors | The research behind each architectural decision |

## New-contributor reading path

1. [`README.md`](../README.md) — what Pylon is
2. [`query-lifecycle.md`](query-lifecycle.md) — follow one query through the system
3. [`architecture.md`](architecture.md) — components + design
4. [`operations.md`](operations.md) — run it, break it, verify it
5. then dive into the code next to whichever lifecycle step interests you

## Conventions

- **`README.md`** is timeless and visitor-facing: what Pylon is, how to
  try it. Development progress, milestone tags, and tuning details do
  not belong here.
- **`roadmap/`** is forward-looking: what is planned/in flight.
- **`notes/`** is the append-only audit trail of what happened
  (sign-off packets, carry-over verifications, candidate ledgers).
  Entries are written once at a milestone boundary and then left as
  historical record — edits only to correct factual errors.
- Design decisions start as an RFC in **`rfcs/`**; once implemented,
  the RFC's status block points at its sign-off packet in `notes/`.
