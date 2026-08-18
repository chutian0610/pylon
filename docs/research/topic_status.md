# Research + Implementation Progress (M1 + RFC-0002)

## Last milestone: RFC-0002 accepted (Trino-aligned abstractions)
## M1 fully shipped and tested

# End state (this commit)
- 28 Rust source files, 2,083 LoC
- 9 workspace crates (8 architecture + 1 worker binary)
- 5 unit tests passing in pylon-coord
- M1 smoke test passing: 33,333 rows on TPC-H-style query
- 2 RFCs (0001 architecture, 0002 execution hierarchy)

# what's next
- M2: implement ExchangeSource/Sink ops, gRPC transport, real multi-worker
- See docs/roadmap/milestones.md
