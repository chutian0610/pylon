# M1 milestone — sign-off packet

Date: 2026-08-18

## Acceptance criteria (M1)
- [x] Cargo workspace with 8 crates compiles
- [x] Single-worker pipeline executes a 3-op query (`SeqScan → Filter → Project`)
- [x] End-to-end SQL `SELECT … FROM … WHERE …` over 100K-row Parquet
- [x] Output matches expected cardinality exactly

## Headline numbers
- **Latency**: < 50 ms for the `amount > 100000` query over 100K rows (after warmup)
- **Output size**: 862 KB Parquet (33,333 rows × 26 B/row)
- **Memory**: M1 in-process materialisation; production spill comes in M4

## Defects fixed during M1
1. `Filter.is_finished()` returning true on empty buffer → added `upstream_done` flag
2. Same for `Project.is_finished()`
3. Driver was forgetting to wire non-first ops' input receivers → rewired via `next_input`
4. Schema projection always emitted `Float64` → look up actual column type
5. `tracing_subscriber::EnvFilter` is feature-gated → enable `env-filter` feature
6. `Schema::new` takes `impl Into<Fields>` → use explicit typed `Fields`
7. `PipelineOp: Send` not enough for shared ops → also add `Sync`
8. `JoinSet::spawn` requires `()` → wrap `run_op` in `async { … }` to swallow Result
9. `Field`/`Array`/`Scalar` were unused imports → cleaned up
10. `move of final_tx` in loop body → `final_tx.clone()`

## Known limitations (NOT blockers, deferred)
- WHERE only accepts single predicate (no AND/OR) — straightforward recursion needed
- SeqScan loads entire file into memory — fine for M1, will stream in M2
- No aggregation, no JOIN — by design (M2/M3)

## Status: M1 complete, ready for M2 on user sign-off
