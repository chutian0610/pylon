@RTK.md

# Pylon — working conventions for AI agents

## Project map

- `README.md` — visitor-facing front door: pitch, features, quickstart,
  tooling. **Timeless**: no milestone narration, no dev-history links.
- `docs/architecture.md` — query flow, crate reference, fault-tolerance
  design, key ADRs.
- `docs/operations.md` — binaries, env vars, sample-data generator,
  chaos/FTE script usage.
- `docs/rfcs/` — design decisions (0001–0007). Status blocks point at
  sign-off packets once implemented.
- `docs/design/trait-stability.md` — SPI vs internal crate boundaries
  (enforced by `tools/check-trait-stability.sh`).
- `docs/roadmap/` — forward-looking: `milestones.md` (per-milestone
  status) is the source of truth for what is done / in flight.
- `docs/notes/` — **append-only** development record: milestone
  sign-off packets (m1, m2, m3, m4), RFC carry-over audits (r7, r9,
  0006), the M4 candidates ledger.

## Documentation duties (mandatory with code changes)

1. **Process records** (`docs/notes/`): completing a milestone or
   phase → write its sign-off packet in `docs/notes/<mN>-status.md`
   (acceptance criteria, per-phase deliveries, headline numbers,
   deviations). Ledger-style files (`*-candidates.md`) are updated at
   milestone boundaries. The directory is append-only: never rewrite or
   delete past entries — corrections go in a new entry or a clearly
   marked addendum.
2. **Architecture sync**: any change that touches public crate
   surfaces, operators, env vars, CLI flags, RPC/proto, or crate
   boundaries must be reflected in `docs/architecture.md` and
   `docs/operations.md` in the same PR. Visitor-visible capabilities
   belong in `README.md`; implementation detail belongs under `docs/`.
3. **RFC status sync**: landing an RFC workstream → update that RFC's
   status block (Draft → Implemented, with a pointer to its evidence)
   in the same PR.
4. **Milestones**: tick DoD items only with evidence (file + line or
   script); unresolved items stay honestly unchecked with a deferral
   note.

## Engineering gates (run before every push)

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Use a Rust toolchain ≥ 1.98 (CI parity; older toolchains miss newer
clippy lints). `data/*.parquet` is generated — never commit it.

## Guardrails

- Never `git push --force`, `git reset --hard`, or rewrite pushed
  history.
- Never delete or rewrite `docs/notes/*` entries (append-only record).
- Conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, …).
- Network calls failing → retry via
  `export https_proxy=http://127.0.0.1:7891 http_proxy=http://127.0.0.1:7891 all_proxy=socks5://127.0.0.1:7891`.
