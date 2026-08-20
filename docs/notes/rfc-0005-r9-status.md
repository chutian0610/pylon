# RFC 0005 — R9 status (carry-over note)

Date: 2026-08-20
Conclusion: R9 is live on `main`. No code change required.

## R9 per RFC 0005 §7

> **R9** — Document the trait surface in
> `docs/design/trait-stability.md`: "stable" vs "internal" marked
> explicitly; SPI breaking-change policy. Low. Depends on R0–R6
> done.

## Why R9 is already in `main`

R8 (`feat(rfc-0005): R8 — trait-stability doc + companion CI
script`, commit `cd652e2bb70dca009fd44834ed164bf6e5d8d52f`) shipped
both deliverables RFC 0005 §7 attributes to R9:

- `docs/design/trait-stability.md` — 477 lines.
- `tools/check-trait-stability.sh` — 127 lines of CI enforcement.

R8's commit message attaches a status banner on the doc header
(`Status: Active (RFC 0005 §7 R9; user's table calls this R8)`);
R9 was absorbed into R8 because they share a single deliverable
file.

## Evidence

Anchor: `cd652e2`. `main` tip at time of writing: `4e6fd02`.

```bash
# R8 commit is a linear ancestor of main, with no further edits.
git merge-base cd652e2 main          # → cd652e2
git log cd652e2 ^main                # → empty

# R8's two files are byte-identical on main.
git diff cd652e2 main -- docs/design/trait-stability.md \
                            tools/check-trait-stability.sh       # → 0 lines

# The two companion CI scripts still pass on current main.
bash tools/check-spi-boundaries.sh   # → OK
bash tools/check-trait-stability.sh  # → OK

# Workspace builds clean (only pre-existing warnings).
cargo check --workspace --all-targets
```

If R9 ever drifts out — e.g. someone reintroduces a forbidden
connector dep — the diff vs. `cd652e2` will surface it and the
`check-*` scripts will fail at PR time.

## Coverage of the R9 spec

| R9 spec item | Doc section |
|---|---|
| "stable" vs "internal" categories | §1 |
| Per-crate classification | §2 |
| Per-type inventory with explicit markers | §3 |
| Code-level markers (`#[non_exhaustive]`, `#[deprecated]`, etc.) | §4 |
| SPI breaking-change policy (SemVer at 1.0+) | §5 |
| Migration procedure (deprecate → CHANGELOG → major bump) | §6 |
| Wire-format stability (proto / Arrow IPC) | §7 |
| CI enforcement (companion scripts) | §8 |
| Graduation procedure (internal → SPI) | §9 |
| Open questions | §10 |
| References | §11 |

Every R9-spec item lives in `main` verbatim. Coverage: **100%**.

## What this PR does

None, code-wise. Same shape as PR #6:

1. Mark the R9 row of RFC 0005 §7 as completed (instead of leaving
   readers to walk the doc's status banner for the link).
2. Leave a one-shot audit recipe that re-runs the merge-base check,
   the diff, and both CI scripts on any future `main` tip.

## RFC 0005 §7 status snapshot (2026-08-20, post-R9)

| PR | Status |
|---|---|
| R0 | landed (squashed into R6 PR #1) |
| R1 | landed (squashed into R6 PR #1 / #2) |
| R2 | merged via `codex/r2-execution-plan-trait` |
| R3 | merged via `codex/r3-fragmenter-rule` |
| R4 | re-scoped during implementation → merged via `codex/r4-logical-optimizer` |
| R5-pre | merged via `codex/ci-boundary-checks` |
| R5 | landed as part of R8 trail (OpRegistry) |
| R6 | PR #1 + #2 squash-merged into `main`; PR #3-5 stacked awaiting base retarget |
| R7 | landed via `d610c8e`; PR #6 marked it |
| R8 | merged via `codex/r8-trait-stability` (also shipped R9's deliverable) |
| R9 | landed via `cd652e2`; **this PR marks it** |

RFC 0005 §7 is now **fully closed at the doc level**. The next
structural workstream is M4 (FTE + spill) per
`docs/roadmap/milestones.md`.
