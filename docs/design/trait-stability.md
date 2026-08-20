# Trait Stability — SPI vs Internal

- **Status**: Active (RFC 0005 §7 R9; user's table calls this R8)
- **Owner**: Pylon working group
- **Applies to**: every `pub` type reachable from another crate
- **Last updated**: 2026-08-19

## 0. Purpose

`pylon` is a Cargo workspace with eleven crates. Some of those crates
are consumed by connector authors — third parties who integrate the
engine with their storage, their catalog, their file format. Other
crates are pure engine internals; we own every line and may refactor
them in any release.

This document locks down **which** types are which, **how** each is
marked so the compiler and `cargo doc` agree, and **what** the
breaking-change policy is for each tier. It complements RFC 0005 §1
(module layout) and §3 (domain boundaries): the RFC says *where*
each trait lives; this doc says *whether* it can change without
warning.

If you are writing a connector (`pylon-catalog` / `pylon-storage` /
`pylon-iceberg` / an out-of-tree one), this is your compatibility
contract. If you are modifying the engine, this is your change-tax
table.

## 1. The two categories

### 1.1 SPI — Stable Plugin Interface

Types whose shape is **promised** to external integrators.

| Property | Requirement |
|---|---|
| Versioning | Independent crate version (`pylon-connector-spi` carries `SPI_VERSION`). |
| Breaking changes | Only on major-version bumps, and only after a deprecation cycle (§ 6). |
| Adding methods | Allowed any time **iff** the method has a default impl. |
| Adding variants | Allowed any time **iff** the enum is `#[non_exhaustive]`. |
| Adding fields | Allowed on `#[non_exhaustive]` structs with builder methods, **not** direct field access. |
| Removing items | Requires one minor-release deprecation cycle first. |
| Trait bounds | May **only loosen** (`Send` → `Send + Sync`), never tighten. |
| Deprecation | `#[deprecated(since = "X.Y", note = "use Foo instead; see ...")]`. |
| CHANGELOG | Every SPI change (additive or breaking) gets a CHANGELOG entry. |

### 1.2 Internal — Engine-internal

Types whose shape is **not promised**. We own every consumer; we
may rename, restructure, or delete them between any two commits.

| Property | Requirement |
|---|---|
| Versioning | Workspace version (all crates ship the same `0.x.y`). |
| Breaking changes | Allowed at any time. **No deprecation cycle required.** |
| Additions / removals | Free. |
| Trait bounds | Free. |
| CHANGELOG | Optional; usually documented only at the RFC level. |

The "Internal" tier is *not* "private" — internal types are still
`pub`, because Rust visibility forces us to expose them to other
crates in the workspace. The tier means: **no external consumer
should rely on this type's shape**.

### 1.3 The asymmetry

Connectors are *upstream consumers* of the engine. The engine is
*not* a consumer of connector code (only of the connector's
returned values). When the engine changes an internal type, the
blast radius is the workspace. When an SPI type changes, the blast
radius is *every connector that has ever been published* — which
we cannot recompile.

That asymmetry is the entire reason for the two tiers.

## 2. Crate classification

Each crate in the workspace has exactly one classification. The
boundary is enforced in CI by [`tools/check-spi-boundaries.sh`](../../tools/check-spi-boundaries.sh)
(RFC 0005 §3 rule #1) and the companion
[`tools/check-trait-stability.sh`](../../tools/check-trait-stability.sh)
shipped with this doc.

| Crate | Tier | Depends on | Allowed consumers |
|---|---|---|---|
| `pylon-types` | **SPI** | arrow-* | workspace + connectors |
| `pylon-connector-spi` | **SPI** | `pylon-types`, arrow-* | connectors only |
| `pylon-catalog` | Connector | `pylon-types`, `pylon-connector-spi` | workspace |
| `pylon-storage` | Connector | `pylon-types`, `pylon-connector-spi` | workspace |
| `pylon-iceberg` | Connector | `pylon-types`, `pylon-connector-spi` | workspace |
| `pylon-plan` | **Internal** | `pylon-types`, `pylon-connector-spi` | workspace |
| `pylon-runtime` | **Internal** | `pylon-types` | workspace |
| `pylon-exchange` | **Internal** | `pylon-types`, `pylon-proto` | workspace |
| `pylon-coord` | **Internal** | engine crates | workspace |
| `pylon-worker` | **Internal** | engine crates | workspace (binary only) |
| `pylon-proto` | **Internal** | prost + transport deps | workspace |

**Rule**: SPI crates depend only on `pylon-types` + Arrow. Internal
crates may depend on any crate above them in the module layout
(RFC 0005 §1). A connector depends only on SPI crates. **An
internal crate must never import a connector crate.**

`tools/check-spi-boundaries.sh` enforces the SPI rule. The companion
script (this PR) checks that connector crates import nothing from
internal crates and that no internal crate re-exports from a
connector crate.

## 3. Per-type inventory (current state)

This is the **as-of** classification. Every change moves an entry
into § 6's migration procedure.

### 3.1 SPI — `pylon-types` (value types)

| Type | Marker | Notes |
|---|---|---|
| `RecordBatch` | re-export from `arrow_array` | Leaf value type; bound to Arrow's release cadence. |
| `Schema`, `SchemaRef` | re-export from `arrow_schema` | Same as above. |
| `DataType`, `Field` | re-export from `arrow_schema` | Same as above. |
| `PylonError` | `#[non_exhaustive]` (planned) | The single engine-wide error crossing the SPI boundary. Variants added in §6. |

Re-exports from Arrow are not really "ours" — they live at Arrow's
release cadence. We document them as SPI because connectors bind to
them through our crate, not directly to `arrow-array`.

### 3.2 SPI — `pylon-connector-spi` (planned for R1)

Per RFC 0005 §4, the connector SPI will host:

| Type | Trait? | Marker |
|---|---|---|
| `Connector` | trait | object-safe, `Send + Sync` |
| `ConnectorFactory` | trait (async) | object-safe, `Send + Sync` |
| `DataSource` | trait | `Send` (driver-thread-owned) |
| `DataSink` | trait | `Send` |
| `ConnectorError` | struct | `#[non_exhaustive]` + accessor methods |
| `ConnectorErrorCode` | enum | `#[non_exhaustive]` |
| `ConnectorPage` | struct | `#[non_exhaustive]`, column accessors only |
| `ConnectorColumns` | trait | object-safe |
| `DataSourceContext`, `DataSinkContext` | structs | builder + accessors, no `pub fields |

Until R1 lands, this crate is empty (R0 axiom; see
`pylon-connector-spi/src/lib.rs`).

### 3.3 Internal — `pylon-plan` (engine)

| Type | Marker | Notes |
|---|---|---|
| `LogicalPlan` | enum | Free to add variants between minor versions. |
| `Expr` | enum | Same. |
| `ExecutionPlan` | trait | Adding methods requires a default impl. May add new methods freely otherwise. |
| `PhysicalExpr` | trait | Same as `ExecutionPlan`. |
| `AggregateExec`, `FilterExec`, `ProjectExec`, `SeqScanExec` | structs | Fields are `pub` for the engine's own `with_new_children` machinery. **Connectors must not downcast to these.** |
| `Distribution`, `RequiredDistribution`, `Boundedness`, `EmissionType` | enums | Engine-internal vocabulary. |
| `PlanProperties` | struct | Free to evolve. |
| `FragmenterRule` | trait (R3) | `pub` because the engine registers rules; not a plugin surface. |
| `BoundaryStrategy`, `BoundaryEmit` | enum / struct (R3) | Engine-internal vocabulary; not connector-facing. |
| `AggregateFragmenterRule` | struct (R3) | First built-in rule. Future M4 rules are also engine-internal. |
| `CatalogStub` | struct (`translate.rs`) | marked `#[doc(hidden)]` (R6.5 follow-up): dev-only test fixture; the engine-internal `Catalog` trait extraction is deferred and intentionally out of scope until M5. |

### 3.4 Internal — `pylon-runtime`, `pylon-coord`, `pylon-worker`, `pylon-exchange`, `pylon-proto`

Every `pub` type in these crates is internal. Notable:

| Type | Crate | Marker | Notes |
|---|---|---|---|
| `PipelineOp` | pylon-runtime | trait | Stable within the workspace; tightening `Send + Sync` is forbidden (see §5.4). |
| `Driver`, `Pipeline` | pylon-runtime | struct | R5-pre cleaned the legacy `DriverMode` enum; the remaining `Driver` struct is the only one. |
| `RuntimeError` | pylon-runtime | `#[non_exhaustive]` (planned) | Internal but using the marker for cheap extensibility. |
| `Fragmenter`, `FragmenterConfig` | pylon-coord | struct | R3 added `with_rule()` + `with_rules()` builder; these are stable for the workspace's own use. |
| `Stage`, `StageDag`, `StageId`, `OpSpec`, `Fragment`, `Distribution` | pylon-coord | struct / enum | R2.2.b plumbed `Arc<dyn ExecutionPlan>` through `Stage`; free to extend. |
| `TaskSpec`, `TaskId`, `Partition`, `ExchangeKind`, `ExchangeSpec` | pylon-coord | struct / enum | Internal — change between any two releases. |
| `Scheduler`, `CapacityScheduler`, `WorkerAddr`, `WorkerCapacity`, `WorkerId` | pylon-coord | trait / struct | Internal — change between any two releases. |
| `QueryStateMachine`, `StageState`, `TaskAck` | pylon-coord | struct / enum | R7 added; free to extend. |
| `FragmenterRule` (re-imported) | pylon-coord | trait | The trait lives in `pylon-plan`; the coord references it but does not own it. |
| All `pylon-proto` types | pylon-proto | protobuf-generated | Wire-format compat is a separate policy (see § 7). |

## 4. Code-level markers

### 4.1 `#[non_exhaustive]`

Applied to every **public enum** in an SPI crate. Forces every
external match to include a wildcard arm — so we can add a variant
without breaking the connector.

**When to apply**:
- ✅ Every `pub enum` in `pylon-types` and `pylon-connector-spi`.
- ⚠️ Internal enums: optional but recommended for cross-cutting
  error / event types that downstream observability might switch on
  (`PylonError`, `RuntimeError`).

**When NOT to apply**:
- ❌ Sealed-by-design enums (e.g. `BoundaryStrategy` — we own every
  variant; the marker adds noise without buying anything).
- ❌ Enums whose variants are part of a wire format (see § 7).

### 4.2 `#[deprecated(since = "...", note = "...")]`

Applied to any public item that is being phased out. The `note`
field must link to the replacement and (where relevant) the
migration guide entry.

```rust
#[deprecated(
    since = "0.4.0",
    note = "use Foo::new() instead; see docs/design/trait-stability.md § 6"
)]
pub fn bar() {}
```

**Rule**: deprecated items stay for **at least one full minor
release** before they can be removed. For SPI types, they stay for
**at least one major-release cycle** of the SPI crate.

### 4.3 Module-level `//!` docs

Every public module that exports SPI types must start with:

```rust
//! This module is part of the **SPI** (see
//! `docs/design/trait-stability.md`).
//!
//! Items here follow the breaking-change policy in § 5:
//! additions of new methods / variants are allowed when
//! backward-compatible; removals require a major-version bump.
```

Internal modules do not need the banner; readers can tell from the
crate (§ 2).

### 4.4 `pub` vs `pub(crate)`

- **SPI**: `pub` only.
- **Internal**: prefer `pub(crate)` for items that don't need to
  cross crate boundaries. Reduce the `pub` surface even within the
  workspace; nothing forces every field to be reachable from a test.

### 4.5 `#[doc(hidden)]`

Apply to items that need `pub` for technical reasons (e.g. blanket
impls, derive macros) but should not appear in `cargo doc`. Used
sparingly.

## 5. Versioning & breaking-change policy

### 5.1 SemVer for the workspace

The workspace follows Cargo's standard SemVer rules, with two
clarifications:

1. **Pre-1.0 (current state, `0.x.y`)**: every crate is allowed
   to break compatibility at minor bumps. SPI types in
   `pylon-types` and `pylon-connector-spi` follow this rule today
   because nothing is *1.0*. The moment we cut `1.0.0` for either
   SPI crate, the policy in § 5.2 kicks in.

2. **Crate-level independence**: `pylon-connector-spi` has its own
   version line. A breaking change there bumps the SPI crate's
   major version, not the engine's. This is the Presto pattern
   (`presto-spi` vs `presto-main`).

### 5.2 SPI breakage at 1.0+

Once an SPI crate reaches `1.0.0`:

- **Major version bump (X+1.0.0)** required for **any** breaking
  change to a public item. There is no "internal SPI" tier — once
  it's `pub`, it's stable.
- **Minor version bump (X.Y+1.0)** allowed for **additive**
  changes:
  - Adding a method to a trait **with a default impl**.
  - Adding a variant to a `#[non_exhaustive]` enum.
  - Adding a `pub fn` to a module (not a trait).
  - Adding a field to a struct behind a builder.
- **Patch version bump (X.Y.Z+1)** for bug fixes and doc updates.

### 5.3 Internal crate breakage — any time

Internal crates may break at any time. The workspace pins them all
to the same `0.x.y` line so we can refactor freely. The only
constraint is:

> A connector author who depends on an internal crate **does so at
> their own risk**. The workspace provides no compatibility
> promise.

### 5.4 Trait bound asymmetry (R5-pre corollary)

A trait bound change is breaking in the strict SemVer sense. Two
practical exceptions the workspace adopts:

- `Send + Sync` → `Send`: **breaking on the surface, harmless in
  practice** when the type was never used across threads in
  practice. RFC 0005 §7.1 step D8 already plans this relaxation for
  `PipelineOp`; we treat it as a non-event because no real code
  breaks.
- Adding `?Sized` or `Send` to a generic bound: breaking on paper,
  additive in practice. Apply judgment; document in CHANGELOG.

### 5.5 `SPI_VERSION` constant

`pylon-connector-spi/src/lib.rs` exposes:

```rust
/// The current SPI version. Connectors can `matches!(pylon_connector_spi::SPI_VERSION, ...)`
/// to feature-detect at compile time.
pub const SPI_VERSION: SemVer = SemVer { major: 0, minor: 4, patch: 0 };
```

A breaking change to the SPI bumps `SPI_VERSION.major` and forces
every connector to re-declare its dependency (`pylon-connector-spi = "1.0"`).
The constant is `pub` so connectors can do exhaustive version
matching at compile time.

## 6. Migration procedure

When an SPI item must be replaced or removed:

### Step 1 — Deprecate

In the **next** minor release, add `#[deprecated(since = "...", note = "...")]`.
The `note` field must:

- Name the replacement.
- Link to the CHANGELOG entry that introduces the replacement.
- For complex migrations, link to a `docs/migrations/<id>.md` file.

The deprecated item still works. New code uses the replacement.

### Step 2 — CHANGELOG

Each crate ships a `CHANGELOG.md` at its root. Every SPI change —
additive or breaking — gets an entry under the version heading:

```markdown
## 0.4.0

### Added
- `Connector::create_data_source` now accepts a `limit: Option<usize>` argument.
  Default-implemented for backward compat; existing impls continue to compile.

### Deprecated
- `Connector::create_page_source` — use `create_data_source` with `limit: None`.
  Removal planned for 1.0.0.

### Breaking (planned)
- `1.0.0` will remove `Connector::create_page_source`. See migration guide.
```

### Step 3 — Major bump

When 1.0.0 (or 0.(N+1).0) ships, the deprecated item is deleted.
The CHANGELOG entry moves from "Deprecated" to "Removed". A
migration guide file ships with the release.

### Step 4 — Migration window

SPI types get **at least one full minor cycle** of deprecation
before removal. For 0.x → (x+1).0: the deprecation must appear in
the last `0.x.0` release at the latest, then removal happens at
`0.(x+1).0`.

For 1.x → 2.0 (post-1.0 SPI): the deprecation appears in `1.y.0`
and removal happens at `2.0.0`. **One full minor cycle** is the
minimum; longer is better when downstream impact is unclear.

## 7. Wire-format stability (proto / Arrow IPC)

Protobuf messages in `pylon-proto/proto/pylon.proto` and Arrow IPC
streams used by `ExchangeSinkRpc` / `ExchangeSource` follow a
**separate** stability contract:

- **Field addition** to a protobuf message: backwards-compatible
  (proto3 default). Bump the proto's minor.
- **Field removal / renumbering**: breaking. Bump the proto's major.
- **Arrow IPC**: byte-format compat is Arrow's responsibility.
  We ship whatever `arrow-ipc` ships; no local versioning needed.

The wire-format and SPI contracts are **independent**. Bumping
`pylon-proto`'s version does not require bumping
`pylon-connector-spi`'s version unless the SPI exposes a wrapper
that changed shape.

## 8. CI enforcement

Two scripts in `tools/` enforce this policy. Both must exit 0 on
CI before merge.

### 8.1 `tools/check-spi-boundaries.sh` (existing, RFC 0005 §3 rule #1)

- Verifies `pylon-connector-spi` does not depend on any engine
  crate in `Cargo.toml`.
- Verifies no `use pylon_<engine>::` import in `pylon-connector-spi/src/**`.

### 8.2 `tools/check-trait-stability.sh` (new in this PR)

Adds three more rules:

1. **No internal-crate dep in connector crates**: `pylon-catalog` /
   `pylon-storage` / `pylon-iceberg` cannot list `pylon-plan`,
   `pylon-runtime`, `pylon-coord`, `pylon-worker`, or
   `pylon-exchange` in `Cargo.toml`.
2. **No engine re-export from connector SPI**: the
   `pylon-connector-spi::lib.rs` re-export list must not include
   items from `pylon-plan` / `pylon-runtime` / `pylon-coord`. (The
   SPI stays minimal: `ConnectorError` lives in `pylon-types` if it
   needs to be visible there, not in `pylon-connector-spi`.)
3. **`SPI_VERSION` declared**: `pylon-connector-spi/src/lib.rs`
   contains `pub const SPI_VERSION`. (Pre-R1: this rule is *warn*
   not *fail*; from R1 onward it is *fail*.)

Run both locally before pushing:

```bash
bash tools/check-spi-boundaries.sh
bash tools/check-trait-stability.sh
```

Wire into CI as required steps in `.github/workflows/ci.yml`.

## 9. Graduation: internal → SPI

Sometimes a type that started as internal becomes part of the SPI
later. The procedure:

1. **Two consumers required**: at least two crates (or one external
   consumer + one workspace crate) must depend on the type's
   contract for it to graduate.
2. **Stable shape**: no planned refactor that would change the
   type's surface in the next 6 months.
3. **Owner**: name a single owner (the team that introduces it) to
   handle migration questions from connectors.
4. **Document**: add an entry to § 3 of this file, mark the type
   with `#[non_exhaustive]` if it's an enum, write the
   `//!` banner per § 4.3.
5. **Bump**: cut a new minor version of `pylon-connector-spi` (or
   `pylon-types`, depending on which crate hosts the type) and add
   a CHANGELOG entry under "Added (promoted from internal)".

This procedure mirrors Presto's pattern for promoting engine
internals to SPI when a second consumer materialises.

## 10. Open questions

- **Multiple SPI tiers** (alpha / beta / stable per type)? The
  ecosystem (Tokio, Apache Arrow) uses this pattern; it costs us
  nothing now but might earn us clarity at 1.0. Decide at the 1.0
  cut.
- **FFI / C bindings** for connectors in other languages? Out of
  scope for 2026 (no consumer asked); would warrant a separate
  `pylon-connector-sys` crate if it ever lands.
- **`#[non_exhaustive]` audit**: § 4.1 says *every* SPI enum
  should be `#[non_exhaustive]`, but the current `pylon-types`
  re-exports Arrow types we cannot mark. Decide whether to wrap
  them in newtypes so we control the marker.
- **`unsafe_code = "forbid"`**: workspace-wide today. This is a
  silent stability guarantee (no `unsafe` means no UB drift) and
  should be promoted to a "stability tier 0" item in this doc.
- **Rust MSRV policy**: workspace pins `rust-version = "1.85"`.
  MSRV bumps are technically breaking under SemVer. We treat them
  as a major-version event; document at 1.0.

## 11. References

- RFC 0001 §2 — ADR-001 / ADR-002 (engine / connector split).
- RFC 0005 §1 — module layout (Presto three-tier pattern).
- RFC 0005 §3 — domain boundaries (rule #1: SPI depends only on
  `pylon-types` + Arrow).
- RFC 0005 §4 — trait signatures for `Connector`, `DataSource`,
  `DataSink`, `ConnectorError`, `ConnectorPage`, `ConnectorColumns`,
  `ConnectorFactory`, `DataSourceContext`, `DataSinkContext`.
- RFC 0005 §6 item 3 — extract `pylon-connector-spi` (this doc is
  its hand-shake with connector authors).
- RFC 0005 §7 R9 (= user's table R8) — this document.
- `tools/check-spi-boundaries.sh` — companion script (RFC 0005 §3).
- `tools/check-trait-stability.sh` — companion script (this PR).
- Presto `presto-spi` — the canonical reference for SPI ↔ engine
  layering. See `docs/research/findings.md` §1 + §3.
