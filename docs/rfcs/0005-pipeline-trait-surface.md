# RFC 0005 — Query Pipeline Trait Surface

- **Status**: Draft (2026-08-18)
- **Owner**: Pylon working group
- **Goal**: Lock down the core trait surface and per-role domain boundaries
  before adding more layer-on-layer feature work, so the existing skeleton
  can be refactored *into* these traits instead of around them.

## 0. Why this RFC now

M3-tail (PR1 + PR2) shipped Exchange unification on top of an opportunistic
skeleton. We now need every layer that the skeleton implies — catalog,
logical/physical plan, fragmenter, scheduler, connector SPI, driver-side
operator, exchange — to be defined as **stable traits** so we can grow
features (M4: JOINS, M5: spill, M6: CBO, …) without re-shuffling types
every few commits.

Three reference projects were surveyed for the trait shapes:

| Project | Trait shape of choice | Notes |
|---|---|---|
| **Apache DataFusion** (`/Users/didi/myprojects/references/datafusion`) | `ExecutionPlan + ExecutionPlanner + OptimizerRule + RecordBatchStream`; `TableSource` ↔ `TableProvider` split | Strong "what vs. how vs. where" layering. CBO is in-plan via `Statistics`. No cluster scheduler in core. |
| **Presto** (`/Users/didi/myprojects/references/presto`) | Three modules: `presto-spi` (stable connector surface), `presto-common` (leaf value types), `presto-main-base` (engine internals incl. `Operator`, `Driver`, `SqlStageExecution`, `PlanFragmenter`). Single canonical error `PrestoException` crossing the SPI. | The cleanest SPI ↔ engine boundary in the ecosystem. `Operator`/`Driver`/`PlanFragmenter` are *engine-internal*, not SPI. |
| **Velox** (`/Users/didi/myprojects/references/velox`) | `PlanNode` + `PlanFragment`, `Operator::isBlocked(BR*, ContinueFuture*)`, `DataSource::next(rows, future&)` returning optional, `ExchangeClient` separates request/sizes/data/ack, `OutputBuffer` back-pressures at `continueSize_`. | Operator never blocks — driver does. Strongly-typed `BlockingReason`. `SplitsStore` pluggable per-PlanNode. |

## 1. Module layout (Presto's three-tier pattern)

The cleanest stable layering is Presto's three modules. Pylon currently
folds all of this into one workspace. We should split:

```
pylon-types/        ← leaf value types (Page-equivalent) — Arrow RecordBatch wrappers, Schema, no engine deps
pylon-connector-spi/ ← stable SPI — Connector / ConnectorFactory / DataSource / DataSink / PageLayout / ConnectorError
pylon-plan/         ← engine — LogicalPlan, PhysicalPlan (= ExecutionPlan), Optimizer rules, Fragmenter rules, Statistics
pylon-runtime/      ← engine — PipelineOp, Driver, Pipeline, RecordBatchStream, Exchange (source/sink)
pylon-coord/        ← engine (control plane) — Dispatcher, Scheduler, Discovery, QueryStateMachine
pylon-worker/       ← engine (worker binary) — Task lifecycle, local TaskDriver, ProbeLoop
pylon-storage/, pylon-iceberg/, pylon-catalog/  ← connectors (depend on connector-spi only)
pylon-exchange/     ← transport (Flight) — impl detail of the runtime Exchange
```

Rule: **a crate may only depend on the crates above it in this list**.
A connector depends on `connector-spi` + `types`; the engine depends on
those plus all internal crates. This stops accidental leakage.

## 2. Core roles (single-page map)

| # | Role | Crate | Trait / struct | One-line domain |
|---|---|---|---|---|
| 1 | **LogicalPlan / LogicalExpr** | pylon-plan | `LogicalPlan` enum + `Expr` enum | Symbolic "what to compute" tree. The input to every optimizer pass. |
| 2 | **Analyzer / Binder** | pylon-plan | `Analyzer` | Resolves names/types against `Catalog`, produces `Analysis` (logical). One pass before planning. |
| 3 | **Catalog (engine-side handle)** | pylon-plan | `CatalogProviderList`, `CatalogProvider`, `SchemaProvider` | Reads/writes catalog metadata; **lives in engine** (mirrors Presto's engine-internal `Catalog`). |
| 4 | **Connector SPI** | pylon-connector-spi | `Connector`, `ConnectorFactory`, `DataSource`, `DataSink`, `ConnectorError`, `ConnectorPage` | The stable plug-in surface. Connectors depend on *this* crate only. |
| 5 | **PhysicalPlan / PhysicalExpr** | pylon-plan | `trait ExecutionPlan` + `trait PhysicalExpr` | Concrete operator tree — "how to compute it". Trait object, `Arc<dyn ExecutionPlan>` everywhere. |
| 6 | **OptimizerRule** | pylon-plan | `trait LogicalOptimizerRule` + `trait PhysicalOptimizerRule` | RBO rewrites, both whole-plan and pattern-matched. Pure tree → tree. |
| 7 | **PhysicalPlanner** | pylon-plan | `trait PhysicalPlanner` | `LogicalPlan + LogicalExpr` → `Arc<dyn ExecutionPlan> + Arc<dyn PhysicalExpr>`. |
| 8 | **Statistics** | pylon-plan | `struct PhysicalStats { … }` + `trait StatisticsProvider` | Row-count / byte-size / min-max-NDV for CBO inputs and push-down decisions. |
| 9 | **Fragmenter / FragmenterRule** | pylon-plan | `trait FragmenterRule` + `fn fragment(plan, query_id, worker_flight_addrs) → StageDag` | Cuts an `ExecutionPlan` at rule-marked operators into a `StageDag`. |
| 10 | **StageDag / TaskSpec** | pylon-plan + pylon-coord | `struct StageDag`, `struct TaskSpec { id, query_id, stage_id, partition, fragment, sources, sinks, budget }` | Frozen-bytes serialization between coord and worker. |
| 11 | **Scheduler** | pylon-coord | `trait Scheduler`, `struct WorkerCapacity` | Assigns `TaskSpec → WorkerId`. Pipelined, capacity-gated; later: cost-based, hash-affinity. |
| 12 | **Dispatcher** | pylon-coord | `Dispatcher` (the `bin/pylon-coord` glue, refactored) | Owner of stage boundaries (PR1 + PR2 pattern); dispatches via gRPC + tracks TaskDone ACKs. |
| 13 | **QueryStateMachine** | pylon-coord | `struct QueryStateMachine` | Per-query FSM: queued → planning → dispatching → running → draining → finished/failed/cancelled. Listeners on transition. |
| 14 | **RecordBatchStream** | pylon-runtime | `trait RecordBatchStream` + `SendableRecordBatchStream` type alias | The single "stream of batches" type between every layer of the engine — *mirrors DataFusion exactly*. |
| 15 | **Pipeline / PipelineOp** | pylon-runtime | `trait PipelineOp` (today) | One pipeline step. Pull-style `get_output()` + push-style `add_input()` + `no_more_input()` + `is_finished()`. Owned by one driver at a time. |
| 16 | **Driver / DriverState** | pylon-runtime | `enum Driver` (today; will become a struct) | Poll loop over one `Pipeline`. Single-thread. Today uses `SingleThreadLoop` driver mode. |
| 17 | **Exchange (source / sink)** | pylon-runtime | `trait ExchangeSource` + `trait ExchangeSink` | Cross-task data path. Different in-proc and cross-worker transports; same trait. Replaces the in-process "push to PylonFlightService" shortcut. |
| 18 | **Connectors** | pylon-catalog, pylon-storage, pylon-iceberg | `impl Connector` + `impl DataSource/DataSink` | Plug-in impls of the SPI. Each owns its catalog type (`CatalogStub` today). |
| 19 | **Error** | pylon-types | `enum PylonError` (existing) + `struct ConnectorError { code, msg }` | Single canonical error crossing every boundary. Connectors must not return raw `anyhow::Error`. |
| 20 | **Session / TaskContext / QueryContext** | pylon-plan + pylon-runtime | `struct SessionConfig`, `struct TaskContext { session_id, query_id, task_id, driver_id, runtime, connector_configs }` | Lifecycle contexts. *Planning-time* (`SessionConfig`) ≠ *exec-time* (`TaskContext`) — same separation as DataFusion calls out explicitly. |

## 3. Domain boundaries — the rules of the game

These are the load-bearing ones. Each is enforceable today with `cargo`
dependency rules; violations should fail a PR-review.

1. **`pylon-connector-spi` depends only on `pylon-types` (and `arrow_*`)**.
   No `pylon-plan`, `pylon-runtime`, `pylon-coord` imports anywhere in
   the SPI crate. This is what makes connectors a stable plug-in surface.

2. **`LogicalPlan` and `ExecutionPlan` are *not* SPI types.** They live
   in `pylon-plan` and may evolve freely. Connectors never see them;
   they only see the row-level `ConnectorPage` + the split metadata
   handed in via `DataSource::add_split`.

3. **`Operator` / `PipelineOp` is engine-internal.** Connectors implement
   `DataSource`, not `PipelineOp`. The engine wraps a `DataSource`
   inside a scan-specific `PipelineOp` (one source op per connector).
   This matches Presto/Velox and avoids per-connector threading contracts.

4. **`Exchange` is engine-internal but split into `ExchangeSource` and
   `ExchangeSink` traits in `pylon-runtime`.** A connector doesn't
   touch exchange — exchanges are between two engine operators.

5. **Error type crossing the SPI is `ConnectorError`, not `anyhow::Error`
   or `pylon_types::Error`.** Code in the engine maps connector errors
   to internal `PylonError` at the SPI boundary.

6. **The driver is the only place that holds `&mut PipelineOp`.**
   Operators are not `Send + Sync`; they are owned by one driver thread.
   Today `PipelineOp::add_input(&mut self, …)` encodes this. **Do not
   ever relax this** without a deliberate decision to switch to a
   connector-multi-threaded model.

7. **`TaskContext` is the single runtime context threaded through every
   operator.** No implicit globals (no `lazy_static` config, no thread
   locals). Stats, memory pool, cancellation token, session config — all
   passed explicitly. This is what DataFusion and Velox both do.

8. **`PlanOptimizer` / `PhysicalOptimizerRule` / `FragmenterRule` are
   all pure tree → tree.** Sync, no `async`. They never touch
   `Catalog` async paths; the `Metadata` helper passed in is a
   blocking, pre-fetched `dyn ConnectorMetadata` snapshot.

## 4. Trait signatures (the load-bearing ones)

These are the contracts to stabilize. Once frozen (`#[non_exhaustive]`,
no breaking renames), downstream work assumes them.

```rust
// =====================================================================
// pylon-types — value types
// =====================================================================

/// The single "stream of batches" type used between *every* engine
/// layer. Mirrors DataFusion's `SendableRecordBatchStream` exactly:
/// the trait is dyn-compatible, the alias boxes a Sendable version,
/// every operator's pull-style sink produces one.
pub trait RecordBatchStream {
    fn schema(&self) -> SchemaRef;
}
// Pin<Box<dyn RecordBatchStream + Send>> is the engine-wide alias.
//
// We intentionally do NOT model blocking/future in this trait:
// operators are non-async (SingleThreadLoop driver). The Poll-style
// driver loops until the stream returns Poll::Ready(None) for EOS.

#[non_exhaustive]
pub enum PylonError {
    InvalidPlan(String),
    Internal(String),
    NotFound(String),
    Io(String),
    Compute(String),
    External(ConnectorError),
    // Engine never returns `anyhow::Error` to callers.
}

// =====================================================================
// pylon-connector-spi — stable plug-in surface
// =====================================================================

#[non_exhaustive]
pub enum ConnectorErrorCode {
    NotFound, InvalidArgument, IO, Schema, Unimplemented,
    ResourceExhausted, Other,
}

pub struct ConnectorError {
    pub code: ConnectorErrorCode,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

pub type ConnectorResult<T> = std::result::Result<T, ConnectorError>;

/// Stable page type going across the SPI boundary.
/// (Mirrors Presto's `Page` Block[] but with Arrow-compatible layout.)
pub struct ConnectorPage {
    pub schema: SchemaRef,
    pub num_rows: usize,
    /// Column handles inside the connector's own arena. The engine
    /// materialises them to `arrow::RecordBatch` once, at the scan
    /// operator boundary.
    columns: Box<dyn ConnectorColumns>,
}

pub trait ConnectorColumns {
    fn column(&self, i: usize) -> ConnectorColumnRef<'_>;
    // … (defined per layout — fixed-width, dictionary, etc.)
}

pub trait Connector: Send + Sync {
    fn connector_id(&self) -> &ConnectorId;
    fn capabilities(&self) -> ConnectorCapabilities;
    /// Factory: one DataSource per scan / driver.
    fn create_data_source(
        &self,
        ctx: &DataSourceContext,
        table_handle: Arc<dyn ConnectorTableHandle>,
        projection: Option<&[usize]>,
        filters: &[BoundPredicate],
        limit: Option<usize>,
    ) -> ConnectorResult<Box<dyn DataSource>>;
    fn create_data_sink(
        &self,
        ctx: &DataSinkContext,
        table_handle: Arc<dyn ConnectorInsertHandle>,
        schema: SchemaRef,
    ) -> ConnectorResult<Box<dyn DataSink>>;
}

#[async_trait::async_trait]
pub trait ConnectorFactory: Send + Sync {
    fn name(&self) -> &str;
    async fn create(
        &self,
        config: ConnectorConfig,
    ) -> ConnectorResult<Box<dyn Connector>>;
}

/// Per-driver / per-task setup passed to every `DataSource`:
/// session properties, memory pool handle, cancellation token,
/// statistics sink. Replaces Velox's `ConnectorQueryCtx`.
pub struct DataSourceContext {
    pub query_id: QueryId,
    pub task_id: Option<TaskId>,
    pub driver_id: Option<DriverId>,
    pub session: Arc<SessionConfig>,
    pub memory_pool: Arc<dyn MemoryPool>,
    pub cancellation: CancellationToken,
    pub stats_sink: Arc<dyn StatisticsSink>,
}

pub trait DataSource: Send {
    fn add_split(&mut self, split: Box<dyn ConnectorSplit>)
        -> ConnectorResult<()>;
    /// Pull-style pages. Returns None on EOS, Some(page) on data.
    /// Implementations back-pressure via the memory pool: when
    /// `pool.try_grow(target)` returns Err, return pending and the
    /// driver will re-poll after downstream drains.
    /// (M4.S1 note: `MemoryPool` also exposes `release(bytes)` as
    /// an explicit additive method; see RFC 0007 §3.1.)
    fn next(&mut self) -> ConnectorResult<Option<ConnectorPage>>;
    fn estimated_row_size(&self) -> usize;
    fn completed_bytes(&self) -> u64 { 0 }
    fn completed_rows(&self) -> u64 { 0 }
    fn cancel(&mut self) {}
}

pub trait DataSink: Send {
    fn append(&mut self, page: ConnectorPage) -> ConnectorResult<()>;
    fn finish(&mut self) -> ConnectorResult<WriteStats>;
    fn abort(&mut self) -> ConnectorResult<()> { Ok(()) }
}

// =====================================================================
// pylon-plan — "what" + "how"
// =====================================================================

pub enum LogicalPlan {
    TableScan { table: String, projection: Vec<usize>, filters: Vec<BoundPredicate> },
    Filter { input: Box<LogicalPlan>, predicate: BoundPredicate },
    Project { input: Box<LogicalPlan>, exprs: Vec<LogicalExpr> },
    Aggregate { input: Box<LogicalPlan>, group_by: Vec<LogicalExpr>, aggs: Vec<AggExpr> },
    Join { kind: JoinKind, left: Box<LogicalPlan>, right: Box<LogicalPlan>, on: Vec<JoinClause> },
    Sort { input: Box<LogicalPlan>, order_by: Vec<LogicalExpr> },
    Limit { input: Box<LogicalPlan>, count: usize },
    ExchangeRef { stage_id: StageId, partition_kind: PartitionKind },
    Extension(Arc<dyn UserDefinedLogicalNode>),
}

pub enum LogicalExpr {
    Column(String), Literal(LiteralValue),
    Binary { op: BinaryOp, lhs: Box<LogicalExpr>, rhs: Box<LogicalExpr> },
    Aggregate { name: String, arg: Option<Box<LogicalExpr>>, distinct: bool },
    ScalarFunc { name: String, args: Vec<LogicalExpr> },
    Cast { expr: Box<LogicalExpr>, target: DataType },
}

/// The single most important trait. Every physical operator conforms.
/// Mirrors DataFusion's `ExecutionPlan` closely, with pylon-specific
/// cuts for the channels it actually carries (statistics, exchange
/// requirements, repartitioning behaviour).
pub trait ExecutionPlan: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
    fn schema(&self) -> SchemaRef;

    /// Properties the planner/scheduler can read in O(1).
    fn properties(&self) -> &PlanProperties;

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;

    /// What the operator REQUIRES of each child's output distribution.
    /// (Mirrors DataFusion's `required_input_distribution`.)
    fn required_input_distribution(&self) -> Vec<DistributionRequirement> {
        vec![DistributionRequirement::Unspecified; self.children().len()]
    }
    /// What the operator CLAIMS about its own output distribution.
    /// This is the trait that lets fragmenter / scheduler decide
    /// whether to cut an exchange here.
    fn output_distribution(&self) -> Distribution;

    /// For CBO: declared output statistics, or `None` to defer.
    fn statistics(&self) -> Option<Arc<PhysicalStats>> { None }

    /// Replace children (used by optimizer/rules).
    fn with_new_children(
        &self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Required for any operator that requests an exchange. Today
    /// only `ExchangeSink`-side operators return true.
    fn requires_exchange(&self) -> bool { false }
}

/// A snapshot of partitioning / ordering / boundedness that the
/// operator publishes once per (immutable plan) re-derivation.
pub struct PlanProperties {
    pub distribution: Distribution,
    pub output_ordering: Option<LexOrdering>,
    pub boundedness: Boundedness,
    pub emission: EmissionType,
}

pub enum Distribution {
    Single,
    RoundRobin { partition_count: usize },
    Hash { keys: Vec<Arc<dyn PhysicalExpr>>, partition_count: usize },
    Broadcast,
    Unknown { estimated_count: usize },
}
pub enum DistributionRequirement {
    Unspecified, Single, Hash { keys: Vec<Arc<dyn PhysicalExpr>> },
    Broadcast, AnyOf(Vec<Distribution>),
}

pub trait PhysicalExpr: Send + Sync + std::fmt::Debug {
    fn data_type(&self, schema: &Schema) -> Result<DataType, PylonError>;
    fn nullable(&self, schema: &Schema) -> Result<bool, PylonError>;
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef, PylonError>;
    fn return_field(&self, schema: &Schema) -> Result<FieldRef, PylonError>;
    fn as_any(&self) -> &dyn std::any::Any;
}

pub trait LogicalOptimizerRule: Send + Sync {
    fn name(&self) -> &str;
    fn rewrite(
        &self,
        plan: LogicalPlan,
        ctx: &mut RewriteContext,
    ) -> Result<LogicalPlan, PylonError>;
    /// When does this rule fire? Used by the optimizer's fixed-point
    /// loop. `EveryPass` for idempotent pushes; `Once` for rewrites
    /// that shouldn't run repeatedly (like CollapseProject→Identity).
    fn apply_order(&self) -> ApplyOrder { ApplyOrder::EveryPass }
}

pub trait PhysicalOptimizerRule: Send + Sync {
    fn name(&self) -> &str;
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &PhysicalOptimizerConfig,
    ) -> Result<Arc<dyn ExecutionPlan>, PylonError>;
}

/// The rule at the centre of the fragmenter. **New:** this is the
/// pattern everyone uses; today we have a single hand-rolled visitor.
/// Each rule says "for an operator of this kind, put it in this stage
/// and emit this exchange materialization".
pub trait FragmenterRule: Send + Sync {
    fn name(&self) -> &str;
    /// Return Some(strategy) if this node is a stage boundary.
    fn boundary_for(
        &self,
        node: &dyn ExecutionPlan,
    ) -> Option<BoundaryStrategy>;
    /// Annotate the exchange that the dispatcher must materialize.
    /// Default: HashPartitionExchange over `keys`, fan-out to
    /// `target_partitions` partitions routed by the dispatcher.
    fn default_strategy(&self) -> BoundaryStrategy {
        BoundaryStrategy::HashPartition { target_partitions: 16 }
    }
}

pub enum BoundaryStrategy {
    HashPartition { target_partitions: usize },
    Broadcast,
    Single,
    Gather,
    GatherToOne,
}

#[async_trait::async_trait]
pub trait PhysicalPlanner: Send + Sync {
    async fn create_physical_plan(
        &self,
        logical: &LogicalPlan,
        session: &Session,
    ) -> Result<Arc<dyn ExecutionPlan>, PylonError>;
}

// =====================================================================
// pylon-coord — stage + task + scheduler + dispatch
// =====================================================================

pub struct StageDag {
    pub stages: Vec<Stage>,
}
pub struct Stage {
    pub id: StageId,
    pub root: Arc<dyn ExecutionPlan>, // after fragmenter marks boundaries
    pub partition_count: usize,
    pub upstream: Vec<StageId>,
    pub downstream: Vec<StageId>,
}

pub struct TaskSpec {
    pub id: TaskId,
    pub query_id: QueryId,
    pub stage_id: StageId,
    pub partition: usize,
    pub root: Arc<dyn ExecutionPlan>,
    pub sources: Vec<ExchangeRequirement>, // per upstream stage
    pub sinks:   Vec<ExchangeRequirement>, // per downstream stage
    pub budget: MemoryBudget,
}
pub struct ExchangeRequirement {
    pub kind: ExchangeKind,
    pub target_peer: Option<PeerLocator>, // None ⇒ same worker
    pub descriptor: ExchangeDescriptor,
}

pub trait Scheduler: Send + Sync {
    fn assign(
        &self,
        dag: &StageDag,
        workers: &[WorkerCapacity],
        query_id: QueryId,
    ) -> Vec<(TaskSpec, WorkerId)>;
}

pub struct QueryStateMachine {
    state: AtomicQueryState,
    listeners: ListenerManager<QueryEvent>,
    // ... (mirrors Presto's QueryStateMachine)
}

// =====================================================================
// pylon-runtime — pipeline + operator + driver + exchange
// =====================================================================

/// Single pipeline step. **Owned by exactly one Driver thread.**
/// `&mut self` enforces this in the type system. Async fn is OK
/// because SingleThreadLoop runs ops sequentially on one task.
#[async_trait::async_trait]
pub trait PipelineOp: Send {
    fn name(&self) -> &'static str;
    async fn needs_input(&self) -> bool;
    async fn add_input(&mut self, batch: RecordBatch) -> Result<(), PylonError>;
    async fn no_more_input(&mut self) -> Result<(), PylonError>;
    /// Pull-style. Returns None on EOS.
    async fn get_output(&mut self) -> Result<Option<RecordBatch>, PylonError>;
    async fn is_finished(&self) -> bool;
}

/// Implementation note: ops may need `&mut self` access to shared
/// channels *across* polls. We use the MPSC-of-batches pattern
/// (DataFusion's `RecordBatchReceiverStreamBuilder`) — spawn the
/// producer in the driver's spawn helper so drop cancels it.

pub struct Driver {
    pipeline: Pipeline,
    state: DriverState,
    task_context: Arc<TaskContext>,
}
impl Driver {
    pub async fn run(self) -> Result<DriverOutput, PylonError>;
}

pub enum DriverState {
    Running,
    Draining,
    BlockedOn { op_index: usize, reason: BlockReason },
    Finished,
    Failed(PylonError),
}

/// Cross-task data path. **New:** one trait for both
/// `ExchangeSink`-side and `ExchangeSource`-side; the per-direction
/// adapter wraps the trait.
pub trait ExchangeTransport: Send + Sync {
    /// Async push. Implementations serialize bytes / Arrow IPC.
    fn push(&self, batch: RecordBatch) -> impl Future<Output = Result<(), PylonError>> + Send;
    /// Async pull. Yields Some(batch) or None on EOS.
    fn pull(&self) -> impl Future<Output = Result<Option<RecordBatch>, PylonError>> + Send;
    fn close(&self) -> impl Future<Output = Result<(), PylonError>> + Send;
}

/// The two adapter enums that frame the trait above:
pub enum ExchangeSinkOp { Local(LocalChannel), Flight(FlightSink) }
pub enum ExchangeSourceOp { Local(LocalChannel), Flight(FlightSource) }

/// Today `PylonFlightService` is the in-process provider. It keeps its
/// role as the *queue* layer under the trait; the trait just stops
/// leaking that detail upward.
```

## 5. Current → target mapping

| Pylon today (file:line) | Trait it should conform to | Notes |
|---|---|---|
| `pylon-plan/src/translate.rs` | `LogicalPlanner` (AST → `LogicalPlan`) | Today returns `LogicalPlan` directly; needs explicit `Analyzer` step first. |
| `pylon-plan/src/translate.rs` `physical_plan` enum | `enum LogicalPlan` + `trait ExecutionPlan` | The current `PhysicalPlan` enum becomes the *logical* one; a new `ExecutionPlan` trait owns the actual operator tree. **Big lift**, but unavoidable. |
| `pylon-plan/src/translate.rs` `CatalogStub` | `impl Catalog` (Presto engine-side) + `impl Connector` (the SPI it owns) | `CatalogStub` is the only impl today; the trait shapes both sides. |
| `pylon-runtime/src/op.rs` `PipelineOp` trait | `trait PipelineOp` (same shape; final) | Stabilize doc + add `&mut self` enforcement hint + explicit `Send` only (no `Sync`). |
| `pylon-runtime/src/pipeline.rs` `Pipeline::new(vec![…])` | `Pipeline::new(Vec<Box<dyn PipelineOp>>)` | Today. The driver-loop invariant is *one driver owns one `Pipeline`*. |
| `pylon-runtime/src/driver.rs` `enum Driver { OwnedSingleThread(…) }` | `struct Driver` (no enum mode) | Drop legacy `PerOpTokioTask` mode from M2 (kept only for old tests); enforce `SingleThreadLoop` everywhere. |
| `pylon-runtime/src/ops/exchange.rs` `ExchangeSinkRpc` + `ExchangeSourceOp` | `enum ExchangeSinkOp { Local/Flight }` + `enum ExchangeSourceOp { Local/Flight }` | The post-M3-tail PR1/PR2 shape is the right basis; **add `LocalChannel` variant** for clarity (today "local" is just the same-worker's flight_addr). |
| `pylon-runtime/src/ops/exchange.rs::compute_partitions` | stays private to op crate | The hash-fn is encapsulated. No trait exposure. |
| `pylon-coord/src/fragment.rs` `Fragmenter::fragment` | `Fragmenter::new(rules: Vec<Arc<dyn FragmenterRule>>)` + `fragment(plan) → StageDag` | The single hard-coded `Aggregate` rule becomes one `FragmenterRule` impl. Adding `HashJoin` / `Distinct` / `Window` becomes a new rule impl. |
| `pylon-coord/src/fragment.rs` `FragmentCtx.worker_flight_addrs` | `(removed — dispatch owns placement)` | Already purged in PR2. The dispatcher rewrite in `bin/pylon-coord.rs` does placement. |
| `pylon-coord/src/scheduler.rs` `CapacityScheduler` | `trait Scheduler::assign` | Existing shape; add `WorkerCapacity { memory, drivers }` (already there). Add `HashAffinityScheduler` later. |
| `pylon-coord/src/discovery.rs` `Discovery` | `pub struct WorkerRegistry { workers: HashMap<WorkerId, Arc<RegisteredWorker>> }` | Same semantics; rename for general use. |
| `pylon-coord/src/bin/pylon-coord.rs` `rewrite_exchange_targets_in_place` | `Dispatcher::assign_exchange_targets(plan, partition_to_worker) → Result<()>` (lift out of bin file) | Already lives at the dispatch seam. Move to a small coord lib. |
| `pylon-worker/src/main.rs` worker factory `match config_name { "SeqScan" => … "ExchangeSinkRpc" => … }` | `OperatorFactory` registry — each engine op registers a constructor | Today the worker holds a giant match. Mirror Velox's `PlanNodeTranslator` registry. |
| `pylon-worker/src/main.rs` worker `PylonFlightService` | `Arc<PylonFlightService>` is the receiver-side of `ExchangeTransport::pull()` for `Local` variant | Keeps its current shape; just becomes a concrete impl. |
| `pylon-exchange/src/{flight_server, flight_rpc, flight_client}` | impl details of `ExchangeTransport` (Flight variant) | Stays internal; the trait is in `pylon-runtime`. |
| `pylon-types/src/lib.rs` `PylonError` | stays; add `ConnectorError` re-export | The "external" variant of `PylonError` is `External(ConnectorError)`. |
| `pylon-catalog/`, `pylon-storage/`, `pylon-iceberg/` | implement `Connector` from `pylon-connector-spi` | **New crate split.** Today these are independent crates with no shared trait. |
| `bin/pylon-coord.rs` `sleep(3)` polling in `wait_for_stage_done_inner` | real `TaskDone` ack gRPC message + `QueryStateMachine::wait_for_stage_done` | The M3-tail #1 follow-up. Ack message: `TaskDone { query_id, stage_id }`. |

## 6. Optimization directions (the existing skeleton, refactored)

1. **`PhysicalPlan` enum → `ExecutionPlan` trait**. **Highest-leverage
   change.** Today `translate.rs` has 6+ variants in the `enum
   PhysicalPlan { SeqScan, Filter, Project, Aggregate, ExchangeSink, … }`
   shape. Every place that consumes a `PhysicalPlan` carries 6 arms.
   Once it's a trait, callers do `node.children()` / `node.properties()`
   and never need a giant match. *Estimated size: ~600 lines deleted
   from `fragment.rs` + scheduler.rs + op factories; replaced by a
   `Vec<Box<dyn PhysicalOptimizerRule>>` registration.*

2. **Drop the legacy `PerOpTokioTask` driver mode** *(lands as
   R5-pre, see § 7.1)*. `crates/pylon-runtime/src/driver.rs` keeps two
   parallel driver paths — `enum Driver { OwnedSingleThread, SharedPerOpTask }`
   (`driver.rs:67`) with `DriverMode::SingleThreadLoop | PerOpTokioTask`
   — and the M1/M2 per-op-as-tokio-task path is dead code today. The
   half-finished refactor debris piles up:
   `run_per_op_task_legacy` (driver.rs ≈210),
   `run_legacy_op` (driver.rs ≈270) with its mpsc-channel `try_recv`
   polling pattern, and two `dyn_clone_*` helpers that `panic!` because
   `dyn PipelineOp: Clone` isn't implementable (driver.rs ≈179 and
   :241). Half-completed state-machine work sits in pipeline.rs
   (`progressed = true` reassigned at :184 and :265 but never read,
   `unused_assignments` warning); a `let oname = …` placeholder is
   still in driver.rs :269 (unused). The modern path is the
   `SingleThreadLoop` driver in `pipeline.rs::run_pipeline_single_thread`
   — once R5-pre lands, `driver.rs` shrinks from 407 → ~250 lines and
   the legacy path's full mpsc + per-op-task machinery is gone.

   **Future tightening (separate task):** the trait `PipelineOp: Send +
   Sync` (`pylon-runtime/src/op.rs:20`) almost certainly only needs
   `Send` — every method takes `&mut self` and one driver thread owns
   the op, so `Sync` is unused. Velox/Presto both keep driver-thread
   methods single-threaded by API shape. This is a 1-line supertrait
   change; do it as a separate PR alongside R5-pre so a `grep` for
   `&PipelineOp` or cross-thread passing proves no one relied on `Sync`.

3. **Extract a `pylon-connector-spi` crate**. Move `CatalogStub` (or
   its successor) to be a thin example impl, and let `pylon-catalog` /
   `pylon-storage` / `pylon-iceberg` depend only on the SPI. **No
   reconnect surgery to engine types.** Today they all reach into
   `pylon-plan`'s `CatalogStub` directly, which ties them to the
   engine-side shape.

4. **Promote `FragmenterRule` to a trait list**. Today the
   `Aggregate`-only cut is hard-baked. Lifting it to
   `Fragmenter::new(rules: Vec<Arc<dyn FragmenterRule>>)` lets a new
   rule join via `Fragmenter::with_rule(...)` builder without
   touching the fragmenter visitor.

5. **Replace the worker op-factory match with a registry**. Worker
   currently has `for op_name in pipeline { match op_name { "SeqScan"
   => … "ExchangeSinkRpc" => … } }`. Replace with `let factory =
   OPERATOR_REGISTRY.get(op_name)?` and register each operator as a
   `fn(OpSpec) -> Result<Box<dyn PipelineOp>>` at startup. Velox calls
   this `PlanNodeTranslator`.

6. **Drop `PylonError::variant_count` explosion**. Audit
   `pylon-types/src/lib.rs::PylonError`; some variants exist only for
   one call site. Fold through `PylonError::InvalidPlan(String)`
   unless they carry info the engine actually propagates.

7. **Ack message + `QueryStateMachine`**. Replace
   `wait_for_stage_done_inner`'s `sleep(2)` with: worker
   `TaskDone { query_id, stage_id }` gRPC unary; coord records in
   `QueryStateMachine`; the dispatcher's poll becomes `await
   state_machine.wait_for(STAGE, stage_id, deadline)`. This is the
   M3-tail #1 we already deferred; the `QueryStateMachine` struct is
   the right home for it.

8. **`DataSource` first-class**. The current "I scan a Parquet file"
   is hidden inside `SeqScanOp`. Pushing this through the connector
   SPI lets `pylon-iceberg` plug in without `pylon-runtime` ever
   knowing about Iceberg. The first concrete pay-off is replacing
   `SeqScanOp` with `ScanOp { source: Box<dyn DataSource>, … }`.

9. **`Statistics` hook**. Plan: add `StatisticsSink` to the
   `DataSourceContext` so connectors can push per-scan stats;
   `PhysicalStats` is on every `PlanProperties`; CBO reads them
   when we eventually ship it. Skeleton today.

## 7. Refactor sequence (the PR sequence to land it)

| PR | Goal | Risk | Depends on |
|---|---|---|---|
| **R0** | Add `pylon-connector-spi` crate skeleton (empty types, `Cargo.toml` deps). CI passes; nothing uses it yet. | Low | none |
| **R1** | Define the value-type leaf in `pylon-types` (`PylonError` non-exhaustive + `ConnectorError` types; `SendableRecordBatchStream` alias). Stabilize the cross-cutting types first. | Low | R0 |
| **R2** | `pylon-plan`: introduce `trait ExecutionPlan` + `trait PhysicalExpr`. Convert the existing `PhysicalPlan` enum into one struct-of-impls (one impl per current variant). Keep the enum as a *thin* ergonomic facade over `Arc<dyn ExecutionPlan>`. Delete the `enum PhysicalPlan` arms one at a time as call sites migrate. | **High** — touches everything that consumes `PhysicalPlan` (fragmenter, scheduler, op factory, worker). | R1 |
| **R3** | `pylon-plan`: introduce `FragmenterRule` trait + lift the `Aggregate` rule into an impl. Fragmenter becomes `new(rules: Vec<Arc<dyn FragmenterRule>>)`. | Medium | R2 |
| **R4** | `pylon-runtime`: introduce `ExchangeTransport` trait + `ExchangeSinkOp`/`ExchangeSourceOp` enums (Local/Flight). The existing `ExchangeSinkRpc` impl becomes the `FlightSink` arm; `PylonFlightService` becomes the `LocalChannel` arm. | Medium | R2 |
| **R5-pre** | `driver.rs` 瘦身（零 trait 改动） — see § 7.1 checklist. Pure dead-code removal + state-machine tight-ups. **Lands before R5** so the registry PR lands on already-clean driver code. | Low | none |
| **R5** | `pylon-worker`: introduce `OperatorFactory` registry; drop the giant match. Each operator adds `register_operator("Name", |spec| { … })` at startup. | Low | R2; benefits from R5-pre |
| **R6** | Move `pylon-catalog`/`pylon-storage`/`pylon-iceberg` onto `pylon-connector-spi`. Engine-side `Catalog`/`SchemaProvider` traits added to `pylon-plan` mirror Presto's `Catalog` / `CatalogProvider` / `SchemaProvider`. | Medium | R0 + R1 |
| **R7** | `QueryStateMachine` + `TaskDone` ack (M3-tail #1). Coord-side `sleep` removed. | Low (mechanical) | none |
| **R8** | Add a `LogicalOptimizer` loop with at least `PredicatePushdown` + `ProjectCollapse` as `dyn LogicalOptimizerRule`. Physical: `RepartitionRule`. | Low | R2 |
| **R9** | Document the trait surface in `docs/design/trait-stability.md`: "stable" vs "internal" marked explicitly; SPI breaking-change policy. | Low | R0–R6 done |

After R9 (probably 5–8 months of steady work): the engine has the
trait shape that Trino/Velox/DataFusion have. New features (broadcast
exchange, hash-join, spill, CBO) plug in via the seams already cut.

### 7.1. R5-pre checklist — `driver.rs` cleanup (zero trait changes)

Pure dead-code removal + thread-safety tightening. **No trait additions
or signature changes** beyond relaxing `PipelineOp: Send + Sync` to
`Send` (paired micro-task, see step 8). The PR keeps `cargo test
--workspace` green (currently 99/99); commits batch by file for easy
revert. Estimated diff: **−100 / +20 lines** net.

- [ ] **D1.** Delete `enum DriverMode` (`driver.rs:35-46`) and the
      `with_mode()` switch (`driver.rs:99-111`). The legacy
      `PerOpTokioTask` arm has no callers in the live test suite.
- [ ] **D2.** Collapse `enum Driver { OwnedSingleThread, SharedPerOpTask }`
      (`driver.rs:67-78`) to a single struct:
      ```rust
      pub struct Driver {
          id: DriverId,
          pipeline: Pipeline,
          task_ctx: Arc<TaskContext>,
      }
      ```
      `Driver::new()` becomes the only constructor; the
      `new_legacy()` constructor + its `Arc<Pipeline>` arm dies.
- [ ] **D3.** Delete `run_per_op_task_legacy()` (`driver.rs ≈210`)
      and `run_legacy_op()` (`driver.rs ≈270`). Removes the entire
      mpsc-per-op-task machinery: the `Vec<Arc<Mutex<Box<dyn
      PipelineOp>>>>`, the per-op `mpsc::channel::<RecordBatch>`,
      the `try_recv`/`Disconnected` polling. Unused for ~6 months;
      `run_pipeline_single_thread` in pipeline.rs is the only path
      any current test reaches.
- [ ] **D4.** Delete `dyn_clone_pipeline_op()` (`driver.rs:179`)
      and `dyn_clone_box()` (`driver.rs:241`). Both `panic!` with the
      same message ("Dyn cloning of PipelineOp is not supported").
      Removing them kills two `#[allow(dead_code)]` cuts hiding the
      fact that the surrounding code is dead.
- [ ] **D5.** Delete the `Vec<DriverOutput>` + `JoinSet` plumbing in
      the legacy branch (driver.rs ≈220-260) and the unused
      `DriverId::generate` static-counter reset (a hint that the
      branch was meant for parallel-driver experiments that never
      shipped).
- [ ] **D6.** In `pipeline.rs`, decide on `progressed`. Two options:
      (a) wire it into the loop so `Running` → `BlockedOn` only fires
      when no progress was made this tick (the PR-author intent), or
      (b) delete the two `progressed = true` writes at :184 and :265
      and remove the variable. Pick one; default to (a) since it
      matches the Velox-style state machine. Document the choice.
- [ ] **D7.** Remove the `let oname = op.lock().await.name().to_string();`
      placeholder at `driver.rs:269` (the surrounding call uses
      `.name()` directly).
- [ ] **D8.** Pair task — relax `PipelineOp: Send + Sync`
      (`pylon-runtime/src/op.rs:20`) to `Send` only. Verify by
      `grep -R '&PipelineOp\b' crates/` and the absence of any
      `Arc<dyn PipelineOp>` shared across threads. If any caller
      passes a `&PipelineOp` across an await, fix it before relaxing.

**Verification**:
- `cargo test --workspace` → 99/99 (no regression).
- `cargo build --workspace --all-targets` → no new warnings.
- `grep -nE 'PerOpTokioTask|run_per_op_task_legacy|run_legacy_op|dyn_clone_pipeline_op|dyn_clone_box' crates/` → zero hits.
- File line counts: `driver.rs` 407 → ~250, `pipeline.rs` ~280 → ~250.

**Estimated schedule**: 1–2 days after R7 lands; batches well with
R7 because both edit files in `pylon-runtime`. Risk: **Low.** The
whole exercise is "delete obvious dead code + tighten one bound". A
test that was secretly relying on the legacy path is the only
regression risk; `cargo test --workspace` will catch it.

**Why this gates R5**: R5 (operator factory registry) replaces the
giant match in `pylon-worker/src/main.rs`. The worker factory lives
on a clean driver boundary; if the underlying `Driver` type still
carries the legacy mpsc/dead-code clutter, R5's diff reads worse than
it should. Land R5-pre first; R5 then diffs ~200 lines of pure
registry work, easy to review.


## 8. Open questions / non-goals

- **CBO in 2026?** We don't have row counts today (no statistics
  collection). Skipping CBO requires `Broadcast` over `Hash` for
  small-side joins heuristically. That's a feature for the next RFC.
- **Async connector SPI? `DataSource::next` is currently sync; Velox
  uses `next(rows, future&) -> SemiFuture`.** We could go either way
  without breaking the trait shape. **Decision: ship sync (this
  matches the SingleThreadLoop + memory-pool back-pressure story),
  promote to async-fn-in-trait later when we know we need stream
  multiplexing.**
- **Per-driver `&mut self` vs shared-`&self`.** The pipeline op trait
  keeps `&mut self`; the engine should not pivot to a shared-state
  model unless we accept that the Spark-style "one driver, many
  threads" model brings its own synchronisation tax. (Velox and Presto
  both chose `&mut self`; pylon should too.)
- **`Boundedness` semantics on streaming sources.** We don't have
  Kafka/Filesystem tailing yet; the type is `pub enum Boundedness {
  Bounded, Unbounded { requires_infinite_memory: bool } }` already
  mirrors DataFusion — leave it.

## 9. References

- Presto SPI layering: see references report (Presto section 1 + 3).
- DataFusion `ExecutionPlan + PhysicalPlanner + OptimizerRule`: see
  references report (DataFusion section 5).
- Velox `Operator::isBlocked(BR*, ContinueFuture*)` + 4-call
  ExchangeSource / OutputBuffer two-phase ack: see references report
  (Velox insights 1, 2, 6).
- Existing pylon RFCs:
  - [0001-architecture.md](0001-architecture.md) — overall topology
  - [0002-execution-hierarchy.md](0002-execution-hierarchy.md) — Q/S/T/D layering
  - [0003-m2-control-data-plane.md](0003-m2-control-data-plane.md) — control vs data
  - [0004-m3-flight-shuffle.md](0004-m3-flight-shuffle.md) — current shuffle
- M3-tail plan: [docs/roadmap/m3-tail-exchange-unify.md](../roadmap/m3-tail-exchange-unify.md)
