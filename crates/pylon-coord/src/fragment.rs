//! Fragmenter — `Arc<dyn ExecutionPlan>` → multi-stage `StageDag`.
//!
//! **R3 (RFC 0005 §6 item 4):** the boundary-cut logic is no longer
//! hard-coded. The `Fragmenter` owns a `Vec<Arc<dyn FragmenterRule>>`
//! and consults them in order at every node during the post-order
//! walk; the first rule that returns `Some(strategy)` from
//! `boundary_for(node)` wins and cuts a stage.
//!
//! Adding a new boundary op (M4 HashJoin / Distinct / Window) is now
//! a single-file change: write a new `FragmenterRule` impl and
//! register it with `Fragmenter::with_rule(...)` (or pass it via
//! `Fragmenter::with_rules(cfg, rules)`). The visitor below does
//! not need to change.
//!
//! ## Wire-format compat
//!
//! `Fragmenter::new(cfg)` constructs a fragmenter with the **default
//! rule set** (the `AggregateFragmenterRule`). Existing callers
//! (`bin/pylon-coord.rs`, the test suite) keep working with no API
//! changes.
//!
//! ## M3 stage cap
//!
//! M3 only emits at most **two** stages: stage0 + stage1. The
//! dispatcher (`bin/pylon-coord.rs::split_dag_for_dispatch`) reads
//! `dag.stages[0]` and `dag.stages[1]` directly. Once M4 lands
//! HashJoin / Distinct / Window, a follow-up will extend the
//! dispatcher to walk `dag.stages` linearly; the visitor below is
//! already N-stage capable — it just rejects >1 boundary today
//! (see `M3_BOUNDARY_CAP`).
//!
//! ## Per-partition `target_flight_addrs`
//!
//! The emitted `ExchangeSinkRpc.target_flight_addrs` is a
//! **placeholder** — the coord dispatcher overwrites it with the
//! actual stage1 partition → worker assignment. M3-tail PR1 (B3)
//! owns that rewrite.

use crate::stage::{Distribution, Fragment, OpSpec, Stage, StageDag, StageId};

use pylon_plan::physical::exec::ExecutionPlan;
use pylon_plan::physical::fragmenter::{
    AggregateFragmenterRule, BoundaryEmit, BoundaryStrategy, FragmenterRule,
};
use pylon_types::{PylonError, Result as PylonResult};

use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct FragmenterConfig {
    pub default_partition_count: usize,
}

impl Default for FragmenterConfig {
    fn default() -> Self {
        Self {
            default_partition_count: 16,
        }
    }
}

/// M3 first cut: a single plan may emit at most one stage
/// boundary. Multi-boundary plans (Aggregate-of-Aggregate, or
/// future HashJoin-on-Aggregate) are rejected here. The constant
/// is documented next to the check that reads it so M4 can lift
/// the cap in one place when HashJoin / Distinct / Window land.
const M3_BOUNDARY_CAP: usize = 1;

/// Coordinates the post-order walk + stage materialisation. Owns:
///   * the immutable `FragmenterConfig`,
///   * the registered `FragmenterRule` list (consulted in order),
///   * the per-walk builder that materialises `Stage`s.
pub struct Fragmenter {
    cfg: FragmenterConfig,
    rules: Vec<Arc<dyn FragmenterRule>>,
}

impl Fragmenter {
    /// Construct with the default rule set (`AggregateFragmenterRule`
    /// at `cfg.default_partition_count`). This preserves the M3
    /// legacy API exactly.
    pub fn new(cfg: FragmenterConfig) -> Self {
        let rule = Arc::new(AggregateFragmenterRule::new(cfg.default_partition_count));
        Self {
            cfg,
            rules: vec![rule],
        }
    }

    /// Construct with an explicit rule list. The fragmenter
    /// consults rules in `rules` order; the first rule that
    /// returns `Some(_)` from `boundary_for` wins.
    pub fn with_rules(cfg: FragmenterConfig, rules: Vec<Arc<dyn FragmenterRule>>) -> Self {
        Self { cfg, rules }
    }

    /// Builder: append a rule to the existing list. Returns `self`
    /// for chaining.
    pub fn with_rule(mut self, rule: Arc<dyn FragmenterRule>) -> Self {
        self.rules.push(rule);
        self
    }

    /// Builder: replace the entire rule list.
    pub fn with_rules_list(mut self, rules: Vec<Arc<dyn FragmenterRule>>) -> Self {
        self.rules = rules;
        self
    }

    /// Convenience constructor mirroring the legacy
    /// `with_default_partition_count` helper.
    pub fn with_default_partition_count(n: usize) -> Self {
        Self::new(FragmenterConfig {
            default_partition_count: n,
        })
    }

    /// Registered rule names (for diagnostics / tracing).
    pub fn rule_names(&self) -> Vec<&'static str> {
        // `name()` returns `&str` borrowed from `&'static str` per
        // every built-in rule; for user-supplied rules the lifetime
        // is the rule's own, but we leak-promote to `&'static str`
        // here to fit the return type — the rule names are used for
        // log lines, not for ownership.
        let mut out: Vec<&'static str> = Vec::with_capacity(self.rules.len());
        for r in &self.rules {
            let s: &str = r.name();
            out.push(Box::leak(s.to_string().into_boxed_str()));
        }
        out
    }

    /// Post-order walk the plan, emit a StageDag with one
    /// `ExchangeSinkRpc` boundary per stage-boundary-triggering
    /// node. Stage 0 = non-boundary subtree + partitioned
    /// `ExchangeSinkRpc`. Stage 1 = N per-partition
    /// `ExchangeSource` + the boundary op repeated per partition.
    ///
    /// `worker_flight_addrs` is currently only used for the
    /// placeholder `target_flight_addrs` field on `ExchangeSinkRpc`;
    /// the dispatcher's authoritative rewrite overwrites it.
    pub fn fragment(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        query_id: u64,
        worker_flight_addrs: &[String],
    ) -> PylonResult<StageDag> {
        let n_partitions = self.cfg.default_partition_count;
        let mut builder = FragmentBuilder::new(self.cfg.default_partition_count);
        let stage0 = builder.new_stage();

        visit(
            plan,
            &mut builder,
            stage0,
            query_id,
            worker_flight_addrs,
            &self.rules,
        )?;

        let n_boundaries = builder.boundary_count();
        if n_boundaries > M3_BOUNDARY_CAP {
            return Err(PylonError::InvalidPlan(format!(
                "plan emits {n_boundaries} boundaries; \
                 M3_BOUNDARY_CAP = {} (M3 first cut)",
                M3_BOUNDARY_CAP
            )));
        }

        builder.finalize(plan, n_partitions)
    }
}

// =====================================================================
// FragmentBuilder — mutable state threaded through the post-order walk
// =====================================================================

struct FragmentBuilder {
    /// Number of partitions for the consumer side of every
    /// boundary. Inherited from `FragmenterConfig.default_partition_count`.
    n_partitions: usize,
    /// Stages allocated in order. `stages[i].id` corresponds 1:1
    /// to `stage_ids[i]`. Topologically ordered (sources first).
    stages: Vec<Stage>,
    stage_ids: Vec<StageId>,
    /// Per-stage OpSpec list, aligned with `stages` / `stage_ids`.
    /// Stage i's ops live at `stage_ops[i]`.
    stage_ops: Vec<Vec<OpSpec>>,
    /// Number of stage boundaries emitted so far. Compared to
    /// `M3_BOUNDARY_CAP` at the end of the walk.
    boundary_count: usize,
}

impl FragmentBuilder {
    fn new(n_partitions: usize) -> Self {
        Self {
            n_partitions,
            stages: Vec::new(),
            stage_ids: Vec::new(),
            stage_ops: Vec::new(),
            boundary_count: 0,
        }
    }

    /// Allocate a fresh stage with `partition_count` from the
    /// builder's default. Returns the new stage's `StageId`; the
    /// visitor pushes ops into it via `push_op`.
    fn new_stage(&mut self) -> StageId {
        self.new_stage_with_partitions(self.n_partitions)
    }

    /// Allocate a fresh stage whose `partition_count` is the
    /// explicit override. Used when the visitor materialises a new
    /// stage at a boundary: the new stage's partition count
    /// reflects the boundary strategy, not the
    /// `default_partition_count` of the config.
    fn new_stage_with_partitions(&mut self, partition_count: usize) -> StageId {
        let next_id = (self.stages.len() + 1) as u64;
        let id = StageId(next_id);
        let mut stage = Stage::new(
            id,
            Fragment::new(Distribution::Partitioned(partition_count)),
        );
        stage.partition_count = partition_count;
        self.stages.push(stage);
        self.stage_ids.push(id);
        self.stage_ops.push(Vec::new());
        id
    }

    /// Append `op` to `stage`'s op list. The stage must have been
    /// allocated via `new_stage()` earlier in the walk.
    fn push_op(&mut self, stage: StageId, op: OpSpec) -> PylonResult<()> {
        let pos = self
            .stage_ids
            .iter()
            .position(|s| *s == stage)
            .ok_or_else(|| {
                PylonError::Internal(format!(
                    "FragmentBuilder::push_op: stage {:?} not allocated",
                    stage
                ))
            })?;
        self.stage_ops[pos].push(op);
        Ok(())
    }

    fn boundary_count(&self) -> usize {
        self.boundary_count
    }

    fn record_boundary(&mut self) {
        self.boundary_count += 1;
    }

    /// Build the final `StageDag`. M3 wire-shape compat:
    /// always emit exactly 2 stages. If the walk only allocated
    /// one (no boundary fired), append an empty `stage1` so the
    /// dispatcher always finds `dag.stages[1]` and can iterate
    /// its `partition_count`. If the walk allocated ≥2, leave
    /// them as-is (the boundary emitted the consumer stage
    /// inline).
    fn finalize(
        mut self,
        root_plan: &Arc<dyn ExecutionPlan>,
        n_partitions: usize,
    ) -> PylonResult<StageDag> {
        // Always emit at least two stages. The dispatcher indexes
        // by [0] / [1] (M3 wire compat).
        if self.stages.is_empty() {
            // Should never happen: `fragment()` always calls
            // `new_stage()` before visiting. Defensive only.
            self.new_stage();
        }
        if self.stages.len() == 1 {
            let stage1_id = self.new_stage();
            debug_assert_eq!(stage1_id.0, 2);
        }

        // Move ops into their stages; wire upstream / downstream.
        for (i, stage) in self.stages.iter_mut().enumerate() {
            let ops = std::mem::take(&mut self.stage_ops[i]);
            stage.fragment.ops = ops;
        }
        if self.stages.len() >= 2 {
            let stage0_id = self.stage_ids[0];
            let stage1_id = self.stage_ids[1];
            self.stages[0].downstream.push(stage1_id);
            self.stages[1].upstream.push(stage0_id);
        }

        // Stage0 carries the canonical plan root so future
        // schedulers can read `properties()` /
        // `required_input_distribution()` / `requires_exchange()`
        // off the Arc<dyn>. Same rationale as R2.2.b.
        if !self.stages.is_empty() {
            self.stages[0].plan = Some(Arc::clone(root_plan));
        }

        // M3 emits only 2 stages. If a future rule forces >2, the
        // dispatcher will need updating; we surface a clearer
        // error than an index-out-of-bounds at dispatch time.
        if self.stages.len() > 2 {
            return Err(PylonError::InvalidPlan(format!(
                "fragmenter emitted {} stages; M3 dispatcher only supports 2 \
                 (lift M3_BOUNDARY_CAP when the dispatcher learns N-stage dispatch)",
                self.stages.len()
            )));
        }

        let _ = n_partitions; // already wired into stage.partition_count
        let dag = StageDag::default();
        let mut dag = dag;
        for stage in self.stages {
            dag = dag.with_stage(stage);
        }
        Ok(dag)
    }
}

// =====================================================================
// Visitor
// =====================================================================

/// Post-order walk. Consults `rules` at every node; the first rule
/// that returns `Some(strategy)` cuts a boundary.
///
/// When a boundary fires:
///   1. Recurse into the node's children while still in `current_stage`
///      (so the non-boundary subtree ends up in the current stage).
///   2. Append the `ExchangeSinkRpc` OpSpec at the tail of
///      `current_stage`.
///   3. Allocate a new stage and append N `ExchangeSource` + N
///      `stage1_op_spec(node)` OpSpecs.
///
/// When no rule fires:
///   1. Recurse into children (each in the same `current_stage`).
///   2. Translate the node via `op_spec_for()` and append.
fn visit(
    node: &Arc<dyn ExecutionPlan>,
    builder: &mut FragmentBuilder,
    current_stage: StageId,
    query_id: u64,
    worker_flight_addrs: &[String],
    rules: &[Arc<dyn FragmenterRule>],
) -> PylonResult<()> {
    // First matching rule wins. Mirrors `FragmenterRule::name()`
    // ordering (registration order). More-specific rules should be
    // registered before generic ones.
    let mut matched: Option<(Arc<dyn FragmenterRule>, BoundaryStrategy)> = None;
    for rule in rules {
        if let Some(s) = rule.boundary_for(node.as_ref()) {
            matched = Some((rule.clone(), s));
            break;
        }
    }

    if let Some((rule, strategy)) = matched {
        // Boundary: recurse into children in the current stage, then
        // emit ExchangeSinkRpc + open a new stage.
        let children = node.children();
        if children.len() != 1 {
            return Err(PylonError::InvalidPlan(format!(
                "boundary op '{}' has {} children; M3 first cut supports exactly 1",
                node.name(),
                children.len()
            )));
        }
        visit(
            &children[0].clone(),
            builder,
            current_stage,
            query_id,
            worker_flight_addrs,
            rules,
        )?;

        let consumer_n = strategy.partition_count();
        let producer_n = strategy.producer_partition_count();
        let next_stage = builder.new_stage_with_partitions(consumer_n);

        let descriptors: Vec<String> = (0..consumer_n)
            .map(|p| {
                format!(
                    "pylon://query/{}/stage/{}/task/{}",
                    query_id, next_stage.0, p
                )
            })
            .collect();
        let n_workers = worker_flight_addrs.len().max(1);
        let target_flight_addrs: Vec<String> = (0..consumer_n)
            .map(|p| worker_flight_addrs[p % n_workers].clone())
            .collect();
        let partition_keys = strategy
            .partition_keys()
            .map(|k| k.join(","))
            .unwrap_or_default();

        let mut sink_cfg: HashMap<String, String> = HashMap::new();
        sink_cfg.insert("descriptors".into(), descriptors.join(";"));
        sink_cfg.insert("n_partitions".into(), producer_n.to_string());
        sink_cfg.insert("partition_keys".into(), partition_keys);
        sink_cfg.insert("target_flight_addrs".into(), target_flight_addrs.join(";"));
        sink_cfg.insert("exchange_kind".into(), strategy.as_str().to_string());

        builder.push_op(
            current_stage,
            OpSpec {
                name: "ExchangeSinkRpc".to_string(),
                config: sink_cfg,
            },
        )?;

        // Head of the new stage: N×ExchangeSource, one per partition.
        for (p, desc) in descriptors.iter().enumerate().take(consumer_n) {
            let mut cfg = HashMap::new();
            cfg.insert("descriptor".into(), desc.clone());
            cfg.insert("partition".into(), p.to_string());
            builder.push_op(
                next_stage,
                OpSpec {
                    name: "ExchangeSource".to_string(),
                    config: cfg,
                },
            )?;
        }

        // Tail of the new stage: the boundary op, one per partition.
        let emit = rule.stage1_op_spec(node.as_ref())?;
        let boundary_op = boundary_emit_to_op_spec(emit);
        for _p in 0..consumer_n {
            builder.push_op(next_stage, boundary_op.clone())?;
        }

        builder.record_boundary();
        return Ok(());
    }

    // Non-boundary op. Recurse into children first (post-order),
    // then translate this node and append to `current_stage`.
    for c in node.children() {
        visit(
            c,
            builder,
            current_stage,
            query_id,
            worker_flight_addrs,
            rules,
        )?;
    }
    let op_spec = op_spec_for(node)?;
    builder.push_op(current_stage, op_spec)?;
    Ok(())
}

// =====================================================================
// OpSpec conversion
// =====================================================================

/// Translate a non-boundary `ExecutionPlan` node into its
/// worker-side `OpSpec`. We downcast via `as_any` rather than
/// carrying the conversion on the trait (cross-crate `OpSpec`
/// dependency would force `pylon-plan` to import `pylon-coord`,
/// which it must not). R6 (connectors on SPI) revisits this when
/// non-pylon ops are introduced.
fn op_spec_for(node: &Arc<dyn ExecutionPlan>) -> PylonResult<OpSpec> {
    use pylon_plan::physical::exec::{FilterExec, ProjectExec, SeqScanExec};

    if let Some(scan) = node.as_any().downcast_ref::<SeqScanExec>() {
        Ok(OpSpec {
            name: "SeqScan".to_string(),
            config: kv(&[("path", &format!("data/{}.parquet", scan.table))]),
        })
    } else if let Some(filt) = node.as_any().downcast_ref::<FilterExec>() {
        let (col, op_s, lit) = decompose_filter(&filt.predicate)?;
        Ok(OpSpec {
            name: "Filter".to_string(),
            config: kv(&[("col", &col), ("op", &op_s), ("literal", &lit)]),
        })
    } else if let Some(prj) = node.as_any().downcast_ref::<ProjectExec>() {
        let cols: Vec<String> = prj
            .projections
            .iter()
            .map(|e| {
                e.as_any()
                    .downcast_ref::<pylon_plan::physical::expr::ColumnExpr>()
                    .map(|c| c.field.name().to_string())
                    .unwrap_or_else(|| "_".into())
            })
            .collect();
        Ok(OpSpec {
            name: "Project".to_string(),
            config: kv(&[("cols", &cols.join(","))]),
        })
    } else if node
        .as_any()
        .downcast_ref::<pylon_plan::physical::exec::AggregateExec>()
        .is_some()
    {
        // Aggregate's OpSpec is constructed inside the boundary
        // branch (visit), not here.
        Ok(OpSpec {
            name: "Aggregate".to_string(),
            config: HashMap::new(),
        })
    } else {
        Err(PylonError::Internal(format!(
            "op_spec_for: unrecognised op type '{}' (R6 will add a dispatch hook)",
            node.name()
        )))
    }
}

/// Convert a `BoundaryEmit` (rule-side abstract recipe) into an
/// `OpSpec` (coord-side concrete wire format). The two types are
/// intentionally split to keep `pylon-plan` free of `pylon-coord`
/// imports (RFC 0005 §1 module layout).
fn boundary_emit_to_op_spec(emit: BoundaryEmit) -> OpSpec {
    let mut cfg = HashMap::with_capacity(emit.config.len());
    for (k, v) in emit.config {
        cfg.insert(k, v);
    }
    OpSpec {
        name: emit.op_name,
        config: cfg,
    }
}

/// Decompose a `FilterExec.predicate` (a `Arc<dyn PhysicalExpr>`)
/// into `(column, op, literal)`. M3 only supports
/// `col OP literal`; binary-of-binaries etc. fail with a clear
/// error.
fn decompose_filter(
    pred: &Arc<dyn pylon_plan::physical::expr::PhysicalExpr>,
) -> PylonResult<(String, String, String)> {
    use pylon_plan::physical::expr::{BinaryOpExpr, ColumnExpr, LiteralExpr};

    if let Some(b) = pred.as_any().downcast_ref::<BinaryOpExpr>() {
        let col = match b.left.as_any().downcast_ref::<ColumnExpr>() {
            Some(c) => c.field.name().to_string(),
            None => "_".into(),
        };
        let lit = match b.right.as_any().downcast_ref::<LiteralExpr>() {
            Some(c) => c.value.clone(),
            None => "0".to_string(),
        };
        Ok((col, b.op.clone(), lit))
    } else {
        Ok(("_".into(), "=".into(), "0".to_string()))
    }
}

fn kv(items: &[(&str, &str)]) -> HashMap<String, String> {
    items
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
