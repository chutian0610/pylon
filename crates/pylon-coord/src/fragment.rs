//! Fragmenter — PhysicalPlan → multi-stage StageDag.
//!
//! M3+: post-order walk with HashPartitionExchange injection. Each
//! stage-boundary-triggering node (Aggregate today; HashJoin /
//! Distinct / Window land as rules in the next pass) forces a
//! stage boundary: the child stage ends with a partitioned
//! `ExchangeSinkRpc` that hash-routes rows to N downstream
//! partitions, and a new stage begins with one `ExchangeSource`
//! per partition followed by the boundary op.
//!
//! Per-partition `target_flight_addrs` is a *placeholder* the
//! coord dispatcher overwrites at dispatch time with the actual
//! stage1 partition → worker assignment (see B3 in
//! `docs/roadmap/m3-tail-exchange-unify.md`).

use crate::stage::{Distribution, Fragment, OpSpec, Stage, StageDag, StageId};

use pylon_plan::physical::exec::ExecutionPlan;
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

pub struct Fragmenter {
    cfg: FragmenterConfig,
}

impl Fragmenter {
    pub fn new(cfg: FragmenterConfig) -> Self {
        Self { cfg }
    }

    pub fn with_default_partition_count(n: usize) -> Self {
        Self {
            cfg: FragmenterConfig {
                default_partition_count: n,
            },
        }
    }

    /// Post-order walk the plan, emit a StageDag with one
    /// ExchangeSinkRpc boundary per stage-boundary-triggering node
    /// (Aggregate today). Stage 0 = scan/filter/project +
    /// partitioned `ExchangeSinkRpc`. Stage 1 = per-partition
    /// `ExchangeSource` + Aggregate.
    ///
    /// The emitted `ExchangeSinkRpc.target_flight_addrs` is a
    /// **placeholder** — the coord dispatcher overwrites it with
    /// the actual stage1 partition → worker assignment. PR1 keeps
    /// the round-robin default so the placeholder is reasonable;
    /// PR1 deletes the in-process `ExchangeSink`/`ExchangeSinkOp`
    /// short-circuit entirely.
    ///
    /// `worker_flight_addrs` is currently only used for that
    /// placeholder and may be removed once the dispatcher's
    /// authoritative rewrite is universally in place.
    pub fn fragment(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        query_id: u64,
        worker_flight_addrs: &[String],
    ) -> PylonResult<StageDag> {
        let n_partitions = self.cfg.default_partition_count;
        let stage0_id = StageId(1);
        let stage1_id = StageId(2);
        let mut ctx = FragmentCtx {
            query_id,
            n_partitions,
            stage0_id,
            stage1_id,
        };
        // R2.3: callers pass `Arc<dyn ExecutionPlan>` directly —
        // no wrap, no enum match. The dispatcher's authoritative
        // rewrite (PR1) consumes the trait object directly.
        let visit = visit_v2(plan, &mut ctx, stage0_id, worker_flight_addrs)?;

        // R2.2.b: stage0 carries the canonical plan root so future
        // schedulers (M4 cost-based, hash-affinity) can read
        // `properties()` / `required_input_distribution()` /
        // `requires_exchange()` off the Arc<dyn> rather than
        // re-deriving from `OpSpec`. Stage1's plan tree would be
        // the (currently-not-materialized) post-aggregate subtree;
        // left as `None` for M3 — populated when R3 introduces
        // explicit stages per OpSpec boundary. Today's
        // `CapacityScheduler::assign` only reads `partition_count`
        // + `fragment.ops`, so this change is wire-stable.
        let stage0 = Stage {
            id: stage0_id,
            fragment: Fragment {
                ops: visit.stage0_ops,
                distribution: Distribution::Partitioned(n_partitions),
            },
            plan: Some(Arc::clone(plan)),
            partition_count: n_partitions,
            memory_budget_bytes: 256 * 1024 * 1024,
            upstream: Vec::new(),
            downstream: vec![stage1_id],
        };
        // M3 B-3: stage1 is split into N per-partition tasks at
        // dispatch time. We expose the flat ops + partition count
        // via a public method; here we still build a single Stage
        // (coord splits when dispatching).
        let stage1 = Stage {
            id: stage1_id,
            fragment: Fragment {
                ops: visit.stage1_ops,
                distribution: Distribution::Partitioned(n_partitions),
            },
            plan: None,
            partition_count: n_partitions,
            memory_budget_bytes: 256 * 1024 * 1024,
            upstream: vec![stage0_id],
            downstream: Vec::new(),
        };
        Ok(StageDag::new().with_stage(stage0).with_stage(stage1))
    }
}

/// Mutable context threaded through the post-order walk.
struct FragmentCtx {
    query_id: u64,
    n_partitions: usize,
    stage0_id: StageId,
    stage1_id: StageId,
}

/// Result of a post-order walk. `stage0_ops` and `stage1_ops` collect
/// OpSpecs for each stage. We allocate ops into stages at boundary
/// time (when an Aggregate is seen).
struct Visit {
    stage0_ops: Vec<OpSpec>,
    stage1_ops: Vec<OpSpec>,
}


/// RFC 0005 R2.2.a: trait-driven walker. Replaces `visit_plan` (the
/// legacy enum match) for any caller that has already wrapped its
/// input. The output `Visit` shape is unchanged.
fn visit_v2(
    node: &Arc<dyn pylon_plan::physical::exec::ExecutionPlan>,
    ctx: &mut FragmentCtx,
    current_stage: StageId,
    worker_flight_addrs: &[String],
) -> PylonResult<Visit> {
    use pylon_plan::physical::exec::{
        AggregateExec, ExecutionPlan, FilterExec, ProjectExec, SeqScanExec,
    };

    // Stage-boundary rule (R3-pre): every `requires_exchange() == true`
    // node cuts a stage. M3 only Aggregate marks it; the legacy
    // fragmenter used the same rule.
    if node.requires_exchange() {
        if current_stage != ctx.stage0_id {
            return Err(PylonError::InvalidPlan(
                "nested Aggregate not supported in M3 first cut fragmenter".into(),
            ));
        }
        // Capture the typed aggregate fields BEFORE recursing into
        // the child (downcast only works on `&Arc<dyn>`, not on the
        // child which is also `&Arc<dyn>`).
        let agg = node
            .as_any()
            .downcast_ref::<AggregateExec>()
            .ok_or_else(|| {
                PylonError::Internal(
                    "visit_v2: requires_exchange=true but node is not AggregateExec".into(),
                )
            })?;
        let group_cols: Vec<String> = agg
            .group_by
            .iter()
            .map(|e| match e.as_any().downcast_ref::<pylon_plan::physical::expr::ColumnExpr>() {
                Some(c) => c.field.name().to_string(),
                None => "_".into(),
            })
            .collect();
        let agg_specs: Vec<String> = agg.aggs.iter().map(agg_spec_to_string_v2).collect();

        // Recurse into the child while we still have the typed
        // AggregateExec fields in scope.
        let mut child = visit_v2(&agg.input, ctx, current_stage, worker_flight_addrs)?;

        // 1. Tail of stage0: partitioned ExchangeSinkRpc.
        let n = ctx.n_partitions;
        let descriptors: Vec<String> = (0..n)
            .map(|p| {
                format!(
                    "pylon://query/{}/stage/{}/task/{}",
                    ctx.query_id,
                    ctx.stage1_id.0,
                    p
                )
            })
            .collect();
        let n_workers = worker_flight_addrs.len().max(1);
        let target_flight_addrs: Vec<String> = (0..n)
            .map(|p| worker_flight_addrs[p % n_workers].clone())
            .collect();
        child.stage0_ops.push(OpSpec {
            name: "ExchangeSinkRpc".to_string(),
            config: kv(&[
                ("descriptors", &descriptors.join(";")),
                ("n_partitions", &n.to_string()),
                ("partition_keys", &group_cols.join(",")),
                ("target_flight_addrs", &target_flight_addrs.join(";")),
            ]),
        });

        // 2. Head of stage1: N ExchangeSource ops, one per partition.
        for p in 0..n {
            let desc = format!(
                "pylon://query/{}/stage/{}/task/{}",
                ctx.query_id,
                ctx.stage1_id.0,
                p
            );
            child.stage1_ops.push(OpSpec {
                name: "ExchangeSource".to_string(),
                config: kv(&[("descriptor", &desc)]),
            });
        }
        // 3. Tail of stage1: the Aggregate op (one per partition).
        for _p in 0..n {
            child.stage1_ops.push(OpSpec {
                name: "Aggregate".to_string(),
                config: kv(&[
                    ("group_by_cols", &group_cols.join(",")),
                    ("agg_specs", &agg_specs.join(";")),
                ]),
            });
        }
        return Ok(child);
    }

    // Non-boundary op. Recurse into children; if any, the first
    // child's visit is the carrier (all children live in the same
    // stage in M3, so we just collapse their Visit into one carrier).
    // A leaf (no children) starts with an empty Visit placed in
    // the current stage.
    let child_visits: Vec<Visit> = node
        .children()
        .into_iter()
        .map(|c| visit_v2(c, ctx, current_stage, worker_flight_addrs))
        .collect::<PylonResult<Vec<_>>>()?;

    let mut visit = child_visits
        .into_iter()
        .next()
        .unwrap_or_else(|| Visit {
            stage0_ops: Vec::new(),
            stage1_ops: Vec::new(),
        });
    let op_spec = op_spec_for(node)?;
    push_op(&mut visit, current_stage, op_spec);
    Ok(visit)
}

/// Translate a non-boundary `ExecutionPlan` node into its
/// worker-side `OpSpec`. We downcast via `as_any` rather than
/// carrying the conversion on the trait (cross-crate `OpSpec`
/// dependency would force `pylon-plan` to import `pylon-coord`,
/// which it must not). R6 (connectors on SPI) revisits this when
/// non-pylon ops are introduced.
fn op_spec_for(
    node: &Arc<dyn pylon_plan::physical::exec::ExecutionPlan>,
) -> PylonResult<OpSpec> {
    use pylon_plan::physical::exec::{
        FilterExec, ProjectExec, SeqScanExec,
    };
    if let Some(scan) = node.as_any().downcast_ref::<SeqScanExec>() {
        Ok(OpSpec {
            name: "SeqScan".to_string(),
            config: kv(&[("path", &format!("data/{}.parquet", scan.table))]),
        })
    } else if let Some(filt) = node.as_any().downcast_ref::<FilterExec>() {
        let (col, op_s, lit) = decompose_filter_v2(&filt.predicate)?;
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
    } else if let Some(_agg) = node.as_any().downcast_ref::<
        pylon_plan::physical::exec::AggregateExec,
    >() {
        // Aggregate's OpSpec is constructed inside the boundary
        // branch (visit_v2), not here.
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

/// Decompose a `FilterExec.predicate` (a `Arc<dyn PhysicalExpr>`) into
/// (column, op, literal). R2.2.a only supports `col OP literal`;
/// binary-of-binaries etc. fail with a clear error.
fn decompose_filter_v2(
    pred: &Arc<dyn pylon_plan::physical::expr::PhysicalExpr>,
) -> PylonResult<(String, String, String)> {
    use pylon_plan::physical::expr::{
        BinaryOpExpr, ColumnExpr, LiteralExpr,
    };
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

/// Format an `Arc<dyn PhysicalExpr>` that's known to be an
/// `AggregateFunctionExpr` for worker-side OpSpec config. Matches
/// the legacy `agg_spec_to_string` output byte-for-byte.
fn agg_spec_to_string_v2(
    e: &Arc<dyn pylon_plan::physical::expr::PhysicalExpr>,
) -> String {
    use pylon_plan::physical::expr::{
        AggregateFunctionExpr, ColumnExpr,
    };
    if let Some(a) = e.as_any().downcast_ref::<AggregateFunctionExpr>() {
        if a.func == "count" && a.args.is_empty() {
            "count()".into()
        } else {
            let arg = match a.args.first() {
                Some(c) => match c.as_any().downcast_ref::<ColumnExpr>() {
                    Some(cc) => cc.field.name().to_string(),
                    None => "*".into(),
                },
                None => "*".into(),
            };
            format!("{}:{}", a.name, arg)
        }
    } else {
        "?".into()
    }
}

/// Push `op` into the appropriate stage's OpSpec list.
fn push_op(visit: &mut Visit, current_stage: StageId, op: OpSpec) {
    if current_stage.0 == 1 {
        visit.stage0_ops.push(op);
    } else {
        visit.stage1_ops.push(op);
    }
}

/// Construct a Visit with `op` placed in the current stage only.
fn op_only_in(current_stage: StageId, ctx: &FragmentCtx, op: OpSpec) -> Visit {
    let mut v = Visit {
        stage0_ops: Vec::new(),
        stage1_ops: Vec::new(),
    };
    if current_stage == ctx.stage0_id {
        v.stage0_ops.push(op);
    } else {
        v.stage1_ops.push(op);
    }
    v
}

fn kv(items: &[(&str, &str)]) -> HashMap<String, String> {
    items.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[allow(dead_code)]
pub fn _unused_marker(_e: PylonError) {}


