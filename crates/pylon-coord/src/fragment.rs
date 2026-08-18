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

use pylon_plan::physical::physical_expr::PhysicalExpr;
use pylon_plan::physical::PhysicalPlan;
use pylon_types::{PylonError, Result as PylonResult};
use std::collections::HashMap;

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
        plan: &PhysicalPlan,
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
        let visit = visit_plan(plan, &mut ctx, stage0_id, worker_flight_addrs)?;

        let stage0 = Stage {
            id: stage0_id,
            fragment: Fragment {
                ops: visit.stage0_ops,
                distribution: Distribution::Partitioned(n_partitions),
            },
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

/// Walk the plan post-order. Returns the OpSpec lists to put on
/// stage0 and stage1. The walk inserts ExchangeSink/Source at every
/// Aggregate[groupBy] boundary.
fn visit_plan(plan: &PhysicalPlan, ctx: &mut FragmentCtx, current_stage: StageId, worker_flight_addrs: &[String]) -> PylonResult<Visit> {
    match plan {
        PhysicalPlan::SeqScan { table, schema } => {
            let op = OpSpec {
                name: "SeqScan".to_string(),
                config: kv(&[("path", &format!("data/{table}.parquet"))]),
            };
            Ok(op_only_in(current_stage, ctx, op))
        }
        PhysicalPlan::Filter { input, predicate } => {
            let mut child = visit_plan(input, ctx, current_stage, worker_flight_addrs)?;
            let (col, op_s, lit) = decompose_filter(predicate)?;
            let op = OpSpec {
                name: "Filter".to_string(),
                config: kv(&[("col", &col), ("op", &op_s), ("literal", &lit)]),
            };
            push_op(&mut child, current_stage, op);
            Ok(child)
        }
        PhysicalPlan::Project { input, projections, schema: _ } => {
            let mut child = visit_plan(input, ctx, current_stage, worker_flight_addrs)?;
            let cols: Vec<String> = projections
                .iter()
                .map(|p| match p {
                    PhysicalExpr::Column { field, .. } => field.name().clone(),
                    _ => "_".into(),
                })
                .collect();
            let op = OpSpec {
                name: "Project".to_string(),
                config: kv(&[("cols", &cols.join(","))]),
            };
            push_op(&mut child, current_stage, op);
            Ok(child)
        }
        PhysicalPlan::Aggregate {
            input,
            group_by,
            aggs,
            schema,
        } => {
            // A2-1 / B-2 rule: any Aggregate forces a stage boundary
            // in M3 first cut. The child runs in the current stage;
            // the Aggregate lives in the next stage. Between them is
            // a partitioned Exchange.
            //
            // In M3 first cut, the child's stage is always stage0
            // (we don't recurse past an Aggregate — there's only one
            // boundary per query). The next stage is stage1.
            let mut child = visit_plan(input, ctx, current_stage, worker_flight_addrs)?;
            if current_stage != ctx.stage0_id {
                return Err(PylonError::InvalidPlan(
                    "nested Aggregate not supported in M3 first cut fragmenter".into(),
                ));
            }

            let group_cols: Vec<String> = group_by
                .iter()
                .map(|e| match e {
                    PhysicalExpr::Column { field, .. } => field.name().clone(),
                    _ => "_".into(),
                })
                .collect();
            let agg_specs: Vec<String> = aggs.iter().map(agg_spec_to_string).collect();

            // 1. Tail of stage0: partitioned ExchangeSink[Rpc].
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

            // Per-partition flight_addr list is a placeholder —
            // the coord dispatcher overwrites it at dispatch time
            // with the actual stage1 partition → worker assignment.
            // A round-robin guess keyed off `worker_flight_addrs`
            // keeps the placeholder reasonable; same-worker
            // partitions naturally resolve to a loopback gRPC
            // target (the same worker's `FlightServerImpl`).
            let n_workers = worker_flight_addrs.len().max(1);
            let target_flight_addrs: Vec<String> = (0..n)
                .map(|p| worker_flight_addrs[p % n_workers].clone())
                .collect();
            let op_name = "ExchangeSinkRpc".to_string();
            let sink_config = kv(&[
                ("descriptors", &descriptors.join(";")),
                ("n_partitions", &n.to_string()),
                ("partition_keys", &group_cols.join(",")),
                ("target_flight_addrs", &target_flight_addrs.join(";")),
            ]);
            child.stage0_ops.push(OpSpec {
                name: op_name,
                config: sink_config,
            });

            // 2. Head of stage1: N ExchangeSource ops, one per
            //    partition. Layout: per-partition pair
            //    [ExchangeSource, Aggregate], so the coord can split
            //    by walking 2 ops at a time.
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
            // Stage 1 doesn't know its post-aggregate schema in the
            // OpSpec config (M3 A1-4 — the op derives it on the
            // first batch).
            let _ = schema; // silence unused warning
            Ok(child)
        }
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

fn decompose_filter(p: &PhysicalExpr) -> PylonResult<(String, String, String)> {
    use pylon_plan::physical::physical_expr::PhysicalExpr as PE;
    Ok(match p {
        PE::BinaryOp { left, op, right, .. } => {
            let col = match left.as_ref() {
                PE::Column { field, .. } => field.name().clone(),
                _ => "_".into(),
            };
            let lit = match right.as_ref() {
                PE::Literal { value, .. } => value.clone(),
                _ => "0".to_string(),
            };
            (col, op.clone(), lit)
        }
        _ => ("_".to_string(), "=".to_string(), "0".to_string()),
    })
}

/// Format a PhysicalExpr::AggregateFunction as a worker-readable
/// string like `count()` or `sum:amount` or `min:id` or `max:id`.
fn agg_spec_to_string(e: &PhysicalExpr) -> String {
    use pylon_plan::physical::physical_expr::PhysicalExpr as PE;
    match e {
        PE::AggregateFunction { name, args, .. } => {
            if name == "count" && args.is_empty() {
                "count()".into()
            } else {
                let arg = match args.first() {
                    Some(PE::Column { field, .. }) => field.name().clone(),
                    _ => "*".into(),
                };
                format!("{name}:{arg}")
            }
        }
        _ => "?".into(),
    }
}

#[allow(dead_code)]
pub fn _unused_marker(_e: PylonError) {}


