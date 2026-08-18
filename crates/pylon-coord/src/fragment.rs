//! Fragmenter — turns a PhysicalPlan into a StageDag (Trino-aligned).
//!
//! M2 simplification:
//!   Only handles SeqScan / Filter / Project — no JOIN, no Aggregates yet.
//!   The default pattern is: Scan in parallel partitions, optional Filter +
//!   Project fused into the same stage (no shuffle boundary).
//!
//! Rules for M2:
//!   - One stage per SeqScan; op chain = Scan→Filter→Project (if present).
//!   - If no Scan, emit a single empty stage (will error out at runtime).
//!
//! M3+ will add:
//!   - HashPartitionExchange insertions before HashJoinExec/HashAggregate
//!   - BroadcastExchange insertions before broadcast joins
//!   - GatherExchange insertions before final-stage aggregations

use pylon_plan::physical::PhysicalPlan;
use crate::stage::{
    Fragment, OpSpec, Stage, StageDag, StageId, Distribution, DEFAULT_PARTITION_COUNT,
};
use pylon_types::PylonError;
use pylon_types::Result as PylonResult;
use std::collections::HashMap;

/// Fragmenter configuration.
#[derive(Debug, Clone)]
pub struct FragmenterConfig {
    pub default_partition_count: usize,
}

impl Default for FragmenterConfig {
    fn default() -> Self {
        Self {
            default_partition_count: DEFAULT_PARTITION_COUNT,
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

    pub fn with_default_partition_count(default_partition_count: usize) -> Self {
        Self {
            cfg: FragmenterConfig { default_partition_count },
        }
    }

    /// Convert a physical plan into a StageDag.
    ///
    /// M2 simplification: the entire physical plan becomes a single Stage
    /// (partition_count = default_partition_count). Exchanges will be
    /// inserted by future M3 logic once JOINs/Aggregates arrive.
    pub fn fragment(&self, plan: &PhysicalPlan) -> PylonResult<StageDag> {
        let stage = self.physical_to_single_stage(plan)?;
        Ok(StageDag::new().with_stage(stage))
    }

    fn physical_to_single_stage(&self, plan: &PhysicalPlan) -> PylonResult<Stage> {
        // M2: collapse the whole plan into one stage with N partitions.
        let fragment = self.physical_to_fragment(plan)?;
        let id = StageId(unique_stage_id());
        Ok(Stage::new(id, fragment).with_partition_count(self.cfg.default_partition_count))
    }

    fn physical_to_fragment(&self, plan: &PhysicalPlan) -> PylonResult<Fragment> {
        let ops = self.collect_ops(plan)?;
        Ok(Fragment {
            ops,
            distribution: Distribution::Partitioned(self.cfg.default_partition_count),
        })
    }

    /// Walk the plan tree pre-order, producing OpSpec list.
    /// M2: simple cases — SeqScan + Filter + Project.
    fn collect_ops(&self, plan: &PhysicalPlan) -> PylonResult<Vec<OpSpec>> {
        let mut out = Vec::new();
        self.collect_ops_into(plan, &mut out)?;
        Ok(out)
    }

    fn collect_ops_into(&self, plan: &PhysicalPlan, out: &mut Vec<OpSpec>) -> PylonResult<()> {
        match plan {
            PhysicalPlan::SeqScan { table, schema: _ } => {
                out.push(OpSpec::new("SeqScan").with("table", table.clone()));
                Ok(())
            }
            PhysicalPlan::Filter { input, predicate } => {
                // Push input first; filter applies to the upstream output.
                self.collect_ops_into(input, out)?;
                let (col, op, lit) = decompose_filter(predicate)?;
                out.push(
                    OpSpec::new("Filter")
                        .with("col", col)
                        .with("op", op)
                        .with("literal", lit),
                );
                Ok(())
            }
            PhysicalPlan::Project { input, projections, schema: _ } => {
                self.collect_ops_into(input, out)?;
                let cols: Vec<String> = projections
                    .iter()
                    .map(|e| match e {
                        pylon_plan::physical::physical_expr::PhysicalExpr::Column { field, .. } => {
                            field.name().clone()
                        }
                        _ => "_".into(),
                    })
                    .collect();
                out.push(
                    OpSpec::new("Project")
                        .with("cols", cols.join(",")),
                );
                Ok(())
            }
        }
    }
}

fn decompose_filter(
    e: &pylon_plan::physical::physical_expr::PhysicalExpr,
) -> PylonResult<(String, String, String)> {
    use pylon_plan::physical::physical_expr::PhysicalExpr as PE;
    Ok(match e {
        PE::BinaryOp { left, op, right } => {
            let col = match left.as_ref() {
                PE::Column { field, .. } => field.name().clone(),
                _ => "_".into(),
            };
            let lit = match right.as_ref() {
                PE::Literal { value, .. } => value.clone(),
                _ => "0".into(),
            };
            (col, op.clone(), lit)
        }
        _ => ("_".into(), "=".into(), "0".into()),
    })
}

fn unique_stage_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

// HashMap is unused in M2; saved here so M3 has a starting import.
#[allow(dead_code)]
const _HM_DOC: fn() = || {
    let _: Option<HashMap<String, String>> = None;
};
#[allow(dead_code)]
fn _placeholder_marker() -> PylonError {
    PylonError::Internal("placeholder".into())
}
