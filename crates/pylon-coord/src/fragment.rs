//! Fragmenter — PhysicalPlan → multi-stage StageDag.
//!
//! M3 first cut: produces a 2-stage DAG (Stage 0 = scan/filters/sink, Stage 1
//! = exchange-source/project) for any SELECT. Future work (M4+) adds real
//! HashPartitionExchange injection based on operator hints.

use crate::stage::{Distribution, Fragment, OpSpec, Stage, StageDag, StageId};
use pylon_plan::physical::physical_expr::PhysicalExpr;
use pylon_plan::physical::PhysicalPlan;
use pylon_types::PylonError;
use pylon_types::Result as PylonResult;
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

    /// M3 multi-stage: 2 stages for any query.
    ///   Stage 0: scan + filter + PartitionFilter + ExchangeSink
    ///   Stage 1: ExchangeSource + Project (final gather/single task)
    pub fn fragment_multi_stage(
        &self,
        plan: &PhysicalPlan,
        query_id: u64,
    ) -> PylonResult<StageDag> {
        let mut stage0_ops = Vec::new();
        let mut stage1_ops = Vec::new();
        split_into_two_stages(plan, &mut stage0_ops, &mut stage1_ops)?;

        let stage0_id = StageId(1);
        let stage1_id = StageId(2);

        let stage0_fragment = Fragment {
            ops: stage0_ops,
            distribution: Distribution::Partitioned(self.cfg.default_partition_count),
        };
        let stage1_fragment = Fragment {
            ops: stage1_ops,
            distribution: Distribution::Single,
        };

        let mut stage0 = Stage::new(stage0_id, stage0_fragment)
            .with_partition_count(self.cfg.default_partition_count);
        let mut stage1 = Stage::new(stage1_id, stage1_fragment);

        stage0.downstream = vec![stage1_id];
        stage1.upstream = vec![stage0_id];

        let qid = query_id;
        // Pin exchange descriptor keys: target_qid = qid, target_stage = 2 (stage1 always),
        // target_partition = 0 (single-partition Stage 1 task in M3 demo).
        let exchange_desc = format!(
            "pylon://query/{qid}/stage/2/task/0"
        );
        append_sink_with_desc(&mut stage0, &exchange_desc);
        append_source_with_desc(&mut stage1, &exchange_desc);

        let mut dag = StageDag::new().with_stage(stage0).with_stage(stage1);
        // Topological order: sources first
        Ok(dag)
    }
}

fn kv(items: &[(&str, &str)]) -> HashMap<String, String> {
    items.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn append_sink_with_desc(stage: &mut Stage, desc: &str) {
    let mut config = HashMap::new();
    config.insert("descriptor".to_string(), desc.to_string());
    stage.fragment.ops.push(OpSpec {
        name: "ExchangeSink".to_string(),
        config,
    });
}

fn append_source_with_desc(stage: &mut Stage, desc: &str) {
    let mut config = HashMap::new();
    config.insert("descriptor".to_string(), desc.to_string());
    // Source goes at the BEGINNING of stage 1 ops
    stage.fragment.ops.insert(0, OpSpec {
        name: "ExchangeSource".to_string(),
        config,
    });
}

fn split_into_two_stages(
    plan: &PhysicalPlan,
    stage0: &mut Vec<OpSpec>,
    stage1: &mut Vec<OpSpec>,
) -> PylonResult<()> {
    // Walks the plan pre-order. Filters (and SeqScan before them) → stage 0.
    // Project goes to stage 1. After processing, fragmenter adds ExchangeSink
    // /ExchangeSource at the stage boundaries (see append_* above).
    match plan {
        PhysicalPlan::SeqScan { table, schema: _ } => {
            stage0.push(OpSpec {
                name: "SeqScan".to_string(),
                config: kv(&[("path", &format!("data/{table}.parquet"))]),
            });
            Ok(())
        }
        PhysicalPlan::Filter { input, predicate } => {
            split_into_two_stages(input, stage0, stage1)?;
            let (col, op_s, lit) = decompose_filter(predicate)?;
            stage0.push(OpSpec {
                name: "Filter".to_string(),
                config: kv(&[("col", &col), ("op", &op_s), ("literal", &lit)]),
            });
            Ok(())
        }
        PhysicalPlan::Project { input, projections, schema: _ } => {
            split_into_two_stages(input, stage0, stage1)?;
            let cols: Vec<String> = projections.iter()
                .map(|p| match p {
                    PhysicalExpr::Column { field, .. } => field.name().clone(),
                    _ => "_".into(),
                })
                .collect();
            stage1.push(OpSpec {
                name: "Project".to_string(),
                config: kv(&[("cols", &cols.join(","))]),
            });
            Ok(())
        }
    }
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

#[allow(dead_code)]
pub fn _unused_marker(_e: PylonError) {}
