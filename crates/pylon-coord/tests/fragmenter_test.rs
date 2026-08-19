//! A2-1 unit tests: Fragmenter post-order walk with HashPartitionExchange
//! injection at Aggregate boundaries.
//!
//! Post-R2.3: every plan fixture uses the new struct form
//! (`SeqScanExec`, `FilterExec`, `AggregateExec`, `ProjectExec`,
//! plus the `expr::*` structs). The traits are still in play —
//! the Fragmenter's internal walker uses trait methods, while
//! these fixtures use the concrete types for clarity.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use pylon_coord::fragment::{Fragmenter, FragmenterConfig};
use pylon_plan::physical::exec::{
    AggregateExec, ExecutionPlan, FilterExec, SeqScanExec,
};
use pylon_plan::physical::expr::{
    AggregateFunctionExpr, BinaryOpExpr, ColumnExpr, LiteralExpr, PhysicalExpr,
};

fn schema_two() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn agg_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("count", DataType::Int64, true),
    ]))
}

fn scan_only() -> Arc<dyn ExecutionPlan> {
    Arc::new(SeqScanExec::new("sample", schema_two()))
}

fn filter_scan() -> Arc<dyn ExecutionPlan> {
    let input = scan_only();
    let id_col: Arc<dyn PhysicalExpr> =
        Arc::new(ColumnExpr::new(0, input.schema().field(0).clone()));
    let five: Arc<dyn PhysicalExpr> = Arc::new(LiteralExpr::new("5", DataType::Utf8));
    let pred: Arc<dyn PhysicalExpr> = Arc::new(BinaryOpExpr::new(
        id_col,
        ">".to_string(),
        five,
    ));
    let schema = input.schema();
    Arc::new(FilterExec::new(input, pred, schema))
}

fn agg_count_by_name(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let group_by: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(ColumnExpr::new(
        1,
        input.schema().field(1).clone(),
    ))];
    let aggs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(AggregateFunctionExpr::new(
        "count",
        "count",
        vec![],
        DataType::Int64,
        vec![],
    ))];
    Arc::new(AggregateExec::new(input, group_by, aggs, agg_schema()))
}

fn op_names(dag: &pylon_coord::StageDag, stage_idx: usize) -> Vec<&str> {
    dag.stages[stage_idx]
        .fragment
        .ops
        .iter()
        .map(|o| o.name.as_str())
        .collect()
}

fn stage0_descriptors(dag: &pylon_coord::StageDag) -> Vec<String> {
    let sink = dag.stages[0]
        .fragment
        .ops
        .iter()
        .find(|o| o.name == "ExchangeSinkRpc");
    match sink {
        None => Vec::new(),
        Some(s) => s
            .config
            .get("descriptors")
            .map(|s| s.split(';').map(|p| p.to_string()).collect())
            .unwrap_or_default(),
    }
}

#[test]
fn plan_without_aggregate_has_no_boundary() {
    let plan = filter_scan();
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: 4,
    });
    let dag = f.fragment(&plan, 0, &["test_addr".to_string()]).unwrap();
    assert_eq!(dag.stages.len(), 2, "still 2 stages in the dag");
    assert_eq!(op_names(&dag, 0), vec!["SeqScan", "Filter"]);
    assert!(dag.stages[1].fragment.ops.is_empty());
}

#[test]
fn plan_with_aggregate_cuts_boundary_at_aggregate() {
    let plan = agg_count_by_name(filter_scan());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: 4,
    });
    let dag = f.fragment(&plan, 99, &["test_addr".to_string()]).unwrap();
    assert_eq!(op_names(&dag, 0), vec!["SeqScan", "Filter", "ExchangeSinkRpc"]);
    let s1_names = op_names(&dag, 1);
    assert_eq!(s1_names.len(), 8, "4 sources + 4 aggregates");
    assert_eq!(&s1_names[..4], &["ExchangeSource"; 4]);
    assert_eq!(&s1_names[4..], &["Aggregate"; 4]);
}

#[test]
fn aggregate_emits_n_partition_descriptors() {
    let n = 8;
    let plan = agg_count_by_name(scan_only());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: n,
    });
    let dag = f.fragment(&plan, 7, &["test_addr".to_string()]).unwrap();
    let descs = stage0_descriptors(&dag);
    assert_eq!(descs.len(), n);
    for (i, d) in descs.iter().enumerate() {
        assert_eq!(d, &format!("pylon://query/7/stage/2/task/{i}"));
    }
}

#[test]
fn aggregate_emits_n_exchange_sources_with_matching_descriptors() {
    let n = 3;
    let plan = agg_count_by_name(scan_only());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: n,
    });
    let dag = f.fragment(&plan, 11, &["test_addr".to_string()]).unwrap();
    let sources: Vec<&str> = dag.stages[1]
        .fragment
        .ops
        .iter()
        .filter(|o| o.name == "ExchangeSource")
        .map(|o| o.config.get("descriptor").map(|s| s.as_str()).unwrap_or(""))
        .collect();
    assert_eq!(sources.len(), n);
    for (i, d) in sources.iter().enumerate() {
        assert_eq!(*d, format!("pylon://query/11/stage/2/task/{i}").as_str());
    }
}

#[test]
fn aggregate_op_spec_carries_partition_keys_and_agg_specs() {
    let plan = agg_count_by_name(scan_only());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: 4,
    });
    let dag = f.fragment(&plan, 1, &["test_addr".to_string()]).unwrap();
    let agg = dag.stages[1]
        .fragment
        .ops
        .iter()
        .find(|o| o.name == "Aggregate")
        .expect("Aggregate op present");
    assert_eq!(agg.config.get("group_by_cols").map(|s| s.as_str()), Some("name"));
    assert_eq!(agg.config.get("agg_specs").map(|s| s.as_str()), Some("count()"));
}

#[test]
fn partitioned_sink_op_spec_carries_partition_keys_and_n_partitions() {
    let n = 5;
    let plan = agg_count_by_name(scan_only());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: n,
    });
    let dag = f.fragment(&plan, 1, &["test_addr".to_string()]).unwrap();
    let sink = dag.stages[0]
        .fragment
        .ops
        .iter()
        .find(|o| o.name == "ExchangeSinkRpc")
        .expect("ExchangeSink present");
    assert_eq!(
        sink.config.get("partition_keys").map(|s| s.as_str()),
        Some("name")
    );
    assert_eq!(
        sink.config.get("n_partitions").map(|s| s.as_str()),
        Some(&n.to_string()).map(|x| x.as_str())
    );
    let descs = sink.config.get("descriptors").unwrap();
    let n_descs = descs.split(';').count();
    assert_eq!(n_descs, n);
}

#[test]
fn descriptor_partition_assignment_is_consistent() {
    let n = 6;
    let plan = agg_count_by_name(scan_only());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: n,
    });
    let dag = f.fragment(&plan, 42, &["test_addr".to_string()]).unwrap();
    let sink_descs: Vec<String> = dag.stages[0]
        .fragment
        .ops
        .iter()
        .find(|o| o.name == "ExchangeSinkRpc")
        .unwrap()
        .config
        .get("descriptors")
        .unwrap()
        .split(';')
        .map(|s| s.to_string())
        .collect();
    let source_descs: Vec<String> = dag.stages[1]
        .fragment
        .ops
        .iter()
        .filter(|o| o.name == "ExchangeSource")
        .map(|o| o.config.get("descriptor").unwrap().clone())
        .collect();
    assert_eq!(sink_descs, source_descs);
}

#[test]
fn plan_with_aggregate_below_filter_still_cuts_once() {
    let plan = agg_count_by_name(filter_scan());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: 4,
    });
    let dag = f.fragment(&plan, 1, &["test_addr".to_string()]).unwrap();
    let n_sinks = dag.stages[0]
        .fragment
        .ops
        .iter()
        .filter(|o| o.name == "ExchangeSinkRpc")
        .count();
    let n_sources = dag.stages[1]
        .fragment
        .ops
        .iter()
        .filter(|o| o.name == "ExchangeSource")
        .count();
    let n_aggs = dag.stages[1]
        .fragment
        .ops
        .iter()
        .filter(|o| o.name == "Aggregate")
        .count();
    assert_eq!(n_sinks, 1);
    assert_eq!(n_sources, 4, "one source per partition");
    assert_eq!(n_aggs, 4, "one aggregate per partition");
}

#[test]
fn stage_partition_count_matches_config() {
    let n = 7;
    let plan = agg_count_by_name(scan_only());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: n,
    });
    let dag = f.fragment(&plan, 1, &["test_addr".to_string()]).unwrap();
    assert_eq!(dag.stages[0].partition_count, n);
    assert_eq!(dag.stages[1].partition_count, n);
}
