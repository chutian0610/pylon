//! A2-1 unit tests: Fragmenter post-order walk with HashPartitionExchange
//! injection at Aggregate boundaries.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use pylon_coord::fragment::{Fragmenter, FragmenterConfig};
use pylon_plan::physical::physical_expr::PhysicalExpr;
use pylon_plan::physical::PhysicalPlan;

fn schema_two() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn scan_only() -> PhysicalPlan {
    PhysicalPlan::SeqScan {
        table: "sample".into(),
        schema: schema_two(),
    }
}

fn filter_scan() -> PhysicalPlan {
    PhysicalPlan::Filter {
        input: Box::new(scan_only()),
        predicate: PhysicalExpr::BinaryOp {
            left: Box::new(PhysicalExpr::Column {
                index: 0,
                field: Field::new("id", DataType::Int64, false),
            }),
            op: ">".into(),
            right: Box::new(PhysicalExpr::Literal {
                value: "5".into(),
                data_type: DataType::Utf8,
            }),
        },
    }
}

fn agg_count_by_name(input: PhysicalPlan) -> PhysicalPlan {
    let agg_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("count", DataType::Int64, true),
    ]));
    PhysicalPlan::Aggregate {
        input: Box::new(input),
        group_by: vec![PhysicalExpr::Column {
            index: 1,
            field: Field::new("name", DataType::Utf8, false),
        }],
        aggs: vec![PhysicalExpr::AggregateFunction {
            func: "count".into(),
            name: "count".into(),
            args: Vec::new(),
            data_type: DataType::Int64,
            input_data_types: Vec::new(),
        }],
        schema: agg_schema,
    }
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
    // The ExchangeSink in stage0 has a "descriptors" key in its config.
    // (If no ExchangeSink, this returns an empty vec.)
    let sink = dag.stages[0]
        .fragment
        .ops
        .iter()
        .find(|o| o.name == "ExchangeSink");
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
    let dag = f.fragment_multi_stage(&plan, 0).unwrap();
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
    let dag = f.fragment_multi_stage(&plan, 99).unwrap();
    // Stage 0: Scan, Filter, ExchangeSink
    assert_eq!(op_names(&dag, 0), vec!["SeqScan", "Filter", "ExchangeSink"]);
    // Stage 1: 4× (ExchangeSource + Aggregate) per partition.
    // Fragmenter layout: N sources first, then N aggregates, so
    // [ExchangeSource, ExchangeSource, ExchangeSource, ExchangeSource,
    //  Aggregate, Aggregate, Aggregate, Aggregate].
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
    let dag = f.fragment_multi_stage(&plan, 7).unwrap();
    let descs = stage0_descriptors(&dag);
    assert_eq!(descs.len(), n, "n descriptors for n partitions");
    // All descriptors are well-formed and have the right qid/stage/partition.
    for (i, d) in descs.iter().enumerate() {
        assert_eq!(
            d,
            &format!("pylon://query/7/stage/2/task/{i}"),
            "descriptor {i}"
        );
    }
}

#[test]
fn aggregate_emits_n_exchange_sources_with_matching_descriptors() {
    let n = 3;
    let plan = agg_count_by_name(scan_only());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: n,
    });
    let dag = f.fragment_multi_stage(&plan, 11).unwrap();
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
    let dag = f.fragment_multi_stage(&plan, 1).unwrap();
    let agg = dag.stages[1]
        .fragment
        .ops
        .iter()
        .find(|o| o.name == "Aggregate")
        .expect("Aggregate op present");
    assert_eq!(
        agg.config.get("group_by_cols").map(|s| s.as_str()),
        Some("name")
    );
    assert_eq!(
        agg.config.get("agg_specs").map(|s| s.as_str()),
        Some("count()")
    );
}

#[test]
fn partitioned_sink_op_spec_carries_partition_keys_and_n_partitions() {
    let n = 5;
    let plan = agg_count_by_name(scan_only());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: n,
    });
    let dag = f.fragment_multi_stage(&plan, 1).unwrap();
    let sink = dag.stages[0]
        .fragment
        .ops
        .iter()
        .find(|o| o.name == "ExchangeSink")
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
    // For an Aggregate, the partition chosen for a given group_key
    // value is determined by the hash function — but it must be the
    // SAME on both the sink side and the source side. The fragmenter
    // uses the descriptor index (i) to encode the partition, so the
    // sink's i-th descriptor and the source's i-th descriptor match.
    let n = 6;
    let plan = agg_count_by_name(scan_only());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: n,
    });
    let dag = f.fragment_multi_stage(&plan, 42).unwrap();
    let sink_descs: Vec<String> = dag.stages[0]
        .fragment
        .ops
        .iter()
        .find(|o| o.name == "ExchangeSink")
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
    // Even with Filter+Aggregate (no Project), exactly one boundary.
    let plan = agg_count_by_name(filter_scan());
    let f = Fragmenter::new(FragmenterConfig {
        default_partition_count: 4,
    });
    let dag = f.fragment_multi_stage(&plan, 1).unwrap();
    let n_sinks = dag.stages[0]
        .fragment
        .ops
        .iter()
        .filter(|o| o.name == "ExchangeSink")
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
    let dag = f.fragment_multi_stage(&plan, 1).unwrap();
    assert_eq!(dag.stages[0].partition_count, n);
    assert_eq!(dag.stages[1].partition_count, n);
}
