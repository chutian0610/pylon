//! Coord unit tests: Fragmenter + scheduler exercise on small
//! synthetic plans (`Arc<dyn ExecutionPlan>` post-R2.3).

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use pylon_coord::fragment::{Fragmenter, FragmenterConfig};
use pylon_plan::physical::exec::{
    AggregateExec, ExecutionPlan, FilterExec, SeqScanExec,
};
use pylon_plan::physical::expr::{
    BinaryOpExpr, ColumnExpr, LiteralExpr,
};

fn schema_two() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Build a `SeqScan` op typed via the new struct.
fn scan() -> Arc<dyn ExecutionPlan> {
    Arc::new(SeqScanExec::new("sample", schema_two()))
}

/// Build `Filter { input: scan, predicate: id > 5 }`.
fn filter_scan() -> Arc<dyn ExecutionPlan> {
    let input = scan();
    let id_col: Arc<dyn pylon_plan::physical::expr::PhysicalExpr> = Arc::new(
        ColumnExpr::new(0, input.schema().field(0).clone()),
    );
    let five: Arc<dyn pylon_plan::physical::expr::PhysicalExpr> = Arc::new(
        LiteralExpr::new("5", DataType::Utf8),
    );
    let pred: Arc<dyn pylon_plan::physical::expr::PhysicalExpr> = Arc::new(
        BinaryOpExpr::new(id_col, ">".to_string(), five),
    );
    let schema = input.schema();
    Arc::new(FilterExec::new(input, pred, schema))
}

#[test]
fn fragmenter_collapses_simple_plan_into_single_stage() {
    let plan = filter_scan();
    let fragmenter = Fragmenter::new(FragmenterConfig {
        default_partition_count: 16,
    });
    let dag = fragmenter
        .fragment(&plan, 0, &["test_addr".to_string()])
        .expect("fragmenter ok");

    assert_eq!(dag.stages[0].partition_count, 16);

    // A2-1: with no Aggregate in the plan, there is no stage
    // boundary — stage0 carries just the scan + filter. ExchangeSink
    // is only emitted at Aggregate boundaries.
    let ops = &dag.stages[0].fragment.ops;
    assert_eq!(ops.len(), 2, "SeqScan, Filter (no ExchangeSink without Aggregate)");
    assert_eq!(ops[0].name, "SeqScan");
    assert_eq!(ops[1].name, "Filter");
    assert_eq!(ops[1].config.get("col").map(|s| s.as_str()), Some("id"));
    assert_eq!(ops[1].config.get("op").map(|s| s.as_str()), Some(">"));
    assert_eq!(ops[1].config.get("literal").map(|s| s.as_str()), Some("5"));
    // Stage 1 should be empty (or near-empty) — no Aggregate means
    // no second stage in M3 first cut.
    assert!(dag.stages[1].fragment.ops.is_empty(), "stage1 empty without Aggregate");
}

#[test]
fn fragment_then_schedule_pipelines_through() {
    use pylon_coord::scheduler::{CapacityScheduler, Scheduler, WorkerAddr, WorkerCapacity};
    // Build the legacy "filter_scan with aggregate" plan by wrapping
    // the chain. For the trait path this is a bit verbose; here we
    // just check that Fragmenter + CapacityScheduler hook together
    // and yield an N-task assignment (N = partition_count).
    //
    // Schedule side stays simple: use a single-worker config so
    // every task lands on it. The assertion is round-trip: same
    // partition_count the Fragmenter produced is what the scheduler
    // reports.
    let scan_op = scan();
    // Build a minimal `Aggregate` exec via the struct; we don't
    // care about the inner expr shape for this test — only that
    // the boundary flag fires.
    use pylon_plan::physical::exec::AggregateExec as _;
    let agg_schema = schema_two();
    let group_by: Vec<Arc<dyn pylon_plan::physical::expr::PhysicalExpr>> = vec![Arc::new(
        ColumnExpr::new(1, agg_schema.field(1).clone()),
    )];
    let aggs: Vec<Arc<dyn pylon_plan::physical::expr::PhysicalExpr>> = vec![Arc::new(
        pylon_plan::physical::expr::AggregateFunctionExpr::new(
            "count",
            "count",
            vec![],
            DataType::Int64,
            vec![],
        ),
    )];
    let agg_plan: Arc<dyn ExecutionPlan> =
        Arc::new(AggregateExec::new(scan_op, group_by, aggs, agg_schema));

    let fragmenter = Fragmenter::new(FragmenterConfig {
        default_partition_count: 4,
    });
    let dag = fragmenter
        .fragment(&agg_plan, 42, &["worker-addr".into()])
        .expect("fragment ok");

    assert_eq!(dag.stages[0].partition_count, 4);
    assert_eq!(dag.stages[1].partition_count, 4);
    // Stage0 should carry SeqScan + ExchangeSinkRpc; stage1
    // ExchangeSource + Aggregate.
    let s0: Vec<&str> = dag.stages[0]
        .fragment
        .ops
        .iter()
        .map(|o| o.name.as_str())
        .collect();
    assert!(s0.contains(&"SeqScan"));
    assert!(s0.contains(&"ExchangeSinkRpc"));
    let s1: Vec<&str> = dag.stages[1]
        .fragment
        .ops
        .iter()
        .map(|o| o.name.as_str())
        .collect();
    assert!(s1.iter().filter(|n| **n == "ExchangeSource").count() == 4);
    assert!(s1.iter().filter(|n| **n == "Aggregate").count() == 4);

    // Scheduler side: CapacityScheduler produces one task per
    // partition. 4 partitions × 2 stages = 8 tasks. Single worker
    // admits everything.
    let sched = CapacityScheduler;
    let workers = vec![WorkerAddr {
        id: pylon_coord::scheduler::WorkerId(0),
        socket: "127.0.0.1:0".parse().unwrap(),
        capacity: WorkerCapacity {
            // 16 drivers so 2 stages × 4 partitions all admit on
            // the single worker. (Default `2 ncpu` would only
            // admit 4, dropping stage1's tasks.)
            max_drivers: 16,
            max_memory: 1 << 30,
        },
        in_flight: 0,
    }];
    let tasks = sched.assign(&dag, &workers, 42);
    // 2 stages × 4 partitions = 8 tasks.
    assert_eq!(tasks.len(), 8);
}

#[test]
fn capacity_scheduler_round_robin_assigns_to_one_worker() {
    use pylon_coord::scheduler::{CapacityScheduler, Scheduler, WorkerAddr, WorkerCapacity};
    let plan = scan();
    let fragmenter = Fragmenter::new(FragmenterConfig {
        default_partition_count: 2,
    });
    let dag = fragmenter.fragment(&plan, 1, &["x".into()]).unwrap();
    // No ExchangeSinkRpc (no Aggregate) means stage0 has the scan
    // op list and stage1 is empty. So only 1 stage × 2 partitions
    // = 2 tasks.
    let sched = CapacityScheduler;
    let workers = vec![WorkerAddr {
        id: pylon_coord::scheduler::WorkerId(0),
        socket: "127.0.0.1:0".parse().unwrap(),
        capacity: WorkerCapacity::default_for_ncpu(2, 1 << 30),
        in_flight: 0,
    }];
    let tasks = sched.assign(&dag, &workers, 1);
    // 2 partitions × 2 stages (stage0 has the scan/filter, stage1
    // is empty but the scheduler still iterates partition_count
    // tasks per stage) = 4 tasks.
    assert_eq!(tasks.len(), 4);
}
