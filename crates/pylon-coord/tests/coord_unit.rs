//! Smoke tests for the coordinator abstractions (M2 prep).

use pylon_coord::scheduler::{CapacityScheduler, Scheduler, WorkerAddr, WorkerCapacity, WorkerId};
use pylon_coord::stage::{Distribution, OpSpec, Stage, StageDag, StageId};
use pylon_coord::task::Partition;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn make_worker(socket: [u8; 4], in_flight: usize) -> WorkerAddr {
    WorkerAddr {
        id: WorkerId::generate(),
        socket: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(socket[0], socket[1], socket[2], socket[3])), 9090),
        capacity: WorkerCapacity { max_drivers: 16, max_memory: 8 * 1024 * 1024 * 1024 },
        in_flight,
    }
}

#[test]
fn fragment_then_schedule_pipelines_through() {
    let mut dag = StageDag::new();
    let scan_stage = Stage::new(
        StageId(1),
        pylon_coord::stage::Fragment {
            ops: vec![OpSpec::new("SeqScan").with("table", "sample")],
            distribution: Distribution::Partitioned(16),
        },
    );
    let agg_stage = Stage::new(
        StageId(2),
        pylon_coord::stage::Fragment {
            ops: vec![OpSpec::new("HashAggregate").with("type", "final")],
            distribution: Distribution::Single,
        },
    );
    dag = dag.with_stage(scan_stage);
    dag = dag.with_stage(agg_stage);
    assert_eq!(dag.tasks_total(), 17);

    let workers = vec![make_worker([10, 0, 0, 1], 0), make_worker([10, 0, 0, 2], 0)];
    let scheduler = CapacityScheduler;
    let assignments = scheduler.assign(&dag, &workers, 42);

    assert_eq!(assignments.len(), 17);
    let scan_count = assignments.iter().filter(|(t, _)| t.stage_id == StageId(1)).count();
    let agg_count = assignments.iter().filter(|(t, _)| t.stage_id == StageId(2)).count();
    assert_eq!(scan_count, 16);
    assert_eq!(agg_count, 1);

    let first_scan = assignments.iter().find(|(t, _)| t.stage_id == StageId(1)).map(|(t, _)| t).unwrap();
    assert_eq!(first_scan.partition, Partition(0));
    assert_eq!(first_scan.query_id, 42);
    assert_eq!(first_scan.fragment.ops[0].name, "SeqScan");
}

#[test]
fn worker_capacity_default_matches_target() {
    let cap = WorkerCapacity::default_for_ncpu(4, 16 * 1024 * 1024 * 1024);
    assert_eq!(cap.max_drivers, 8);
    let cap = WorkerCapacity::default_for_ncpu(64, 256 * 1024 * 1024 * 1024);
    assert_eq!(cap.max_drivers, 16);
    let cap = WorkerCapacity::default_for_ncpu(8, 8 * 1024 * 1024 * 1024);
    assert_eq!(cap.max_memory, 4 * 1024 * 1024 * 1024);
}

#[test]
fn stage_partition_count_follows_distribution() {
    let s = Stage::new(StageId(1), pylon_coord::stage::Fragment {
        ops: vec![],
        distribution: Distribution::Partitioned(8),
    });
    assert_eq!(s.partition_count, 8);
    let s = s.with_partition_count(32);
    assert_eq!(s.partition_count, 32);
}

#[test]
fn distribution_partition_count() {
    assert_eq!(Distribution::Single.partition_count(), 1);
    assert_eq!(Distribution::Partitioned(16).partition_count(), 16);
    assert_eq!(Distribution::Broadcast.partition_count(), 1);
    assert!(Distribution::Broadcast.is_broadcast());
    assert!(!Distribution::Single.is_broadcast());
    assert!(!Distribution::Partitioned(4).is_broadcast());
}

#[test]
fn fragmenter_collapses_simple_plan_into_single_stage() {
    use pylon_coord::fragment::{Fragmenter, FragmenterConfig};
    use pylon_plan::physical::physical_expr::PhysicalExpr;
    use pylon_plan::physical::PhysicalPlan;

    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::SeqScan {
            table: "sample".to_string(),
            schema: schema.clone(),
        }),
        predicate: PhysicalExpr::BinaryOp {
            left: Box::new(PhysicalExpr::Column {
                index: 0,
                field: Field::new("id", DataType::Int64, false),
            }),
            op: ">".to_string(),
            right: Box::new(PhysicalExpr::Literal {
                value: "5".to_string(),
                data_type: DataType::Utf8,
            }),
        },
    };

    let fragmenter = Fragmenter::new(FragmenterConfig { default_partition_count: 16 });
    let dag = fragmenter.fragment_multi_stage(&plan, 0).expect("fragmenter ok");

    
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
