//! M3+ 2-stage aggregate e2e: `SeqScan → ExchangeSinkRpc`
//! (partitioned, RPC) + N × `ExchangeSource → HashAggregate`
//! against a single loopback Arrow Flight server.
//!
//! This mirrors the production same-worker path (PR2 in
//! `docs/roadmap/m3-tail-exchange-unify.md`): every batch is sent
//! over a real `DoExchange` gRPC frame even when source and
//! destination are the same process. Hash-partition stability is
//! the A2 contract; the transport is now uniform with cross-worker.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};
use pylon_exchange::{FlightDescriptor, PylonFlightService};
use pylon_runtime::ops::{
    AggSpec, ExchangeSinkRpc, ExchangeSourceOp, HashAggregateOp, RpcTarget, SeqScanOp,
};
use pylon_runtime::{Driver, Pipeline, PipelineOp};

fn sample_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop();
    p.pop();
    p.push("data");
    p.push("sample.parquet");
    p
}

fn make_descriptors(query_id: u64, stage_id: u64, n: usize) -> Vec<FlightDescriptor> {
    (0..n)
        .map(|i| {
            FlightDescriptor(format!(
                "pylon://query/{query_id}/stage/{stage_id}/task/{i}"
            ))
        })
        .collect()
}

fn make_targets(addr: std::net::SocketAddr, descs: &[FlightDescriptor]) -> Vec<RpcTarget> {
    descs
        .iter()
        .map(|d| RpcTarget {
            flight_addr: addr.to_string(),
            descriptor: d.clone(),
        })
        .collect()
}

fn expected_aggregates() -> Vec<(String, i64, f64)> {
    let path = sample_path();
    let file = std::fs::File::open(&path).expect("sample.parquet open");
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("builder");
    let reader = builder.build().expect("reader");

    let mut acc: std::collections::BTreeMap<String, (i64, f64)> = std::collections::BTreeMap::new();
    for batch in reader {
        let batch = batch.expect("read batch");
        let region_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "name")
            .expect("name col");
        let amount_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "amount")
            .expect("amount col");
        let names = batch
            .column(region_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amounts = batch
            .column(amount_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let r = names.value(row).to_string();
            let a = if amounts.is_null(row) {
                0.0
            } else {
                amounts.value(row)
            };
            let entry = acc.entry(r).or_insert((0, 0.0));
            entry.0 += 1;
            entry.1 += a;
        }
    }
    acc.into_iter().map(|(k, (c, s))| (k, c, s)).collect()
}

fn make_stage0_pipeline(targets: Vec<RpcTarget>) -> Pipeline {
    let scan: Box<dyn PipelineOp> = Box::new(SeqScanOp::new(
        sample_path().to_string_lossy().to_string(),
        8192,
    ));
    let sink: Box<dyn PipelineOp> = Box::new(ExchangeSinkRpc::new_partitioned(
        targets,
        vec!["name".into()],
    ));
    Pipeline::new(vec![scan, sink])
}

fn make_stage1_pipeline(
    service: Arc<PylonFlightService>,
    descriptor: FlightDescriptor,
) -> Pipeline {
    let source: Box<dyn PipelineOp> = Box::new(ExchangeSourceOp::new(descriptor, service));
    let aggregate: Box<dyn PipelineOp> = Box::new(HashAggregateOp::new(
        vec!["name".into()],
        vec![
            AggSpec {
                func: "count".into(),
                arg_col: None,
                out_name: "count".into(),
            },
            AggSpec {
                func: "sum".into(),
                arg_col: Some("amount".into()),
                out_name: "sum_amount".into(),
            },
        ],
        Arc::new(Schema::empty()),
    ));
    Pipeline::new(vec![source, aggregate])
}

async fn collect_final_batches(
    mut rx: tokio::sync::mpsc::Receiver<RecordBatch>,
) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    while let Some(b) = rx.recv().await {
        out.push(b);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_2stage_partitioned_aggregate_matches_expected() {
    let n_partitions = 4;
    let (addr, service, h) = common::start_flight_server().await;
    let descs = make_descriptors(1, 2, n_partitions);
    let targets = make_targets(addr, &descs);

    let stage0_driver = Driver::new(make_stage0_pipeline(targets));
    let stage1_drivers: Vec<Driver> = (0..n_partitions)
        .map(|p| Driver::new(make_stage1_pipeline(service.clone(), descs[p].clone())))
        .collect();

    // Run Stage 0 to completion first, then wait deterministically
    // until every row has landed in the FlightService queues before
    // starting Stage 1. The old 500 ms sleep raced on shared CI
    // runners; `pending_rows` is an exact drain barrier (RFC 0007
    // M4.S5 follow-up).
    let stage0_handle = tokio::spawn(async move {
        let rx = stage0_driver.run(None).await.expect("stage0 run");
        collect_final_batches(rx).await
    });
    let stage0_batches = stage0_handle.await.expect("stage0 task ok");
    assert!(
        stage0_batches.is_empty(),
        "stage0 sink produces no output batches"
    );
    let total_rows = 100_000; // sample.parquet row count
    let barrier_deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut landed = 0usize;
        for d in &descs {
            landed += service.pending_rows(d).await;
        }
        if landed >= total_rows {
            break;
        }
        assert!(
            std::time::Instant::now() < barrier_deadline,
            "stage0 drain barrier: only {landed}/{total_rows} rows landed in 10s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let results: Vec<_> = futures::future::join_all(
        stage1_drivers
            .into_iter()
            .map(|d| {
                tokio::spawn(async move {
                    let rx = d.run(None).await.expect("stage1 run");
                    collect_final_batches(rx).await
                })
            })
            .collect::<Vec<_>>(),
    )
    .await;

    let mut stage1_batches: Vec<RecordBatch> = Vec::new();
    for (i, h) in results.iter().enumerate() {
        let batches = h.as_ref().unwrap_or_else(|e| panic!("stage1 {i}: {e}"));

        assert_eq!(
            batches.len(),
            1,
            "stage1 task {i} should emit exactly 1 final batch"
        );
        stage1_batches.push(batches[0].clone());
    }

    let all_batches: Vec<RecordBatch> = stage1_batches;
    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    let expected = expected_aggregates();
    assert_eq!(total_rows, expected.len(), "groups span all partitions");

    let schema = &all_batches[0].schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "name");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "count");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    assert_eq!(schema.field(2).name(), "sum_amount");
    assert_eq!(schema.field(2).data_type(), &DataType::Float64);

    let mut actual: std::collections::BTreeMap<String, (i64, f64)> =
        std::collections::BTreeMap::new();
    for b in &all_batches {
        let names = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let counts = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let sums = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        for r in 0..b.num_rows() {
            let name = names.value(r).to_string();
            let c = counts.value(r);
            let s = sums.value(r);
            let entry = actual.entry(name).or_insert((0, 0.0));
            entry.0 += c;
            entry.1 += s;
        }
    }
    assert_eq!(actual.len(), expected.len());
    for (exp_name, exp_count, exp_sum) in &expected {
        let (got_c, got_s) = actual.get(exp_name).expect(exp_name);
        assert_eq!(*got_c, *exp_count, "{exp_name} count");
        let diff = (got_s - exp_sum).abs();
        assert!(
            diff < 1e-3,
            "{exp_name} sum: expected {exp_sum}, got {got_s}"
        );
    }
    h.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_2stage_same_groups_not_split_across_partitions() {
    let n_partitions = 4;
    let (addr, service, h) = common::start_flight_server().await;
    let descs = make_descriptors(1, 2, n_partitions);
    let targets = make_targets(addr, &descs);

    let stage0_driver = Driver::new(make_stage0_pipeline(targets));
    let mut stage1_drivers: Vec<Driver> = (0..n_partitions)
        .map(|p| Driver::new(make_stage1_pipeline(service.clone(), descs[p].clone())))
        .collect();

    // Stage 0 first, then a deterministic drain barrier (all rows
    // landed in the FlightService queues), then Stage 1. This
    // removes the race that made this test flaky on shared CI
    // runners (it previously ran both stages concurrently and
    // relied on the ExchangeSourceOp empty-poll heuristic).
    let stage0_handle = tokio::spawn(async move {
        let rx = stage0_driver.run(None).await.expect("stage0 run");
        collect_final_batches(rx).await
    });
    let stage0_batches = stage0_handle.await.expect("stage0 task ok");
    assert!(stage0_batches.is_empty(), "stage0 produces no output");

    let mut handles = Vec::new();
    let total_rows = 100_000; // sample.parquet row count
    let barrier_deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut landed = 0usize;
        for d in &descs {
            landed += service.pending_rows(d).await;
        }
        if landed >= total_rows {
            break;
        }
        assert!(
            std::time::Instant::now() < barrier_deadline,
            "stage0 drain barrier: only {landed}/{total_rows} rows landed in 10s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    for d in stage1_drivers.drain(..) {
        handles.push(tokio::spawn(async move {
            let rx = d.run(None).await.expect("stage1 run");
            collect_final_batches(rx).await
        }));
    }
    let results = futures::future::join_all(handles).await;
    let stage1_results: Vec<Vec<RecordBatch>> = results
        .iter()
        .map(|h| h.as_ref().unwrap().clone())
        .collect();

    let mut seen: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for batches in &stage1_results {
        for b in batches {
            let names = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for r in 0..b.num_rows() {
                let name = names.value(r).to_string();
                *seen.entry(name).or_insert(0) += 1;
            }
        }
    }
    let expected = expected_aggregates();
    for (exp_name, _, _) in &expected {
        assert_eq!(
            seen.get(exp_name).copied().unwrap_or(0),
            1,
            "name {exp_name} should appear in exactly 1 stage1 task's output"
        );
    }
    h.abort();
}
