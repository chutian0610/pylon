//! A2-2 end-to-end: in-process 2-stage `SeqScan → ExchangeSink(partitioned)`
//! + N× `ExchangeSource → HashAggregate` pipelines running on
//! `data/sample.parquet`.
//!
//! The test mirrors the real cross-task shuffle topology: stage0
//! emits batches to N partition descriptors (via the partitioned
//! `ExchangeSinkOp`); N stage1 tasks each pull from one descriptor
//! and aggregate independently. The global result is the concat of
//! all N stage1 outputs (no group spans partitions, so no
//! cross-partition merge is needed).
//!
//! This is the **A2** wire-up — single worker, in-process, no real
//! Flight RPC. The 2-worker cross-process version is **B (TODO)**.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pylon_exchange::{FlightDescriptor, PylonFlightService};
use pylon_runtime::ops::{
    AggSpec, ExchangeSinkOp, ExchangeSourceOp, HashAggregateOp, SeqScanOp,
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
        .map(|i| FlightDescriptor(format!("pylon://query/{query_id}/stage/{stage_id}/task/{i}")))
        .collect()
}

fn expected_aggregates() -> Vec<(String, i64, f64)> {
    // Same expected-aggregate helper as A1-5's E2E: read the parquet
    // file with the parquet crate and compute the answer in pure
    // Rust.
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
            let a = if amounts.is_null(row) { 0.0 } else { amounts.value(row) };
            let entry = acc.entry(r).or_insert((0, 0.0));
            entry.0 += 1;
            entry.1 += a;
        }
    }
    acc.into_iter().map(|(k, (c, s))| (k, c, s)).collect()
}

fn make_stage0_pipeline(service: Arc<PylonFlightService>, n_partitions: usize) -> Pipeline {
    let scan: Box<dyn PipelineOp> = Box::new(SeqScanOp::new(
        sample_path().to_string_lossy().to_string(),
        8192,
    ));
    let sink: Box<dyn PipelineOp> = Box::new(ExchangeSinkOp::new_partitioned(
        make_descriptors(1, 2, n_partitions),
        vec!["name".into()],
        service,
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

async fn collect_final_batches(mut rx: tokio::sync::mpsc::Receiver<RecordBatch>) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    while let Some(b) = rx.recv().await {
        out.push(b);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_2stage_partitioned_aggregate_matches_expected() {
    let n_partitions = 4;
    let service = Arc::new(PylonFlightService::new());

    // Stage 0 + N stage 1 tasks run in parallel; the in-process
    // PylonFlightService acts as the "wire" between them.
    let stage0_driver = Driver::new(make_stage0_pipeline(service.clone(), n_partitions));
    let mut stage1_drivers: Vec<Driver> = (0..n_partitions)
        .map(|p| {
            Driver::new(make_stage1_pipeline(
                service.clone(),
                make_descriptors(1, 2, n_partitions)[p].clone(),
            ))
        })
        .collect();

    // Run stage0 + all stage1 concurrently.
    let mut handles = Vec::new();
    handles.push(tokio::spawn(async move {
        let rx = stage0_driver.run(None).await.expect("stage0 run");
        collect_final_batches(rx).await
    }));
    for d in stage1_drivers.drain(..) {
        handles.push(tokio::spawn(async move {
            let rx = d.run(None).await.expect("stage1 run");
            collect_final_batches(rx).await
        }));
    }
    let results = futures::future::join_all(handles).await;
    let stage0_batches = results[0].as_ref().expect("stage0 task ok");
    // Stage 0 has a sink op (no output batches), but the driver
    // still produces a final receiver; it should be empty.
    assert!(
        stage0_batches.is_empty(),
        "stage0 sink produces no output batches"
    );

    // Each stage1 task emits exactly 1 final batch.
    let mut stage1_batches: Vec<RecordBatch> = Vec::new();
    for (i, h) in results[1..].iter().enumerate() {
        let batches = h.as_ref().unwrap_or_else(|e| panic!("stage1 {i}: {e}"));
        assert_eq!(
            batches.len(),
            1,
            "stage1 task {i} should emit exactly 1 final batch"
        );
        stage1_batches.push(batches[0].clone());
    }

    // Concat all stage1 outputs. Since each group goes to exactly
    // one partition, no group appears in two batches — concat is the
    // full global result.
    let all_batches: Vec<RecordBatch> = stage1_batches;
    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    let expected = expected_aggregates();
    assert_eq!(total_rows, expected.len(), "groups span all partitions");

    // Verify the schema of one stage1 batch (they all share the
    // post-aggregate schema).
    let schema = &all_batches[0].schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "name");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "count");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    assert_eq!(schema.field(2).name(), "sum_amount");
    assert_eq!(schema.field(2).data_type(), &DataType::Float64);

    // Aggregate (name, count, sum) across all stage1 batches into a
    // single map, then compare to expected.
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
        assert!(diff < 1e-3, "{exp_name} sum: expected {exp_sum}, got {got_s}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_2stage_same_groups_not_split_across_partitions() {
    // Stronger guarantee: each distinct name goes to exactly one
    // partition. We can't trivially observe this from the result,
    // but we can prove it via the row count per stage1 task. If the
    // hash were broken, the per-task rows would not equal the
    // global count's split.
    //
    // A subtler check: each stage1 task's final batch has at most
    // as many distinct (name, count) entries as there are distinct
    // names — meaning within a partition, no name is double-emitted.
    let n_partitions = 4;
    let service = Arc::new(PylonFlightService::new());

    let stage0_driver = Driver::new(make_stage0_pipeline(service.clone(), n_partitions));
    let mut stage1_drivers: Vec<Driver> = (0..n_partitions)
        .map(|p| {
            Driver::new(make_stage1_pipeline(
                service.clone(),
                make_descriptors(1, 2, n_partitions)[p].clone(),
            ))
        })
        .collect();

    let mut handles = Vec::new();
    handles.push(tokio::spawn(async move {
        let rx = stage0_driver.run(None).await.expect("stage0 run");
        collect_final_batches(rx).await
    }));
    for d in stage1_drivers.drain(..) {
        handles.push(tokio::spawn(async move {
            let rx = d.run(None).await.expect("stage1 run");
            collect_final_batches(rx).await
        }));
    }
    let results = futures::future::join_all(handles).await;
    let stage1_results: Vec<Vec<RecordBatch>> = results[1..]
        .iter()
        .map(|h| h.as_ref().unwrap().clone())
        .collect();

    // Each (name, count) pair must appear in exactly one stage1
    // output. This is the "groups are partition-stable" check.
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
}
