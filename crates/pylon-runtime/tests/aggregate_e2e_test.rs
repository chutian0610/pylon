//! A1-5 end-to-end test: in-process `SeqScanOp → HashAggregateOp`
//! pipeline running on `data/sample.parquet`.
//!
//! This is the **A1** wire-up — single stage, single worker, no
//! Exchange. Real Flight-based shuffle across workers (B) is still a
//! TODO and is not exercised here.
//!
//! The expected output is computed by directly reading the parquet
//! file with the parquet crate and aggregating in-process, so the
//! test doesn't depend on any external tool (no Python / DuckDB).

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pylon_runtime::ops::{AggSpec, HashAggregateOp, SeqScanOp};
use pylon_runtime::{Driver, Pipeline, PipelineOp};

fn sample_path() -> PathBuf {
    // Tests run from the crate root (cargo test changes cwd to the
    // crate dir). data/sample.parquet is one level up at the workspace
    // root.
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates/pylon-runtime → crates
    p.pop(); // crates → repo root
    p.push("data");
    p.push("sample.parquet");
    p
}

/// Read the whole sample.parquet and compute the expected aggregate
/// result with pure Rust (parquet crate). Returns (name → (count,
/// sum_amount)) as BTreeMap-like Vec sorted by name.
fn expected_aggregates() -> Vec<(String, i64, f64)> {
    let path = sample_path();
    let file = std::fs::File::open(&path).expect("sample.parquet open");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("builder");
    let reader = builder.build().expect("reader");

    let mut acc: std::collections::BTreeMap<String, (i64, f64)> = std::collections::BTreeMap::new();

    for batch in reader {
        let batch = batch.expect("read batch");
        let name_idx = batch
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
            .column(name_idx)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_scan_aggregate_single_stage() {
    // M3 A1-5 E2E: SeqScan → HashAggregate, single stage, single
    // worker, no shuffle. This is the M1-aggregate equivalence check.
    // Real cross-worker Flight shuffle is B (TODO).

    // 1. Build the pipeline.
    let scan: Box<dyn PipelineOp> = Box::new(SeqScanOp::new(
        sample_path().to_string_lossy().to_string(),
        8192,
    ));
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
        // Empty → op derives from first input batch.
        Arc::new(Schema::empty()),
    ));
    let pipeline = Pipeline::new(vec![scan, aggregate]);
    let driver = Driver::new(pipeline);

    // 2. Run.
    let mut out_rx = driver.run(None).await.expect("driver.run");
    let mut collected: Vec<RecordBatch> = Vec::new();
    while let Some(b) = out_rx.recv().await {
        collected.push(b);
    }

    // 3. Aggregate may emit exactly one final batch.
    assert_eq!(collected.len(), 1, "expected 1 final batch");
    let out = &collected[0];

    // Schema: name (Utf8) + count (Int64) + sum_amount (Float64).
    let schema = out.schema();
    assert_eq!(schema.fields().len(), 3, "3 output fields");
    assert_eq!(schema.field(0).name(), "name");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "count");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    assert_eq!(schema.field(2).name(), "sum_amount");
    assert_eq!(schema.field(2).data_type(), &DataType::Float64);

    // 4. Compare row-by-row to expected.
    let expected = expected_aggregates();
    assert_eq!(out.num_rows(), expected.len(), "one row per distinct name");

    let name_col = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let count_col = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    let sum_col = out
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    for (i, (exp_name, exp_count, exp_sum)) in expected.iter().enumerate() {
        let got_name = name_col.value(i);
        assert_eq!(got_name, *exp_name, "row {i} name");
        let got_count = count_col.value(i);
        assert_eq!(got_count, *exp_count, "row {i} count");
        let got_sum = sum_col.value(i);
        // float compare with epsilon
        let diff = (got_sum - exp_sum).abs();
        assert!(
            diff < 1e-3,
            "row {i} sum: expected {exp_sum}, got {got_sum}, diff {diff}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_count_star_only_global_aggregate() {
    // SELECT COUNT(*) FROM sample — no group_by, one row in the
    // output. Confirms the global-aggregate path (no group_by cols)
    // works end-to-end.
    let scan: Box<dyn PipelineOp> = Box::new(SeqScanOp::new(
        sample_path().to_string_lossy().to_string(),
        8192,
    ));
    let aggregate: Box<dyn PipelineOp> = Box::new(HashAggregateOp::new(
        vec![],
        vec![AggSpec {
            func: "count".into(),
            arg_col: None,
            out_name: "count".into(),
        }],
        Arc::new(Schema::empty()),
    ));
    let pipeline = Pipeline::new(vec![scan, aggregate]);
    let driver = Driver::new(pipeline);

    let mut out_rx = driver.run(None).await.expect("driver.run");
    let mut collected: Vec<RecordBatch> = Vec::new();
    while let Some(b) = out_rx.recv().await {
        collected.push(b);
    }
    assert_eq!(collected.len(), 1);
    let out = &collected[0];

    // Compute the expected total row count from the parquet file.
    let file = std::fs::File::open(sample_path()).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();
    let total_rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();

    assert_eq!(out.num_rows(), 1, "global aggregate → exactly 1 row");
    assert_eq!(out.num_columns(), 1, "one output column: count");
    let count_col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(count_col.value(0) as usize, total_rows);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_aggregate_emits_exactly_one_batch() {
    // The op must not emit intermediate batches. Even with many
    // input batches, the output stream should be a single batch on
    // no_more_input.
    let scan: Box<dyn PipelineOp> = Box::new(SeqScanOp::new(
        sample_path().to_string_lossy().to_string(),
        // Small batch size so the scan produces many batches.
        128,
    ));
    let aggregate: Box<dyn PipelineOp> = Box::new(HashAggregateOp::new(
        vec!["name".into()],
        vec![AggSpec {
            func: "count".into(),
            arg_col: None,
            out_name: "count".into(),
        }],
        Arc::new(Schema::empty()),
    ));
    let pipeline = Pipeline::new(vec![scan, aggregate]);
    let driver = Driver::new(pipeline);

    let mut out_rx = driver.run(None).await.expect("driver.run");
    let mut n_batches = 0;
    while let Some(b) = out_rx.recv().await {
        n_batches += 1;
        // Final batch sanity: count is Int64, non-empty.
        assert!(b.num_rows() > 0, "non-empty result");
    }
    assert_eq!(n_batches, 1, "HashAggregate must emit exactly one batch");
}

// Reference schema for the documented input shape (used in the doc
// comment only; satisfies dead-code warning for SchemaRef import).
#[allow(dead_code)]
fn _sample_input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, true),
    ]))
}
