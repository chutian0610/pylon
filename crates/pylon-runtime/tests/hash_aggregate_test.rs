//! A1-3 unit tests for `HashAggregateOp`.

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pylon_runtime::ops::{build_aggregate_output_schema, AggSpec, HashAggregateOp};
use pylon_runtime::PipelineOp;

fn schema_i64_str_f64() -> Arc<Schema> {
    // amount is nullable so the count-nulls test can insert nulls.
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, true),
    ]))
}

fn make_input_batch() -> RecordBatch {
    let schema = schema_i64_str_f64();
    let ids = Int64Array::from(vec![1, 2, 3, 4, 5, 6]);
    let regions = StringArray::from(vec!["a", "b", "a", "b", "a", "c"]);
    let amounts = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(regions), Arc::new(amounts)]).unwrap()
}

fn agg(name: &str, func: &str, arg: Option<&str>) -> AggSpec {
    AggSpec {
        func: func.into(),
        arg_col: arg.map(|s| s.into()),
        out_name: name.into(),
    }
}

fn groups_of(batch: &RecordBatch, col: &str) -> Vec<String> {
    let idx = batch.schema().fields().iter().position(|f| f.name() == col).unwrap();
    let arr = batch.column(idx).as_any().downcast_ref::<StringArray>().unwrap();
    (0..batch.num_rows()).map(|i| arr.value(i).to_string()).collect()
}

fn i64_col(batch: &RecordBatch, col: &str) -> Vec<Option<i64>> {
    let idx = batch.schema().fields().iter().position(|f| f.name() == col).unwrap();
    let arr = batch.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
    (0..batch.num_rows()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }).collect()
}

fn f64_col(batch: &RecordBatch, col: &str) -> Vec<Option<f64>> {
    let idx = batch.schema().fields().iter().position(|f| f.name() == col).unwrap();
    let arr = batch.column(idx).as_any().downcast_ref::<Float64Array>().unwrap();
    (0..batch.num_rows()).map(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }).collect()
}

#[tokio::test]
async fn count_star_group_by_one_column() {
    // GROUP BY region; COUNT(*). Expect one row per distinct region.
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("cnt", DataType::Int64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("cnt", "count", None)],
        output_schema,
    );

    let input = make_input_batch();
    op.add_input(input).await.unwrap();
    // Output is not ready until EOS.
    assert!(op.get_output().await.unwrap().is_none());
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().expect("final batch");
    assert_eq!(out.num_columns(), 2);
    assert_eq!(out.num_rows(), 3, "three distinct regions: a, b, c");
    let groups = groups_of(&out, "region");
    // Sorted by GroupKey: Utf8 enum → "a" < "b" < "c".
    assert_eq!(groups, vec!["a", "b", "c"]);
    let counts = i64_col(&out, "cnt");
    assert_eq!(counts, vec![Some(3), Some(2), Some(1)]);
    assert!(op.is_finished().await);
}

#[tokio::test]
async fn count_column_ignores_nulls() {
    // COUNT(amount) on 6 rows where amount=10,20,30,40,50,60 — all non-null.
    // Then we add a second batch with one null and verify the count is
    // still 6 (null skipped).
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("cnt_amount", DataType::Int64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("cnt_amount", "count", Some("amount"))],
        output_schema,
    );

    let b1 = make_input_batch();
    op.add_input(b1).await.unwrap();

    // Second batch: one extra row per region, with one null amount.
    let schema = schema_i64_str_f64();
    let ids = Int64Array::from(vec![7, 8, 9, 10]);
    let regions = StringArray::from(vec!["a", "a", "b", "b"]);
    let amounts = Float64Array::from(vec![
        Some(70.0),
        None, // null
        Some(80.0),
        None, // null
    ]);
    let b2 = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(regions), Arc::new(amounts)]).unwrap();
    op.add_input(b2).await.unwrap();
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();

    let groups = groups_of(&out, "region");
    assert_eq!(groups, vec!["a", "b", "c"]);
    let counts = i64_col(&out, "cnt_amount");
    // a: 3 (b1) + 1 (b2 non-null) = 4
    // b: 2 (b1) + 1 (b2 non-null) = 3
    // c: 1 (b1) + 0 = 1
    assert_eq!(counts, vec![Some(4), Some(3), Some(1)]);
}

#[tokio::test]
async fn sum_group_by_one_column() {
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("total", DataType::Float64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("total", "sum", Some("amount"))],
        output_schema,
    );
    op.add_input(make_input_batch()).await.unwrap();
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();
    let groups = groups_of(&out, "region");
    assert_eq!(groups, vec!["a", "b", "c"]);
    let totals = f64_col(&out, "total");
    // a: 10+30+50 = 90
    // b: 20+40 = 60
    // c: 60
    assert_eq!(totals, vec![Some(90.0), Some(60.0), Some(60.0)]);
}

#[tokio::test]
async fn min_max_on_int_and_string() {
    // GROUP BY region, MIN(id), MAX(id), MIN(region), MAX(region).
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![
            Field::new("lo_id", DataType::Int64, true),
            Field::new("hi_id", DataType::Int64, true),
            Field::new("lo_r", DataType::Utf8, true),
            Field::new("hi_r", DataType::Utf8, true),
        ],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![
            agg("lo_id", "min", Some("id")),
            agg("hi_id", "max", Some("id")),
            agg("lo_r", "min", Some("region")),
            agg("hi_r", "max", Some("region")),
        ],
        output_schema,
    );
    op.add_input(make_input_batch()).await.unwrap();
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();
    let lo_id = i64_col(&out, "lo_id");
    let hi_id = i64_col(&out, "hi_id");
    // a: ids 1,3,5 → min 1, max 5
    // b: ids 2,4   → min 2, max 4
    // c: id  6     → min 6, max 6
    assert_eq!(lo_id, vec![Some(1), Some(2), Some(6)]);
    assert_eq!(hi_id, vec![Some(5), Some(4), Some(6)]);
}

#[tokio::test]
async fn multiple_aggregates_in_one_op() {
    // COUNT(*), SUM(amount), MIN(id), MAX(id) — all on region.
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![
            Field::new("cnt", DataType::Int64, true),
            Field::new("total", DataType::Float64, true),
            Field::new("lo", DataType::Int64, true),
            Field::new("hi", DataType::Int64, true),
        ],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![
            agg("cnt", "count", None),
            agg("total", "sum", Some("amount")),
            agg("lo", "min", Some("id")),
            agg("hi", "max", Some("id")),
        ],
        output_schema,
    );
    op.add_input(make_input_batch()).await.unwrap();
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(i64_col(&out, "cnt"), vec![Some(3), Some(2), Some(1)]);
    assert_eq!(f64_col(&out, "total"), vec![Some(90.0), Some(60.0), Some(60.0)]);
    assert_eq!(i64_col(&out, "lo"), vec![Some(1), Some(2), Some(6)]);
    assert_eq!(i64_col(&out, "hi"), vec![Some(5), Some(4), Some(6)]);
}

#[tokio::test]
async fn global_aggregate_with_no_group_by() {
    // No group_by cols; one aggregate (SUM). Single output row.
    let output_schema = build_aggregate_output_schema(
        vec![],
        vec![Field::new("total", DataType::Float64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec![],
        vec![agg("total", "sum", Some("amount"))],
        output_schema,
    );
    op.add_input(make_input_batch()).await.unwrap();
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();
    assert_eq!(out.num_rows(), 1, "global aggregate → exactly one row");
    assert_eq!(f64_col(&out, "total"), vec![Some(210.0)]);
}

#[tokio::test]
async fn empty_input_emits_zero_row_well_formed_batch() {
    // Even with zero input, after no_more_input the op must emit a
    // well-formed (zero-row) output batch with the right schema.
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("cnt", DataType::Int64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("cnt", "count", None)],
        output_schema.clone(),
    );
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();
    assert_eq!(out.num_rows(), 0);
    assert_eq!(out.num_columns(), output_schema.fields().len());
    assert_eq!(out.schema().as_ref(), output_schema.as_ref());
    assert!(op.is_finished().await);
}

#[tokio::test]
async fn empty_batch_in_middle_is_ignored() {
    // add_input with 0-row batch is a no-op; later add_input still
    // works.
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("cnt", DataType::Int64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("cnt", "count", None)],
        output_schema,
    );
    let empty = RecordBatch::new_empty(schema_i64_str_f64());
    op.add_input(empty).await.unwrap();
    op.add_input(make_input_batch()).await.unwrap();
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();
    assert_eq!(out.num_rows(), 3);
}

#[tokio::test]
async fn add_input_after_emit_is_rejected() {
    // After no_more_input, the op has emitted its final batch. Any
    // further add_input must error rather than silently corrupt state.
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("cnt", DataType::Int64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("cnt", "count", None)],
        output_schema,
    );
    op.add_input(make_input_batch()).await.unwrap();
    op.no_more_input().await.unwrap();
    // Drain the final batch so is_finished becomes true.
    let _ = op.get_output().await.unwrap();
    assert!(op.is_finished().await);
    let err = op.add_input(make_input_batch()).await.unwrap_err();
    assert!(
        err.to_string().contains("after emitting final batch"),
        "got: {err}"
    );
}

#[tokio::test]
async fn multiple_batches_combine_correctly() {
    // Two non-overlapping batches; group_by state merges.
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("total", DataType::Float64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("total", "sum", Some("amount"))],
        output_schema,
    );
    // First batch: only "a" + "b".
    let schema = schema_i64_str_f64();
    let ids = Int64Array::from(vec![1, 2]);
    let regions = StringArray::from(vec!["a", "b"]);
    let amounts = Float64Array::from(vec![100.0, 200.0]);
    let b1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(regions), Arc::new(amounts)]).unwrap();
    op.add_input(b1).await.unwrap();
    // Second batch: same "a" + new "c".
    let ids = Int64Array::from(vec![3, 4]);
    let regions = StringArray::from(vec!["a", "c"]);
    let amounts = Float64Array::from(vec![5.0, 50.0]);
    let b2 = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(regions), Arc::new(amounts)]).unwrap();
    op.add_input(b2).await.unwrap();
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();
    let groups = groups_of(&out, "region");
    assert_eq!(groups, vec!["a", "b", "c"]);
    let totals = f64_col(&out, "total");
    // a: 100 + 5 = 105
    // b: 200
    // c: 50
    assert_eq!(totals, vec![Some(105.0), Some(200.0), Some(50.0)]);
}

#[tokio::test]
async fn unknown_group_by_column_errors() {
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("cnt", DataType::Int64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["nope".into()],
        vec![agg("cnt", "count", None)],
        output_schema,
    );
    let err = op.add_input(make_input_batch()).await.unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
}

#[tokio::test]
async fn unknown_aggregate_arg_column_errors() {
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("total", DataType::Float64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("total", "sum", Some("nope"))],
        output_schema,
    );
    let err = op.add_input(make_input_batch()).await.unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
}

#[tokio::test]
async fn sum_i64_works() {
    // SUM on Int64 column; the op must coerce result type correctly.
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("qty", DataType::Int64, false),
    ]));
    let input = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "a", "b"])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )
    .unwrap();
    let output_schema = build_aggregate_output_schema(
        vec![Field::new("region", DataType::Utf8, false)],
        vec![Field::new("total_qty", DataType::Int64, true)],
    );
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("total_qty", "sum", Some("qty"))],
        output_schema,
    );
    op.add_input(input).await.unwrap();
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();
    assert_eq!(groups_of(&out, "region"), vec!["a", "b"]);
    // a: 10+30+40 = 80
    // b: 20+50 = 70
    assert_eq!(i64_col(&out, "total_qty"), vec![Some(80), Some(70)]);
}

#[tokio::test]
async fn empty_output_schema_is_resolved_on_first_batch() {
    // When the worker constructs the op from an OpSpec, it doesn't
    // have a way to know the post-aggregate schema. It can pass
    // Schema::empty() and the op will derive the schema from the
    // first input batch.
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("cnt", "count", None)],
        Arc::new(Schema::empty()),
    );
    op.add_input(make_input_batch()).await.unwrap();
    op.no_more_input().await.unwrap();
    let out = op.get_output().await.unwrap().unwrap();
    // Schema should now have region (Utf8) + cnt (Int64) = 2 fields.
    assert_eq!(out.schema().fields().len(), 2);
    assert_eq!(out.schema().field(0).name(), "region");
    assert_eq!(out.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(out.schema().field(1).name(), "cnt");
    assert_eq!(out.schema().field(1).data_type(), &DataType::Int64);
    assert_eq!(out.num_rows(), 3);
}

#[tokio::test]
async fn empty_input_with_empty_schema_errors() {
    // No batch ever arrives, output schema wasn't supplied → error
    // rather than silently emit nothing.
    let mut op = HashAggregateOp::new(
        vec!["region".into()],
        vec![agg("cnt", "count", None)],
        Arc::new(Schema::empty()),
    );
    // The op rejects no_more_input when it has no way to derive the
    // output schema (no input, no schema).
    let err = op.no_more_input().await.unwrap_err();
    assert!(err.to_string().contains("empty input and no output schema"));
}
