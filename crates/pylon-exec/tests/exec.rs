//! 执行器测试：并行下结果完整且不重复、LIMIT 精确、投影正确、错误不 panic。

use std::collections::HashSet;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::{Float64Type, Int64Type};
use pylon_exec::{ExecError, PipelineDriver};
use pylon_plan::{Operator, Pipeline, plan};
use pylon_table::ParquetDirFormat;
use pylon_table::testing::{FixtureSpec, expected_rows, fixture_row, write_fixture_table};

// 3 个文件 × 2500 行：跨文件、跨 row group（每 1024 行一个）、跨 morsel 边界
const SPEC: FixtureSpec = FixtureSpec {
    files: 3,
    rows_per_file: 2500,
};

fn fixture() -> (tempfile::TempDir, ParquetDirFormat) {
    let dir = tempfile::tempdir().unwrap();
    write_fixture_table(dir.path(), "t", SPEC).unwrap();
    let format = ParquetDirFormat::new(dir.path());
    (dir, format)
}

fn run(sql: &str, format: &ParquetDirFormat, driver: &PipelineDriver) -> RecordBatch {
    let pipeline = plan(sql, format).unwrap();
    let batches = driver.run(&pipeline).unwrap();
    concat_batches(&pipeline.output_schema, &batches).unwrap()
}

fn names(batch: &RecordBatch) -> Vec<&str> {
    batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect()
}

fn i64s(batch: &RecordBatch, col: usize) -> Vec<i64> {
    batch
        .column(col)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

#[test]
fn full_scan_returns_every_row_exactly_once() {
    let (_dir, format) = fixture();
    let result = run("SELECT * FROM t", &format, &PipelineDriver::default());
    assert_eq!(names(&result), ["a", "b", "c"]);
    assert_eq!(result.num_rows(), SPEC.total_rows());

    let mut got = i64s(&result, 0);
    got.sort_unstable();
    let mut want: Vec<i64> = expected_rows(SPEC).into_iter().map(|r| r.a).collect();
    want.sort_unstable();
    assert_eq!(got, want);
}

#[test]
fn limit_yields_exactly_min_limit_total_distinct_rows() {
    let (_dir, format) = fixture();
    let driver = PipelineDriver::default();
    let universe: HashSet<i64> = expected_rows(SPEC).into_iter().map(|r| r.a).collect();

    for limit in [1usize, 10, 8191, 8192, 8193, 7499, 7500, 100_000] {
        let result = run(&format!("SELECT a FROM t LIMIT {limit}"), &format, &driver);
        let got = i64s(&result, 0);
        assert_eq!(got.len(), limit.min(SPEC.total_rows()), "LIMIT {limit}");
        let distinct: HashSet<i64> = got.iter().copied().collect();
        assert_eq!(distinct.len(), got.len(), "LIMIT {limit}: duplicated rows");
        assert!(
            distinct.is_subset(&universe),
            "LIMIT {limit}: unknown values"
        );
    }
}

#[test]
fn limit_zero_yields_no_batches() {
    let (_dir, format) = fixture();
    let pipeline = plan("SELECT a FROM t LIMIT 0", &format).unwrap();
    assert!(PipelineDriver::default().run(&pipeline).unwrap().is_empty());
}

#[test]
fn project_reorders_and_duplicates_columns_row_consistently() {
    let (_dir, format) = fixture();
    let result = run(
        "SELECT c, a, c FROM t LIMIT 50",
        &format,
        &PipelineDriver::default(),
    );
    assert_eq!(names(&result), ["c", "a", "c"]);
    assert_eq!(result.num_rows(), 50);

    let c1 = result.column(0).as_primitive::<Float64Type>();
    let a = result.column(1).as_primitive::<Int64Type>();
    let c2 = result.column(2).as_primitive::<Float64Type>();
    for row in 0..result.num_rows() {
        let g = ((a.value(row) + 7) / 3) as usize;
        let want = fixture_row(g);
        assert_eq!(a.value(row), want.a);
        assert_eq!(c1.value(row), want.c, "row {row}");
        assert_eq!(c2.value(row), want.c, "row {row}");
    }
}

#[test]
fn nulls_survive_scan_and_projection() {
    let (_dir, format) = fixture();
    let result = run("SELECT b, a FROM t", &format, &PipelineDriver::default());
    let b = result.column(0).as_string::<i32>();
    let a = result.column(1).as_primitive::<Int64Type>();
    let mut nulls = 0;
    for row in 0..result.num_rows() {
        let g = ((a.value(row) + 7) / 3) as usize;
        let want = fixture_row(g);
        match want.b {
            None => {
                assert!(b.is_null(row), "row {row} (g={g}) should be NULL");
                nulls += 1;
            }
            Some(s) => assert_eq!(b.value(row), s),
        }
    }
    assert_eq!(nulls, (0..SPEC.total_rows()).filter(|g| g % 7 == 6).count());
}

#[test]
fn morsels_respect_row_and_byte_targets() {
    let (_dir, format) = fixture();
    let by_rows = PipelineDriver {
        morsel_rows: 1000,
        ..PipelineDriver::default()
    };
    let pipeline = plan("SELECT * FROM t", &format).unwrap();
    let batches = by_rows.run(&pipeline).unwrap();
    assert!(batches.iter().all(|b| b.num_rows() <= 1000));
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        SPEC.total_rows()
    );

    // 字节目标：默认 morsel_bytes 下估算值是充分条件，这里只验证切片逻辑不丢行
    let tiny_bytes = PipelineDriver {
        morsel_rows: 8192,
        morsel_bytes: 4096,
        ..PipelineDriver::default()
    };
    let batches = tiny_bytes.run(&pipeline).unwrap();
    assert_eq!(
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        SPEC.total_rows()
    );
}

#[test]
fn single_thread_tiny_queue_and_tiny_morsels_still_complete() {
    let (_dir, format) = fixture();
    let driver = PipelineDriver {
        compute_threads: 1,
        io_threads: 1,
        morsel_rows: 64,
        queue_depth: 1,
        morsel_bytes: 8192,
    };
    assert_eq!(
        run("SELECT * FROM t", &format, &driver).num_rows(),
        SPEC.total_rows()
    );
    assert_eq!(
        run("SELECT a FROM t LIMIT 10", &format, &driver).num_rows(),
        10
    );
}

#[test]
fn many_threads_with_small_limit_terminate_promptly() {
    let (_dir, format) = fixture();
    let driver = PipelineDriver {
        compute_threads: 16,
        morsel_rows: 16,
        queue_depth: 2,
        ..PipelineDriver::default()
    };
    assert_eq!(
        run("SELECT a FROM t LIMIT 1", &format, &driver).num_rows(),
        1
    );
}

fn pipeline_for(sql: &str, format: &ParquetDirFormat) -> Pipeline {
    plan(sql, format).unwrap()
}

#[test]
fn file_disappearing_after_planning_is_a_scan_error_not_a_panic() {
    let (dir, format) = fixture();
    let pipeline = pipeline_for("SELECT a FROM t", &format);
    std::fs::remove_file(dir.path().join("t").join("part-01.parquet")).unwrap();
    let err = PipelineDriver::default().run(&pipeline).unwrap_err();
    assert!(
        matches!(&err, ExecError::Scan { split, .. } if split.contains("part-01.parquet")),
        "{err:?}"
    );
}

#[test]
fn operator_failure_is_reported() {
    let (_dir, format) = fixture();
    let mut pipeline = pipeline_for("SELECT a FROM t", &format);
    pipeline.ops = vec![Operator::Project {
        columns: vec![42],
        schema: pipeline.output_schema.clone(),
    }];
    let err = PipelineDriver::default().run(&pipeline).unwrap_err();
    assert!(matches!(err, ExecError::Operator(_)), "{err:?}");
}

#[test]
fn corrupt_file_is_a_scan_error() {
    let (dir, format) = fixture();
    std::fs::write(
        dir.path().join("t").join("part-zz.parquet"),
        b"not a parquet file",
    )
    .unwrap();
    let pipeline = pipeline_for("SELECT a FROM t", &format);
    let err = PipelineDriver::default().run(&pipeline).unwrap_err();
    assert!(
        matches!(&err, ExecError::Scan { split, .. } if split.contains("part-zz.parquet")),
        "{err:?}"
    );
}
