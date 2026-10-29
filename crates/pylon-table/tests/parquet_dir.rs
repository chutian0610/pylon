//! TableFormat 契约测试（以 ParquetDirFormat 为首个实现）。

use std::path::PathBuf;

use arrow::datatypes::DataType;
use futures_util::StreamExt;
use pylon_table::testing::{FixtureSpec, write_fixture_table};
use pylon_table::{ParquetDirFormat, TableError, TableFormat};

const SPEC: FixtureSpec = FixtureSpec {
    files: 3,
    rows_per_file: 50,
};

fn fixture() -> (tempfile::TempDir, ParquetDirFormat) {
    let dir = tempfile::tempdir().unwrap();
    write_fixture_table(dir.path(), "t", SPEC).unwrap();
    // 非 Parquet 文件必须被忽略
    std::fs::write(dir.path().join("t").join("_SUCCESS"), b"").unwrap();
    let format = ParquetDirFormat::new(dir.path());
    (dir, format)
}

#[test]
fn table_schema_is_arrow_schema_of_files() {
    let (_dir, format) = fixture();
    let schema = format.table_schema("t").unwrap();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["a", "b", "c"]);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert!(schema.field(1).is_nullable());
    assert_eq!(schema.field(2).data_type(), &DataType::Float64);
}

#[test]
fn scan_lists_one_split_per_parquet_file_in_order() {
    let (dir, format) = fixture();
    let plan = format.scan("t", None).unwrap();
    assert_eq!(plan.projection, vec![0, 1, 2]);
    let described: Vec<String> = plan.splits.iter().map(|s| s.describe()).collect();
    let expected: Vec<String> = (0..3)
        .map(|i| {
            PathBuf::from(dir.path())
                .join("t")
                .join(format!("part-{i:02}.parquet"))
                .display()
                .to_string()
        })
        .collect();
    assert_eq!(described, expected);
}

#[test]
fn scan_pushes_down_deduplicated_sorted_projection() {
    let (_dir, format) = fixture();
    let plan = format.scan("t", Some(&[2, 0, 2])).unwrap();
    assert_eq!(plan.projection, vec![0, 2]);
}

#[test]
fn scan_rejects_out_of_range_projection() {
    let (_dir, format) = fixture();
    let err = format.scan("t", Some(&[0, 3])).unwrap_err();
    assert!(
        matches!(
            err,
            TableError::ColumnOutOfRange {
                index: 3,
                width: 3,
                ..
            }
        ),
        "{err:?}"
    );
}

/// 接缝核心契约：split 打开为 Morsel 流，产出投影后的 schema、行数受 morsel_rows 约束。
#[tokio::test]
async fn split_opens_into_morsel_stream_with_projection_and_row_cap() {
    let (_dir, format) = fixture();
    let plan = format.scan("t", Some(&[2, 0])).unwrap();
    assert_eq!(plan.splits.len(), 3);

    let mut total = 0usize;
    let mut saw_cap = false;
    for split in &plan.splits {
        let mut stream = split.open(8).await.unwrap();
        while let Some(morsel) = stream.next().await {
            let morsel = morsel.unwrap();
            assert!(morsel.num_rows() <= 8);
            saw_cap |= morsel.num_rows() == 8;
            let names: Vec<&str> = morsel
                .schema_ref()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            assert_eq!(names, ["a", "c"], "投影子集且保持表序");
            total += morsel.num_rows();
        }
    }
    assert_eq!(total, SPEC.total_rows());
    assert!(saw_cap, "morsel 行数上限应当真实生效");
}

#[tokio::test]
async fn split_open_error_names_the_file() {
    let (dir, format) = fixture();
    let plan = format.scan("t", None).unwrap();
    std::fs::remove_file(dir.path().join("t").join("part-01.parquet")).unwrap();
    let err = plan.splits[1].open(8).await.err().unwrap();
    assert!(
        matches!(&err, TableError::Scan { path, .. } if path.ends_with("part-01.parquet")),
        "{err:?}"
    );
}

#[test]
fn unknown_table_is_not_found() {
    let (_dir, format) = fixture();
    assert!(
        matches!(format.table_schema("nope"), Err(TableError::TableNotFound(n)) if n == "nope")
    );
    assert!(matches!(
        format.scan("nope", None),
        Err(TableError::TableNotFound(_))
    ));
}

#[test]
fn table_names_cannot_escape_root() {
    let (_dir, format) = fixture();
    for name in ["../t", "t/../t", "..", ".", "", "a\\b"] {
        assert!(
            matches!(format.table_schema(name), Err(TableError::TableNotFound(_))),
            "{name:?} should not resolve"
        );
    }
}

#[test]
fn directory_without_parquet_files_is_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("empty")).unwrap();
    std::fs::write(dir.path().join("empty").join("notes.txt"), b"x").unwrap();
    let format = ParquetDirFormat::new(dir.path());
    assert!(matches!(format.table_schema("empty"), Err(TableError::EmptyTable(n)) if n == "empty"));
}

#[test]
fn format_reports_its_name() {
    let (_dir, format) = fixture();
    assert_eq!(format.name(), "parquet-dir");
}
