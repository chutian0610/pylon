//! 测试 fixture：生成确定性的本地 Parquet 目录，供全仓各 crate 的测试复用。
//!
//! 表 schema 固定为 `a: Int64 (非空), b: Utf8 (可空), c: Float64 (非空)`；
//! 第 `g` 行（全局行号）的值由 [`fixture_row`] 给定，测试据此得到无需手写的期望结果。

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixtureSpec {
    pub files: usize,
    pub rows_per_file: usize,
}

impl FixtureSpec {
    pub fn total_rows(&self) -> usize {
        self.files * self.rows_per_file
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FixtureRow {
    pub a: i64,
    pub b: Option<String>,
    pub c: f64,
}

pub fn fixture_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Float64, false),
    ]))
}

/// 全局第 `g` 行的内容：`a` 覆盖负数，`b` 每 7 行一个 NULL，`c` 为精确可表示的小数。
pub fn fixture_row(g: usize) -> FixtureRow {
    FixtureRow {
        a: g as i64 * 3 - 7,
        b: if g % 7 == 6 {
            None
        } else {
            Some(format!("s{g}"))
        },
        c: g as f64 / 4.0,
    }
}

pub fn expected_rows(spec: FixtureSpec) -> Vec<FixtureRow> {
    (0..spec.total_rows()).map(fixture_row).collect()
}

/// 在 `<root>/<table>/part-<i>.parquet` 写入 fixture（每 1024 行一个 row group），返回表 schema。
pub fn write_fixture_table(
    root: &Path,
    table: &str,
    spec: FixtureSpec,
) -> Result<SchemaRef, Box<dyn std::error::Error>> {
    let dir = root.join(table);
    std::fs::create_dir_all(&dir)?;
    let schema = fixture_schema();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1024))
        .build();

    for f in 0..spec.files {
        let rows: Vec<FixtureRow> = (f * spec.rows_per_file..(f + 1) * spec.rows_per_file)
            .map(fixture_row)
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.a).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.b.as_deref()).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.c).collect::<Vec<_>>(),
                )),
            ],
        )?;
        let file = File::create(dir.join(format!("part-{f:02}.parquet")))?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props.clone()))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(schema)
}
