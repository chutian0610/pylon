//! Planner 测试：绑定、投影裁剪下推、Project 生成、带位置错误。

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use pylon_plan::{Operator, PlanError, plan};
use pylon_sql::Span;
use pylon_table::testing::{FixtureSpec, write_fixture_table};
use pylon_table::{ParquetDirFormat, ScanPlan, TableError, TableFormat};

fn fixture() -> (tempfile::TempDir, ParquetDirFormat) {
    let dir = tempfile::tempdir().unwrap();
    write_fixture_table(
        dir.path(),
        "t",
        FixtureSpec {
            files: 2,
            rows_per_file: 10,
        },
    )
    .unwrap();
    let format = ParquetDirFormat::new(dir.path());
    (dir, format)
}

fn names(schema: &Schema) -> Vec<&str> {
    schema.fields().iter().map(|f| f.name().as_str()).collect()
}

#[test]
fn subset_in_table_order_needs_no_project_op() {
    let (_dir, format) = fixture();
    let p = plan("SELECT a, b FROM t LIMIT 10", &format).unwrap();
    assert_eq!(p.table, "t");
    assert_eq!(p.scan.projection, vec![0, 1]);
    assert_eq!(p.scan.splits.len(), 2);
    assert_eq!(names(&p.scan_schema), ["a", "b"]);
    assert_eq!(names(&p.output_schema), ["a", "b"]);
    assert!(p.ops.is_empty(), "{:?}", p.ops);
    assert_eq!(p.limit, Some(10));
}

#[test]
fn reordered_and_duplicated_columns_get_project_op() {
    let (_dir, format) = fixture();
    let p = plan("SELECT c, a, c FROM t", &format).unwrap();
    assert_eq!(p.scan.projection, vec![0, 2]);
    assert_eq!(names(&p.scan_schema), ["a", "c"]);
    assert_eq!(names(&p.output_schema), ["c", "a", "c"]);
    assert_eq!(
        p.ops,
        vec![Operator::Project {
            columns: vec![1, 0, 1],
            schema: p.output_schema.clone()
        }]
    );
    assert_eq!(p.limit, None);
}

#[test]
fn wildcard_selects_all_columns_without_project_op() {
    let (_dir, format) = fixture();
    let p = plan("SELECT * FROM t", &format).unwrap();
    assert_eq!(p.scan.projection, vec![0, 1, 2]);
    assert_eq!(names(&p.output_schema), ["a", "b", "c"]);
    assert!(p.ops.is_empty());
}

#[test]
fn wildcard_mixed_with_columns() {
    let (_dir, format) = fixture();
    let p = plan("SELECT *, a FROM t", &format).unwrap();
    assert_eq!(p.scan.projection, vec![0, 1, 2]);
    assert_eq!(names(&p.output_schema), ["a", "b", "c", "a"]);
    assert_eq!(
        p.ops,
        vec![Operator::Project {
            columns: vec![0, 1, 2, 0],
            schema: p.output_schema.clone()
        }]
    );
}

#[test]
fn column_names_resolve_case_insensitively_to_schema_name() {
    let (_dir, format) = fixture();
    let p = plan("SELECT A FROM t", &format).unwrap();
    assert_eq!(p.scan.projection, vec![0]);
    assert_eq!(names(&p.output_schema), ["a"]);
}

#[test]
fn unknown_column_carries_span() {
    let (_dir, format) = fixture();
    let sql = "SELECT a, zz FROM t";
    let err = plan(sql, &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::ColumnNotFound { ident } if ident.name == "zz" && ident.span == Span::new(10, 12)),
        "{err:?}"
    );
    let rendered = err.render(sql);
    assert!(
        rendered.starts_with("error: column not found: zz\n"),
        "{rendered}"
    );
    assert!(rendered.contains("line 1, column 11"), "{rendered}");
    assert!(
        rendered.ends_with("   | SELECT a, zz FROM t\n   |           ^^"),
        "{rendered}"
    );
}

#[test]
fn unknown_table_carries_span() {
    let (_dir, format) = fixture();
    let sql = "SELECT a FROM nope";
    let err = plan(sql, &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::TableNotFound { ident } if ident.name == "nope" && ident.span == Span::new(14, 18)),
        "{err:?}"
    );
    assert!(err.render(sql).contains("line 1, column 15"));
}

#[test]
fn syntax_errors_pass_through() {
    let (_dir, format) = fixture();
    let err = plan("SELECT FROM t", &format).unwrap_err();
    assert!(matches!(err, PlanError::Syntax(_)));
    assert!(err.render("SELECT FROM t").starts_with("syntax error: "));
}

#[test]
fn empty_table_error_passes_through_without_position() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("e")).unwrap();
    let format = ParquetDirFormat::new(dir.path());
    let err = plan("SELECT a FROM e", &format).unwrap_err();
    assert!(matches!(err, PlanError::Table(TableError::EmptyTable(_))));
    assert_eq!(err.span(), None);
    assert_eq!(
        err.render("SELECT a FROM e"),
        "error: table 'e' has no Parquet files"
    );
}

/// TableFormat 若违反 scan 投影契约（未按表序去重返回），planner 必须拒绝而非静默错位。
struct BadFormat;

impl TableFormat for BadFormat {
    fn name(&self) -> &'static str {
        "bad"
    }
    fn table_schema(&self, _table: &str) -> Result<SchemaRef, TableError> {
        Ok(Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ])))
    }
    fn scan(&self, _table: &str, _projection: Option<&[usize]>) -> Result<ScanPlan, TableError> {
        Ok(ScanPlan {
            projection: vec![1, 0],
            splits: vec![],
        })
    }
}

#[test]
fn contract_violation_by_table_format_is_internal_error() {
    let err = plan("SELECT a, b FROM t", &BadFormat).unwrap_err();
    assert!(
        matches!(&err, PlanError::Internal(msg) if msg.contains("projection")),
        "{err:?}"
    );
}

// ---- 语法已解析、执行未跟上（M1/T2 后续 ticket）：带位置的 Unsupported ----

#[test]
fn where_is_parsed_but_not_planned_yet() {
    let (_dir, format) = fixture();
    let sql = "SELECT a FROM t WHERE a > 1";
    let err = plan(sql, &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Unsupported { span, what } if *span == Span::new(22, 27) && *what == "WHERE"),
        "{err:?}"
    );
    let rendered = err.render(sql);
    assert!(
        rendered.starts_with("error: WHERE is not supported yet\n"),
        "{rendered}"
    );
    assert!(rendered.contains("line 1, column 23"), "{rendered}");
}

#[test]
fn aggregate_projection_is_not_planned_yet() {
    let (_dir, format) = fixture();
    let err = plan("SELECT COUNT(*) FROM t", &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Unsupported { span, what } if *span == Span::new(7, 15) && *what == "aggregate functions"),
        "{err:?}"
    );
}

#[test]
fn arithmetic_projection_is_not_planned_yet() {
    let (_dir, format) = fixture();
    let sql = "SELECT a + 1 FROM t";
    let err = plan(sql, &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Unsupported { span, what } if *span == Span::new(7, 12) && *what == "expressions in SELECT"),
        "{err:?}"
    );
    assert!(err.render(sql).contains("line 1, column 8"), "line check");
}

#[test]
fn group_order_distinct_are_not_planned_yet() {
    let (_dir, format) = fixture();
    let err = plan("SELECT a FROM t GROUP BY a", &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Unsupported { what, .. } if *what == "GROUP BY"),
        "{err:?}"
    );

    let err = plan("SELECT a FROM t ORDER BY a", &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Unsupported { what, .. } if *what == "ORDER BY"),
        "{err:?}"
    );

    let err = plan("SELECT DISTINCT a FROM t", &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Unsupported { span, .. } if *span == Span::new(7, 15)),
        "{err:?}"
    );
}

#[test]
fn show_tables_and_describe_are_not_planned_yet() {
    let (_dir, format) = fixture();
    let err = plan("SHOW TABLES", &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Unsupported { span, what } if *span == Span::new(0, 11) && *what == "SHOW TABLES"),
        "{err:?}"
    );

    let err = plan("DESCRIBE t", &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Unsupported { what, .. } if *what == "DESCRIBE"),
        "{err:?}"
    );
}
