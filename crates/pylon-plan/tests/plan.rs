//! Planner 测试（M1/T3）：绑定、三条优化规则的计划快照、物理 lowering、
//! 带位置错误。快照断言走 [`pylon_plan::render`]，与 `--explain` 同一渲染。

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use pylon_plan::{Operator, PlanError, plan, plan_explain, render};
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

/// 优化后的逻辑计划文本（做规则断言用）。
fn after(sql: &str, format: &ParquetDirFormat) -> String {
    render::logical(&plan_explain(sql, format).unwrap().logical_after)
}

fn physical_text(sql: &str, format: &ParquetDirFormat) -> String {
    render::physical(&plan_explain(sql, format).unwrap().pipeline.unwrap())
}

// ---- 既有行为：投影 + LIMIT（T1 基线不回归）----

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
        physical_text("SELECT c, a, c FROM t", &format),
        "Pipeline scan=t [a, c] splits=2\n  Project [c, a, c]\n"
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
fn column_names_resolve_case_insensitively_to_schema_name() {
    let (_dir, format) = fixture();
    let p = plan("SELECT A FROM t", &format).unwrap();
    assert_eq!(p.scan.projection, vec![0]);
    assert_eq!(names(&p.output_schema), ["a"]);
}

// ---- 规则 1：常量折叠 ----

#[test]
fn constant_folds_in_predicate_and_projection() {
    let (_dir, format) = fixture();
    let text = after("SELECT a FROM t WHERE a > 1 + 2", &format);
    assert!(
        text.contains("Filter (a > 3)"),
        "1 + 2 应折叠为 3：\n{text}"
    );
    assert!(!text.contains("1 + 2"), "折叠后不应残留原式：\n{text}");

    let text = after("SELECT 1 + 2 FROM t", &format);
    assert!(
        text.contains("Project [3 AS 1 + 2]"),
        "常量折叠生效、输出名保留原文：\n{text}"
    );
}

#[test]
fn fold_eliminates_true_conjunct() {
    let (_dir, format) = fixture();
    let sql = "SELECT a FROM t WHERE a > 0 AND 1 = 1";
    let text = after(sql, &format);
    assert!(
        text.contains("Filter (a > 0)"),
        "恒真合取项应被折叠掉：\n{text}"
    );
}

// ---- 规则 2：投影裁剪 ----

#[test]
fn pruning_keeps_only_referenced_columns() {
    let (_dir, format) = fixture();
    let sql = "SELECT a FROM t WHERE c > 1.5";
    let text = after(sql, &format);
    assert!(
        text.contains("Scan t [a, c]"),
        "未被引用的 b 不应出现在 scan：\n{text}"
    );
    let p = plan(sql, &format).unwrap();
    assert_eq!(p.scan.projection, vec![0, 2]);
}

#[test]
fn pruning_remaps_expression_indices() {
    let (_dir, format) = fixture();
    // scan 投影裁剪后表达式索引须随之重映射
    let p = plan("SELECT c * 2 FROM t WHERE c > 0.5", &format).unwrap();
    assert_eq!(p.scan.projection, vec![2]);
    assert_eq!(
        physical_text("SELECT c * 2 FROM t WHERE c > 0.5", &format),
        "Pipeline scan=t [c] splits=2\n  Filter (c > 0.5)\n  Project [c * 2]\n"
    );
}

// ---- 规则 3：谓词下推 ----

#[test]
fn simple_comparisons_are_pushed_into_scan_hints() {
    let (_dir, format) = fixture();
    let sql = "SELECT a FROM t WHERE a > 5 AND b = 'x'";
    let text = after(sql, &format);
    assert!(
        text.contains("pushed=[a > 5 AND b = 'x']"),
        "简单比较应进入 scan 下推提示：\n{text}"
    );
    // 残差 Filter 保留（提示非精确，正确性由它保证）
    assert!(text.contains("Filter (a > 5 AND b = 'x')"), "{text}");
}

#[test]
fn folded_comparison_pushes_like_q1_shape() {
    let (_dir, format) = fixture();
    // 比较右侧折叠成标量后即可下推（TPC-H Q1 的 date-interval 同构形态）
    let sql = "SELECT a FROM t WHERE a >= 10 - 3";
    let text = after(sql, &format);
    assert!(text.contains("pushed=[a >= 7]"), "{text}");
}

#[test]
fn non_pushable_conjuncts_stay_residual_only() {
    let (_dir, format) = fixture();
    let sql = "SELECT a FROM t WHERE a + 1 > 5 AND a IS NULL";
    let text = after(sql, &format);
    assert!(
        !text.contains("pushed="),
        "无 `列 op 标量` 形态的谓词不应下推：\n{text}"
    );
    assert!(text.contains("Filter"), "{text}");
}

// ---- BETWEEN 脱糖 ----

#[test]
fn between_desugars_to_two_comparisons() {
    let (_dir, format) = fixture();
    let text = after("SELECT a FROM t WHERE a BETWEEN 1 AND 3", &format);
    assert!(
        text.contains("a >= 1 AND a <= 3"),
        "BETWEEN 应脱糖为 >= AND <=：\n{text}"
    );
    assert!(text.contains("pushed=[a >= 1 AND a <= 3]"), "{text}");
}

// ---- 物理计划：WHERE / 表达式投影实际可执行形态 ----

#[test]
fn where_becomes_filter_operator() {
    let (_dir, format) = fixture();
    let p = plan("SELECT a FROM t WHERE a > 5", &format).unwrap();
    assert_eq!(p.scan.projection, vec![0]);
    assert_eq!(p.ops.len(), 1);
    assert!(matches!(&p.ops[0], Operator::Filter { .. }));
    assert_eq!(p.limit, None);
}

#[test]
fn arithmetic_projection_becomes_project_operator() {
    let (_dir, format) = fixture();
    let p = plan("SELECT a + a FROM t LIMIT 4", &format).unwrap();
    assert_eq!(p.scan.projection, vec![0]);
    assert!(matches!(&p.ops[0], Operator::Project { .. }), "{:?}", p.ops);
    assert_eq!(names(&p.output_schema), ["a + a"]);
    assert_eq!(p.limit, Some(4));
}

// ---- 优化前后两级计划（--explain 的数据源）----

#[test]
fn explain_keeps_both_logical_levels() {
    let (_dir, format) = fixture();
    let e = plan_explain("SELECT a FROM t WHERE a > 1 + 2", &format).unwrap();
    let before = render::logical(&e.logical_before);
    let after_text = render::logical(&e.logical_after);
    assert!(before.contains("1 + 2"), "优化前保留原式：\n{before}");
    assert!(after_text.contains("a > 3"), "优化后已折叠：\n{after_text}");
    assert_eq!(
        render::physical(&e.pipeline.unwrap()),
        "Pipeline scan=t [a] splits=2\n  Filter (a > 3)\n"
    );
}

// ---- 绑定/校验错误：带位置 ----

#[test]
fn unknown_column_carries_span() {
    let (_dir, format) = fixture();
    let sql = "SELECT a, zz FROM t";
    let err = plan(sql, &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::ColumnNotFound { ident } if ident.name == "zz" && ident.span == Span::new(10, 12)),
        "{err:?}"
    );
    assert!(err.render(sql).contains("line 1, column 11"));
}

#[test]
fn unknown_table_carries_span() {
    let (_dir, format) = fixture();
    let sql = "SELECT a FROM nope";
    let err = plan(sql, &format).unwrap_err();
    assert!(matches!(&err, PlanError::TableNotFound { ident } if ident.name == "nope"));
    assert!(err.render(sql).contains("line 1, column 15"));
}

#[test]
fn type_mismatch_is_a_positional_bind_error() {
    let (_dir, format) = fixture();
    let sql = "SELECT a + b FROM t";
    let err = plan(sql, &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Bind { span, message } if *span == Span::new(7, 12) && message.contains("cannot apply + to Int64 and Utf8")),
        "{err:?}"
    );
    let rendered = err.render(sql);
    assert!(rendered.contains("line 1, column 8"), "{rendered}");
    assert!(
        rendered.ends_with("   | SELECT a + b FROM t\n   |        ^^^^^"),
        "{rendered}"
    );
}

#[test]
fn invalid_date_literal_is_positional() {
    let (_dir, format) = fixture();
    let sql = "SELECT DATE '2023-02-29' FROM t";
    let err = plan(sql, &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Bind { message, .. } if message.contains("invalid DATE literal '2023-02-29'")),
        "{err:?}"
    );
}

#[test]
fn aggregates_are_rejected_in_where_and_group_by() {
    let (_dir, format) = fixture();
    let err = plan("SELECT a FROM t WHERE SUM(a) > 1", &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Bind { message, .. } if message.contains("not allowed in WHERE")),
        "{err:?}"
    );
    let err = plan("SELECT a FROM t GROUP BY SUM(a)", &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Bind { message, .. } if message.contains("not allowed in GROUP BY")),
        "{err:?}"
    );
}

#[test]
fn bare_column_outside_group_keys_is_rejected() {
    let (_dir, format) = fixture();
    let sql = "SELECT a FROM t GROUP BY b";
    let err = plan(sql, &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Bind { span, message } if *span == Span::new(7, 8) && message.contains("must appear in the GROUP BY")),
        "{err:?}"
    );
}

#[test]
fn order_by_must_reference_output_columns() {
    let (_dir, format) = fixture();
    let sql = "SELECT a FROM t ORDER BY b";
    let err = plan(sql, &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::ColumnNotFound { ident, .. } if ident.name == "b"),
        "{err:?}"
    );
}

#[test]
fn wildcard_with_group_by_is_rejected() {
    let (_dir, format) = fixture();
    let err = plan("SELECT * FROM t GROUP BY a", &format).unwrap_err();
    assert!(
        matches!(&err, PlanError::Bind { message, .. } if message.contains("SELECT *")),
        "{err:?}"
    );
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

// ---- 语法已解析、执行未跟上（T4 前的物理化拒绝）----

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
        matches!(&err, PlanError::Unsupported { span, what } if *span == Span::new(7, 15) && *what == "DISTINCT"),
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

/// 聚合查询的逻辑计划完整可解释（物理拒绝不影响 --explain 逻辑部分）。
#[test]
fn aggregate_logical_plan_is_explainable() {
    let (_dir, format) = fixture();
    let sql = "SELECT b, COUNT(*), SUM(a * 2) FROM t WHERE a > 0 GROUP BY b";
    let e = plan_explain(sql, &format).unwrap();
    let text = render::logical(&e.logical_after);
    assert!(
        text.contains("Aggregate keys=[b] aggs=[count(*), sum(a * 2)]"),
        "{text}"
    );
    assert!(text.contains("Filter (a > 0)"), "{text}");
    assert!(text.contains("Scan t [a, b] pushed=[a > 0]"), "{text}");
    // 物理化拒绝但带聚合位置（--explain 仍可用）
    let err = e.pipeline.unwrap_err();
    assert!(matches!(
        err,
        PlanError::Unsupported {
            what: "aggregate functions",
            ..
        }
    ));
    let err = plan(sql, &format).unwrap_err();
    assert!(matches!(
        err,
        PlanError::Unsupported {
            what: "aggregate functions",
            ..
        }
    ));
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
    fn scan(
        &self,
        _table: &str,
        _projection: Option<&[usize]>,
        _filters: &[pylon_table::ScanPredicate],
    ) -> Result<ScanPlan, TableError> {
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

// ---- 评审修复的回归：裁剪重映射不得越界到投影输出（结构断言，渲染按名会掩盖）----

/// 取树中第一个 distinct 聚合的键列索引。
fn distinct_key_indices(plan: &pylon_plan::logical::LogicalPlan) -> Vec<usize> {
    use pylon_plan::logical::LogicalPlan;
    let mut node = Some(plan);
    while let Some(n) = node {
        if let LogicalPlan::Aggregate {
            distinct: true,
            keys,
            ..
        } = n
        {
            return keys
                .iter()
                .map(|k| match &k.expr {
                    pylon_expr::Expr::Column { index, .. } => *index,
                    other => panic!("DISTINCT 键应为列引用：{other:?}"),
                })
                .collect();
        }
        node = n.input().map(|i| i.as_ref());
    }
    panic!("没有 DISTINCT 聚合节点");
}

#[test]
fn pruning_does_not_remap_distinct_keys() {
    // b/c 的 scan 相对索引是 0/1（a 被裁掉），DISTINCT 键引用 Project 输出 0/1——
    // 若误用 scan 映射重写，c 会指向 b（分组退化为按 b）
    let (_dir, format) = fixture();
    let e = plan_explain("SELECT DISTINCT b, c FROM t", &format).unwrap();
    assert_eq!(distinct_key_indices(&e.logical_after), vec![0, 1]);
    let text = render::logical(&e.logical_after);
    assert!(text.contains("Scan t [b, c]"), "{text}");
}

#[test]
fn pruning_does_not_remap_sort_keys() {
    let (_dir, format) = fixture();
    let e = plan_explain("SELECT b, c FROM t ORDER BY c", &format).unwrap();
    use pylon_plan::logical::LogicalPlan;
    let mut node = Some(&e.logical_after);
    while let Some(n) = node {
        if let LogicalPlan::Sort { keys, .. } = n {
            assert_eq!(keys.len(), 1);
            match &keys[0].expr {
                pylon_expr::Expr::Column { index, name, .. } => {
                    assert_eq!((name.as_ref(), *index), ("c", 1));
                }
                other => panic!("Sort 键应为列引用：{other:?}"),
            }
            return;
        }
        node = n.input().map(|i| i.as_ref());
    }
    panic!("没有 Sort 节点");
}

#[test]
fn pushed_predicates_use_table_schema_indices() {
    // TableFormat 契约：ScanPredicate.column 是表 schema 索引。裁剪后 scan 相对
    // 索引 ≠ 表索引（c 的表索引是 2），T5 adapter 按契约实现会据此读列
    let (_dir, format) = fixture();
    let e = plan_explain("SELECT a FROM t WHERE c > 1.5", &format).unwrap();
    use pylon_plan::logical::LogicalPlan;
    let mut node = Some(&e.logical_after);
    while let Some(n) = node {
        if let LogicalPlan::Scan {
            filters, columns, ..
        } = n
        {
            assert_eq!(columns, &vec![0, 2], "scan 读取 a(0) 与 c(2)");
            assert_eq!(filters.len(), 1);
            assert_eq!(filters[0].column, 2, "下推提示用表索引定位 c");
            return;
        }
        node = n.input().map(|i| i.as_ref());
    }
    panic!("没有 Scan 节点");
}
