//! Planner：AST → LogicalPlan →（折叠/裁剪/下推）→ PhysicalPlan（M1/T3）。
//!
//! 职责分层：
//! - [`lower`]（私有）：绑定与校验（列名、类型裁决、聚合提取、GROUP BY 合法性），
//!   错误带位置；
//! - [`rules`]（私有）：三条优化规则，纯函数重写逻辑计划；
//! - 物理 lowering：逻辑树 → 线性 [`Pipeline`]（Scan → Filter → Project → Sink，
//!   LIMIT 在 Sink 生效）。Aggregate/Sort 的物理算子随 T4 加入，物理化拒绝时返回
//!   带位置的 [`PlanError::Unsupported`]。
//!
//! 规格级接缝不变：SQL 文本进，Arrow 结果（或带位置用户可见错误）出。

pub mod logical;
mod lower;
pub mod render;
mod rules;

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use pylon_expr::Expr;
use pylon_sql::{Ident, Span, Statement, SyntaxError, render_at};
use pylon_table::{ScanPlan, TableError, TableFormat, normalize_projection};

use logical::LogicalPlan;

/// 物理计划：一条线性 Push Pipeline。执行器（pylon-exec）直接消费。
#[derive(Debug)]
pub struct Pipeline {
    pub table: String,
    pub scan: ScanPlan,
    /// Scan 产出的 morsel schema：表 schema 在 `scan.projection` 上的子集（表序）。
    pub scan_schema: SchemaRef,
    pub ops: Vec<Operator>,
    pub output_schema: SchemaRef,
    pub limit: Option<u64>,
}

/// Pipeline 中的算子，按数据流向排列。
#[derive(Debug, Clone)]
pub enum Operator {
    /// 谓词过滤：表达式须产出 Boolean。
    Filter { predicate: Expr },
    /// 表达式投影：逐列求值，输出 schema 由 `schema` 给定。
    Project { exprs: Vec<Expr>, schema: SchemaRef },
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error(transparent)]
    Syntax(#[from] SyntaxError),
    #[error("table not found: {}", ident.name)]
    TableNotFound { ident: Ident },
    #[error("column not found: {}", ident.name)]
    ColumnNotFound { ident: Ident },
    #[error("column reference is ambiguous: {}", ident.name)]
    AmbiguousColumn { ident: Ident },
    /// 绑定/校验错误（类型不兼容、非法聚合位置、无效字面量…），带位置。
    #[error("{message}")]
    Bind { span: Span, message: String },
    /// 语法已解析、执行能力未跟上（M1 后续 ticket）：指向对应构造的位置
    #[error("{what} is not supported yet")]
    Unsupported { span: Span, what: &'static str },
    #[error(transparent)]
    Table(#[from] TableError),
    #[error("internal planner error: {0}")]
    Internal(String),
}

impl PlanError {
    pub fn span(&self) -> Option<Span> {
        match self {
            PlanError::Syntax(e) => Some(e.span),
            PlanError::TableNotFound { ident }
            | PlanError::ColumnNotFound { ident }
            | PlanError::AmbiguousColumn { ident } => Some(ident.span),
            PlanError::Bind { span, .. } | PlanError::Unsupported { span, .. } => Some(*span),
            PlanError::Table(_) | PlanError::Internal(_) => None,
        }
    }

    /// 渲染为用户可见错误；带位置的错误附源码行与脱字符。
    pub fn render(&self, source: &str) -> String {
        match self {
            PlanError::Syntax(e) => e.render(source),
            other => match other.span() {
                Some(span) => render_at(source, span, &format!("error: {other}")),
                None => format!("error: {other}"),
            },
        }
    }
}

/// 计划一条查询：解析 → 绑定 → 优化 → 物理 lowering。
pub fn plan(sql: &str, format: &dyn TableFormat) -> Result<Pipeline, PlanError> {
    let (_, after) = lower_and_optimize(sql, format)?;
    physical(&after, format)
}

/// 计划一条查询并保留两级逻辑计划与物理化结果（`--explain` 用）。
///
/// 与 [`plan`] 不同：物理化失败（如聚合在 T4 前无算子）不整体报错——explain 的
/// 价值恰在于此，能单独展示逻辑计划与优化效果。
pub fn plan_explain(sql: &str, format: &dyn TableFormat) -> Result<ExplainPlan, PlanError> {
    let (before, after) = lower_and_optimize(sql, format)?;
    let pipeline = physical(&after, format);
    Ok(ExplainPlan {
        logical_before: before,
        logical_after: after,
        pipeline,
    })
}

pub struct ExplainPlan {
    /// 优化前的逻辑计划（绑定后）
    pub logical_before: LogicalPlan,
    /// 优化后的逻辑计划
    pub logical_after: LogicalPlan,
    /// 物理计划；Err = 物理化拒绝（Aggregate/Sort 随 T4 加入），带位置
    pub pipeline: Result<Pipeline, PlanError>,
}

/// 解析 → 绑定 → 优化，返回优化前后两级逻辑计划（`plan` 与 `plan_explain` 共用）。
fn lower_and_optimize(
    sql: &str,
    format: &dyn TableFormat,
) -> Result<(LogicalPlan, LogicalPlan), PlanError> {
    let query = match pylon_sql::parse(sql)? {
        Statement::Query(q) => q,
        Statement::ShowTables(span) => {
            return Err(PlanError::Unsupported {
                span,
                what: "SHOW TABLES",
            });
        }
        Statement::Describe { table } => {
            return Err(PlanError::Unsupported {
                span: table.span,
                what: "DESCRIBE",
            });
        }
    };

    let table = &query.from;
    let table_schema = format.table_schema(&table.name).map_err(|e| match e {
        TableError::TableNotFound(_) => PlanError::TableNotFound {
            ident: table.clone(),
        },
        other => PlanError::Table(other),
    })?;

    let before = lower::lower(&query, table_schema)?;
    let after = rules::optimize(before.clone());
    Ok((before, after))
}

/// 物理 lowering：线性树 → Pipeline。遇到 Aggregate/Sort 即拒绝（T4 前无对应算子）。
fn physical(plan: &LogicalPlan, format: &dyn TableFormat) -> Result<Pipeline, PlanError> {
    let mut limit = None;
    let mut project: Option<(Vec<Expr>, SchemaRef)> = None;
    let mut filters: Vec<Expr> = Vec::new();
    let mut node = plan;

    loop {
        match node {
            LogicalPlan::Limit { n, input } => {
                limit = Some(*n);
                node = input;
            }
            LogicalPlan::Project {
                exprs,
                schema,
                input,
            } => {
                let plain: Vec<Expr> = exprs.iter().map(|e| e.expr.clone()).collect();
                project = Some((plain, schema.clone()));
                node = input;
            }
            LogicalPlan::Filter { predicate, input } => {
                filters.push(predicate.clone());
                node = input;
            }
            LogicalPlan::Aggregate {
                span,
                distinct,
                aggs,
                ..
            } => {
                let what = if *distinct {
                    "DISTINCT"
                } else if !aggs.is_empty() {
                    "aggregate functions"
                } else {
                    "GROUP BY"
                };
                return Err(PlanError::Unsupported { span: *span, what });
            }
            LogicalPlan::Sort { span, .. } => {
                return Err(PlanError::Unsupported {
                    span: *span,
                    what: "ORDER BY",
                });
            }
            LogicalPlan::Scan { .. } => break,
        }
    }

    let LogicalPlan::Scan {
        table,
        columns,
        table_schema,
        filters: pushed,
        ..
    } = node
    else {
        return Err(PlanError::Internal("physical lowering 丢失 Scan".into()));
    };

    let pruned = normalize_projection(table, Some(columns), table_schema.fields().len())
        .map_err(PlanError::Table)?;
    let scan = format.scan(table, Some(&pruned), pushed)?;
    if scan.projection != pruned {
        return Err(PlanError::Internal(format!(
            "TableFormat '{}' returned projection {:?}, expected {:?}",
            format.name(),
            scan.projection,
            pruned
        )));
    }
    let scan_schema = Arc::new(
        table_schema
            .project(&scan.projection)
            .map_err(|e| PlanError::Internal(e.to_string()))?,
    );

    let mut ops: Vec<Operator> = filters
        .into_iter()
        .map(|predicate| Operator::Filter { predicate })
        .collect();

    let output_schema = match project {
        Some((exprs, schema)) => {
            // 恒等投影（列引用按序取全部 scan 列）不生成算子，输出即 scan schema
            // （列名归一到表 schema 的大小写）
            let identity = exprs.len() == scan_schema.fields().len()
                && exprs
                    .iter()
                    .enumerate()
                    .all(|(i, e)| matches!(e, Expr::Column { index, .. } if *index == i));
            if !identity {
                ops.push(Operator::Project {
                    exprs,
                    schema: schema.clone(),
                });
                schema
            } else {
                scan_schema.clone()
            }
        }
        None => scan_schema.clone(),
    };

    Ok(Pipeline {
        table: table.clone(),
        scan,
        scan_schema,
        ops,
        output_schema,
        limit,
    })
}

/// 列名绑定：精确匹配优先；否则按 ASCII 不区分大小写唯一匹配；多个候选报歧义。
pub fn resolve_column(schema: &Schema, ident: &Ident) -> Result<usize, PlanError> {
    let fields = schema.fields();
    if let Some(i) = fields.iter().position(|f| f.name() == &ident.name) {
        return Ok(i);
    }
    let mut candidates = fields
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name().eq_ignore_ascii_case(&ident.name))
        .map(|(i, _)| i);
    match (candidates.next(), candidates.next()) {
        (Some(i), None) => Ok(i),
        (Some(_), Some(_)) => Err(PlanError::AmbiguousColumn {
            ident: ident.clone(),
        }),
        (None, _) => Err(PlanError::ColumnNotFound {
            ident: ident.clone(),
        }),
    }
}
