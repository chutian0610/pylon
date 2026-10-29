//! Planner：AST → 可执行 [`Pipeline`]（M1/T1 最小规划器）。
//!
//! 当前职责：表/列名绑定、投影裁剪下推到 TableFormat scan、LIMIT 传递。
//! 逻辑优化规则（谓词下推、常量折叠）与多算子物理计划留给后续 ticket；此处的
//! Pipeline 是一条线性 Push Pipeline：Scan → ops… → Sink（LIMIT 在 Sink 生效）。

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use pylon_sql::{Ident, Query, SelectItem, Span, SyntaxError, render_at};
use pylon_table::{ScanPlan, TableError, TableFormat, normalize_projection};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    /// 按 `columns`（输入列索引，可重复）选取/重排列，输出 schema 为 `schema`。
    Project {
        columns: Vec<usize>,
        schema: SchemaRef,
    },
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

pub fn plan(sql: &str, format: &dyn TableFormat) -> Result<Pipeline, PlanError> {
    let query = pylon_sql::parse(sql)?;
    plan_query(&query, format)
}

pub fn plan_query(query: &Query, format: &dyn TableFormat) -> Result<Pipeline, PlanError> {
    let table = &query.from;
    let schema = format.table_schema(&table.name).map_err(|e| match e {
        TableError::TableNotFound(_) => PlanError::TableNotFound {
            ident: table.clone(),
        },
        other => PlanError::Table(other),
    })?;

    // 绑定投影：每个输出列对应表 schema 的一个列索引（允许重复）
    let mut selected = Vec::new();
    for item in &query.items {
        match item {
            SelectItem::Wildcard(_) => selected.extend(0..schema.fields().len()),
            SelectItem::Column(ident) => selected.push(resolve_column(&schema, ident)?),
        }
    }

    // 列裁剪下推：scan 只读被引用的列
    let pruned = normalize_projection(&table.name, Some(&selected), schema.fields().len())
        .map_err(PlanError::Table)?;
    let scan = format.scan(&table.name, Some(&pruned))?;
    if scan.projection != pruned {
        return Err(PlanError::Internal(format!(
            "TableFormat '{}' returned projection {:?}, expected {:?}",
            format.name(),
            scan.projection,
            pruned
        )));
    }
    let scan_schema = Arc::new(
        schema
            .project(&scan.projection)
            .map_err(|e| PlanError::Internal(e.to_string()))?,
    );

    // Project：把 scan 产出（表序子集）映射为查询给定的输出顺序
    let columns = selected
        .iter()
        .map(|c| scan.projection.binary_search(c))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| PlanError::Internal("selected column missing from scan projection".into()))?;
    let output_fields: Vec<_> = selected
        .iter()
        .map(|&c| schema.fields()[c].clone())
        .collect();
    let output_schema = Arc::new(Schema::new(output_fields));

    let is_identity =
        columns.len() == scan.projection.len() && columns.iter().enumerate().all(|(i, &c)| i == c);
    let ops = if is_identity {
        Vec::new()
    } else {
        vec![Operator::Project {
            columns,
            schema: output_schema.clone(),
        }]
    };

    Ok(Pipeline {
        table: table.name.clone(),
        scan,
        scan_schema,
        ops,
        output_schema,
        limit: query.limit.as_ref().map(|l| l.n),
    })
}

/// 列名绑定：精确匹配优先；否则按 ASCII 不区分大小写唯一匹配；多个候选报歧义。
fn resolve_column(schema: &Schema, ident: &Ident) -> Result<usize, PlanError> {
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
