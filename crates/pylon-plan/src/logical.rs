//! 逻辑计划：AST lowering 的目标形态，优化规则的重写对象。
//!
//! 单表树（M1 无 JOIN）：叶子是 [`LogicalPlan::Scan`]；表达式为
//! [`pylon_expr::Expr`]（列索引相对各节点输入 schema）。Aggregate/Sort 在 T3 完成
//! 逻辑 lowering（explain 可见），物理执行随 T4 加入——因此节点带 span，物理化
//! 拒绝时能给出带位置的用户错误。

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use pylon_expr::Expr;
use pylon_sql::{AggFunc, Direction, Span};
use pylon_table::ScanPredicate;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// 基表扫描。`schema` 为输出 schema（列裁剪规则会把它收窄为表 schema 子集，
    /// 始终保持表序）；`filters` 是已下推的非精确谓词提示（行级正确性由残差
    /// Filter 保证）。
    Scan {
        table: String,
        /// 表 schema 的列索引（升序去重）；`schema` 即其投影
        columns: Vec<usize>,
        /// 输出 schema：`table_schema` 在 `columns` 上的子集（表序）
        schema: SchemaRef,
        /// 原始表 schema（物理 lowering 向 TableFormat 申请扫描时用）
        table_schema: SchemaRef,
        filters: Vec<ScanPredicate>,
    },
    Filter {
        predicate: Expr,
        input: Arc<LogicalPlan>,
    },
    /// 输出 schema 由 `exprs` 决定；`name` 为输出列名（列引用用列名，表达式用
    /// 其 SQL 形态文本——Trino 同款命名）。
    Project {
        exprs: Vec<NamedExpr>,
        schema: SchemaRef,
        input: Arc<LogicalPlan>,
    },
    /// 聚合（含 DISTINCT：keys=全部输出列、aggs 空）。输出 schema = keys + aggs。
    Aggregate {
        keys: Vec<NamedExpr>,
        aggs: Vec<AggExpr>,
        schema: SchemaRef,
        /// 首个聚合或分组键的位置，物理化拒绝时定位
        span: Span,
        /// SELECT DISTINCT 降级来的聚合（keys=全部输出列，aggs 空）
        distinct: bool,
        input: Arc<LogicalPlan>,
    },
    Sort {
        keys: Vec<SortKey>,
        span: Span,
        input: Arc<LogicalPlan>,
    },
    Limit {
        n: u64,
        input: Arc<LogicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub struct NamedExpr {
    pub expr: Expr,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct AggExpr {
    pub func: AggFunc,
    /// COUNT(*) 无参数，用 `arg: None` 表示
    pub arg: Option<Expr>,
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: Expr,
    pub direction: Direction,
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan { schema, .. } => schema.clone(),
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { schema, .. } => schema.clone(),
            LogicalPlan::Aggregate { schema, .. } => schema.clone(),
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
        }
    }

    pub fn input(&self) -> Option<&Arc<LogicalPlan>> {
        match self {
            LogicalPlan::Scan { .. } => None,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => Some(input),
        }
    }
}

/// 由命名表达式推导输出 schema。
pub fn schema_of(exprs: &[NamedExpr]) -> SchemaRef {
    let fields: Vec<Field> = exprs
        .iter()
        .map(|e| Field::new(e.name.clone(), e.expr.data_type(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// 聚合输出 schema：分组键在前，聚合值在后。
pub fn schema_of_aggregate(keys: &[NamedExpr], aggs: &[AggExpr]) -> SchemaRef {
    let mut fields: Vec<Field> = keys
        .iter()
        .map(|k| Field::new(k.name.clone(), k.expr.data_type(), true))
        .collect();
    fields.extend(
        aggs.iter()
            .map(|a| Field::new(a.name.clone(), a.data_type.clone(), true)),
    );
    Arc::new(Schema::new(fields))
}
