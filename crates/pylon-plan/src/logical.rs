//! LogicalPlan for the M1/M3 subset:
//!   Scan / Filter / Project / Aggregate
//!
//! M3 first cut: `Aggregate` supports `COUNT(*)`, `SUM(int|float)`,
//! `MIN`, `MAX` with one or more group-by columns. Partial / distinct
//! aggregation arrive in M4+.

use arrow_schema::{DataType, Field, SchemaRef};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        schema: SchemaRef,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        projections: Vec<Expr>,
    },
    /// `SELECT <group_by>, <aggs> FROM <input> GROUP BY <group_by>`
    ///
    /// `group_by` are column refs (M3 first cut); `aggs` are
    /// `Expr::AggregateFunction` calls. `schema` is the post-aggregation
    /// schema: one field per group_by column (same name + type as the
    /// input) followed by one field per aggregate.
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggs: Vec<Expr>,
        schema: SchemaRef,
    },
}

#[derive(Debug, Clone)]
pub enum Expr {
    /// Reference to an input column. The `Field` carries the resolved
    /// name + data type from the input schema.
    Column(Field),
    /// A literal value (number, string, boolean).
    Literal(String),
    /// `op` is one of `=`, `<>`, `<`, `<=`, `>`, `>=` for M1 filters.
    BinaryOp {
        left: Box<Expr>,
        op: String,
        right: Box<Expr>,
    },
    /// `*` — only valid in projection position (passes through all
    /// columns unchanged).
    Wildcard,
    /// An aggregate function call. M3 first cut supports:
    ///   `COUNT(*)` (args empty), `SUM(col)`, `MIN(col)`, `MAX(col)`.
    ///
    /// `func` is the lowercased function name: `count` | `sum` | `min` | `max`.
    /// `args` is empty for `COUNT(*)`; otherwise exactly one input `Expr`
    /// (typically `Column`). `data_type` is the function's result type.
    /// `input_data_types` parallels `args` and is used at the physical
    /// layer to pick the right accumulator.
    /// `name` is the **output field name** (alias if supplied, else
    /// `func` for COUNT(*) or `func_col` e.g. `sum_amount`).
    AggregateFunction {
        func: String,
        name: String,
        args: Vec<Expr>,
        data_type: DataType,
        input_data_types: Vec<DataType>,
    },
}

/// Helper: `is_aggregate(expr)` — true if the (sub)expression contains
/// any `AggregateFunction`. Used by the translator to decide whether a
/// query has aggregation at the top level.
pub fn is_aggregate_expr(e: &Expr) -> bool {
    match e {
        Expr::AggregateFunction { .. } => true,
        Expr::BinaryOp { left, right, .. } => is_aggregate_expr(left) || is_aggregate_expr(right),
        Expr::Column(_) | Expr::Literal(_) | Expr::Wildcard => false,
    }
}
