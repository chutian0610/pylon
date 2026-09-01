//! LogicalPlan for the M1/M3 subset:
//!   Scan / Filter / Project / Aggregate
//!
//! M3 first cut: `Aggregate` supports `COUNT(*)`, `SUM(int|float)`,
//! `MIN`, `MAX` with one or more group-by columns. Partial / distinct
//! aggregation arrive in M4+.

use arrow_schema::{DataType, Field, SchemaRef};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Returns the schema that this plan produces row-by-row as
/// output. Used by optimizer rules that need to verify whether a
/// predicate references columns that exist in the input
/// subtree (predicate pushdown) or whether a projection
/// expression can be evaluated against the inner subtree
/// (project collapse).
///
/// The M3 first cut computes the schema inline rather than
/// caching it on the plan node — schemas are small, plans
/// are shallow, and recomputation keeps the `LogicalPlan`
/// enum minimal. M4 may add a `schema: SchemaRef` field to
/// every variant and remove this helper.
pub fn input_schema(plan: &LogicalPlan) -> arrow_schema::SchemaRef {
    use std::sync::Arc;
    match plan {
        LogicalPlan::Scan { schema, .. } => schema.clone(),
        LogicalPlan::Filter { input, .. } => input_schema(input),
        LogicalPlan::Project { input, projections } => {
            // Project may rename / drop columns. Compute the
            // output field list from the projections.
            let parent = input_schema(input);
            let mut fields: Vec<Arc<arrow_schema::Field>> = Vec::with_capacity(projections.len());
            for (i, e) in projections.iter().enumerate() {
                fields.push(projection_field(e, &parent, i));
            }
            Arc::new(arrow_schema::Schema::new(
                fields
                    .iter()
                    .map(|f| f.as_ref().clone())
                    .collect::<Vec<_>>(),
            ))
        }
        LogicalPlan::Aggregate { schema, .. } => schema.clone(),
    }
}

fn projection_field(
    e: &Expr,
    parent_schema: &arrow_schema::Schema,
    index: usize,
) -> Arc<arrow_schema::Field> {
    match e {
        Expr::Column(f) => Arc::new(f.clone()),
        Expr::AggregateFunction {
            name, data_type, ..
        } => Arc::new(arrow_schema::Field::new(name, data_type.clone(), true)),
        // Literal / BinaryOp / Wildcard: M3 first cut is permissive
        // — generate a synthetic field name `_<index>`. Rules that
        // need real column refs (ProjectCollapse) treat these as
        // opaque; rules that just need schema shape treat the name
        // as a placeholder.
        _ => Arc::new(arrow_schema::Field::new(
            format!("_{index}"),
            parent_schema
                .fields()
                .get(index.min(parent_schema.fields().len().saturating_sub(1)))
                .map(|f| f.data_type().clone())
                .unwrap_or(arrow_schema::DataType::Null),
            true,
        )),
    }
}

/// Returns the set of column names referenced anywhere in `e`.
/// Used by `PredicatePushdown` to decide whether a predicate can
/// be pushed past a Project (all referenced columns must exist in
/// the input subtree's schema).
pub fn expr_columns(e: &Expr) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    collect_columns(e, &mut out);
    out
}

fn collect_columns(e: &Expr, out: &mut std::collections::HashSet<String>) {
    match e {
        Expr::Column(f) => {
            out.insert(f.name().to_string());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        Expr::AggregateFunction { args, .. } => {
            for a in args {
                collect_columns(a, out);
            }
        }
        Expr::Literal(_) | Expr::Wildcard => {}
    }
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
