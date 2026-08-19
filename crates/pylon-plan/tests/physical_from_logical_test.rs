//! A1-2 unit tests: SQL → LogicalPlan → PhysicalPlan lowering
//! (`Arc<dyn ExecutionPlan>` post-R2.3).
//!
//! We construct an SQL string and assert that
//! `physical_from_logical` produces a plan tree whose shape
//! matches the expected SQL semantics. Because the post-R2.3
//! output is `Arc<dyn ExecutionPlan>`, the assertions use the
//! trait API (`children()`, `as_any().downcast_ref::<...>()`)
//! rather than destructuring a named `enum`.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use pylon_plan::physical::exec::{
    AggregateExec, ExecutionPlan, FilterExec, ProjectExec, SeqScanExec,
};
use pylon_plan::physical::expr::{
    AggregateFunctionExpr, BinaryOpExpr, ColumnExpr, LiteralExpr,
    PhysicalExpr,
};
use pylon_plan::translate::{logical_from_sql, physical_from_logical, CatalogStub};

fn catalog() -> CatalogStub {
    let mut c = CatalogStub::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    c.register("orders", schema, "../data/orders.parquet");
    c
}

fn sql_to_physical(sql: &str) -> Arc<dyn ExecutionPlan> {
    let logical = logical_from_sql(sql, &catalog()).unwrap();
    physical_from_logical(logical).unwrap()
}

fn downcast_op<T: 'static>(node: &Arc<dyn ExecutionPlan>) -> &T {
    node.as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("expected {:?}", std::any::type_name::<T>()))
}

fn downcast_expr<T: 'static>(node: &Arc<dyn PhysicalExpr>) -> &T {
    node.as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("expected {:?}", std::any::type_name::<T>()))
}

/// Convenience: pull a child's `Arc<dyn>` out of a unary op.
fn only_child(node: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let cs = node.children();
    assert_eq!(cs.len(), 1, "expected exactly one child, got {}", cs.len());
    Arc::clone(cs[0])
}

#[test]
fn simple_count_star_lowers_to_aggregate() {
    let plan = sql_to_physical("SELECT region, COUNT(*) FROM orders GROUP BY region");
    assert_eq!(plan.name(), "Aggregate");
    let agg = downcast_op::<AggregateExec>(&plan);
    // The Aggregate's input is a bare Scan (no filter, no project).
    let child = only_child(&plan);
    let input = downcast_op::<SeqScanExec>(&child);
    assert_eq!(input.table, "orders");
    // One group_by column.
    assert_eq!(agg.group_by.len(), 1);
    let group = downcast_expr::<ColumnExpr>(&agg.group_by[0]);
    assert_eq!(group.field.name(), "region");
    // One aggregate: COUNT(*).
    assert_eq!(agg.aggs.len(), 1);
    let func = downcast_expr::<AggregateFunctionExpr>(&agg.aggs[0]);
    assert_eq!(func.func, "count");
    assert_eq!(func.name, "count");
    assert!(func.args.is_empty(), "COUNT(*) has no args");
    assert_eq!(func.data_type, DataType::Int64);
    assert!(func.input_data_types.is_empty());
    // Output schema: region (Utf8) + count (Int64).
    let schema = agg.schema.clone();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "region");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "count");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
}

#[test]
fn count_with_column_preserves_arg() {
    let plan = sql_to_physical("SELECT region, COUNT(amount) FROM orders GROUP BY region");
    let agg = downcast_op::<AggregateExec>(&plan);
    let func = downcast_expr::<AggregateFunctionExpr>(&agg.aggs[0]);
    assert_eq!(func.func, "count");
    assert_eq!(func.name, "count_amount", "default field name = func_col");
    assert_eq!(func.args.len(), 1);
    assert_eq!(
        func.args[0].as_any().downcast_ref::<ColumnExpr>().map(|c| c.field.name().to_string()),
        Some("amount".to_string())
    );
    assert_eq!(func.input_data_types, vec![DataType::Float64]);
    let schema = agg.schema.clone();
    assert_eq!(schema.field(1).name(), "count_amount");
}

#[test]
fn sum_uses_input_type_for_result() {
    // SUM on Float64 → Float64.
    let plan = sql_to_physical("SELECT region, SUM(amount) FROM orders GROUP BY region");
    let agg = downcast_op::<AggregateExec>(&plan);
    let func = downcast_expr::<AggregateFunctionExpr>(&agg.aggs[0]);
    assert_eq!(func.func, "sum");
    assert_eq!(func.data_type, DataType::Float64);
    assert_eq!(agg.schema.field(1).data_type(), &DataType::Float64);
}

#[test]
fn min_max_return_input_type() {
    let plan = sql_to_physical(
        "SELECT region, MIN(id) AS lo, MAX(id) AS hi FROM orders GROUP BY region",
    );
    let agg = downcast_op::<AggregateExec>(&plan);
    assert_eq!(agg.aggs.len(), 2);
    let f1 = downcast_expr::<AggregateFunctionExpr>(&agg.aggs[0]);
    assert_eq!(f1.func, "min");
    assert_eq!(f1.name, "lo", "alias overwrites default field name");
    assert_eq!(f1.data_type, DataType::Int64);
    let f2 = downcast_expr::<AggregateFunctionExpr>(&agg.aggs[1]);
    assert_eq!(f2.func, "max");
    assert_eq!(f2.name, "hi");
    assert_eq!(f2.data_type, DataType::Int64);
    // Output schema: region + lo + hi.
    let schema = agg.schema.clone();
    assert_eq!(schema.field(0).name(), "region");
    assert_eq!(schema.field(1).name(), "lo");
    assert_eq!(schema.field(2).name(), "hi");
}

#[test]
fn no_group_by_returns_project_node() {
    let plan = sql_to_physical("SELECT id, amount FROM orders");
    assert_eq!(plan.name(), "Project");
    let proj = downcast_op::<ProjectExec>(&plan);
    assert_eq!(proj.projections.len(), 2);
}

#[test]
fn no_alias_uses_agg_name_col_naming() {
    // Default field name = func + arg_col for `func:col`. No
    // explicit alias → name stays as that default.
    let plan = sql_to_physical("SELECT region, SUM(amount) FROM orders GROUP BY region");
    let agg = downcast_op::<AggregateExec>(&plan);
    let func = downcast_expr::<AggregateFunctionExpr>(&agg.aggs[0]);
    assert_eq!(func.func, "sum");
    assert_eq!(func.name, "sum_amount");
}

#[test]
#[test]
fn no_group_by_columns_rejected_in_m3_first_cut() {
    // M3 first cut's logical planner accepts a global aggregate (no
    // GROUP BY); the fragmenter emits an `AggregateExec` with the
    // default partition_count. We assert the lower boundary here:
    // the produced plan IS an Aggregate, and `partition_count`
    // collapses to 1 (M3's implicit-single-partition fallback).
    let plan = sql_to_physical("SELECT COUNT(*) FROM orders");
    assert_eq!(plan.name(), "Aggregate");
    let agg = downcast_op::<AggregateExec>(&plan);
    assert!(agg.aggs.len() >= 1);
    assert_eq!(agg.group_by.len(), 0, "no GROUP BY cols");
}

#[test]
fn group_by_all_is_rejected_in_m3_first_cut() {
    // GROUP BY ALL is rejected at the LOGICAL layer.
    let result = logical_from_sql(
        "SELECT region, COUNT(*) FROM orders GROUP BY ALL",
        &catalog(),
    );
    assert!(result.is_err(), "expected Err for GROUP BY ALL");
}

#[test]
fn select_star_with_group_by_is_rejected_in_m3_first_cut() {
    // SELECT * with GROUP BY is rejected at the LOGICAL layer.
    let result = logical_from_sql(
        "SELECT *, COUNT(*) FROM orders GROUP BY region",
        &catalog(),
    );
    let err = result.err().expect("expected error for SELECT * with GROUP BY");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("select *")
            || msg.to_lowercase().contains("aggregate")
            || msg.to_lowercase().contains("group by"),
        "expected rejection, got: {msg}"
    );
}

#[test]
fn aggregate_in_group_by_is_rejected_in_m3_first_cut() {
    // Aggregate function inside GROUP BY is rejected at the LOGICAL
    // layer (`aggregate functions are not allowed in GROUP BY`).
    let result = logical_from_sql(
        "SELECT region, COUNT(*) FROM orders GROUP BY region, COUNT(amount)",
        &catalog(),
    );
    let err = result.err().expect("expected error for aggregate in GROUP BY");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("group by")
            || msg.to_lowercase().contains("aggregate function"),
        "expected rejection, got: {msg}"
    );
}

#[test]
fn non_grouped_column_in_projection_is_rejected_in_m3_first_cut() {
    // M3 first cut rejects ungrouped columns in the projection
    // list at the LOGICAL layer (`column … must appear in the
    // GROUP BY clause or inside an aggregate`).
    let result = logical_from_sql(
        "SELECT region, amount, COUNT(*) FROM orders GROUP BY region",
        &catalog(),
    );
    let err = result
        .err()
        .expect("expected error when non-grouped col in projection");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("group by")
            || msg.to_lowercase().contains("aggregate")
            || msg.to_lowercase().contains("must appear"),
        "expected rejection, got: {msg}"
    );
}
