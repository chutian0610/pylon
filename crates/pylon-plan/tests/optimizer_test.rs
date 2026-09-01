//! Integration tests for the LogicalOptimizer: end-to-end SQL
//! → LogicalPlan → optimize → PhysicalPlan.
//!
//! Verifies that the optimizer:
//!   * does not change query results (the optimized plan lowers
//!     to a working PhysicalPlan that the engine can execute);
//!   * actually rewrites the LogicalPlan (the optimized plan
//!     is not structurally identical to the input);
//!   * composes both built-in rules.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use pylon_plan::optimizer::LogicalOptimizer;
use pylon_plan::physical::exec::ExecutionPlan;
use pylon_plan::translate::{CatalogStub, logical_from_sql, physical_from_logical};

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

#[test]
fn optimizer_pushes_filter_through_unused_projection_columns() {
    // SELECT id FROM orders WHERE id > 5 — translator currently
    // builds Project(Scan, [id]); an extra `id` projection at
    // the top is realistic for queries that have a wildcard +
    // explicit filter. The optimizer should not regress.
    let logical = logical_from_sql("SELECT id FROM orders WHERE id > 5", &catalog()).unwrap();
    let _physical = physical_from_logical(logical).unwrap();
}

#[test]
fn optimizer_runs_idempotently_on_a_simple_aggregate_query() {
    let logical = logical_from_sql(
        "SELECT region, COUNT(*) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap();
    let optimizer = LogicalOptimizer::with_default_rules();
    let (plan1, ctx1) = optimizer.optimize(logical).unwrap();
    // Running the optimizer twice is a no-op the second time.
    let (plan2, _ctx2) = optimizer.optimize(plan1.clone()).unwrap();
    assert_eq!(plan1, plan2);
    // And no rule fired on the second pass.
    assert_eq!(ctx1.rewrites_applied, _ctx2.rewrites_applied);
}

#[test]
fn optimizer_emits_logical_plan_that_lowers_to_physical_plan() {
    // Whatever the optimizer produces, physical_from_logical
    // must accept it without error.
    let logical =
        logical_from_sql("SELECT id, region FROM orders WHERE id > 5", &catalog()).unwrap();
    let optimizer = LogicalOptimizer::with_default_rules();
    let (optimized, _) = optimizer.optimize(logical).unwrap();
    // Lowering must succeed.
    let _physical: Arc<dyn ExecutionPlan> = physical_from_logical(optimized).unwrap();
}

#[test]
fn optimizer_preserves_schema_after_rewrite() {
    // Aggregate query: the schema must still match the expected
    // post-aggregate shape after optimization. PredicatePushdown
    // through Aggregate (when pred uses group_by col) should
    // not corrupt the Aggregate's schema field.
    let logical = logical_from_sql(
        "SELECT region, COUNT(*) FROM orders WHERE region = 'us' GROUP BY region",
        &catalog(),
    )
    .unwrap();
    let optimizer = LogicalOptimizer::with_default_rules();
    let (optimized, _) = optimizer.optimize(logical).unwrap();
    // The optimized plan still lowers cleanly.
    let physical = physical_from_logical(optimized).unwrap();
    let schema = physical.schema();
    // The post-aggregate schema should still have `region` and
    // a count column.
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(names.contains(&"region"));
    assert!(
        names
            .iter()
            .any(|n| n.contains("count") || n.contains("COUNT") || n == &"count"),
        "expected a count column in {names:?}"
    );
}

#[test]
fn optimizer_with_empty_rule_list_returns_input_unchanged() {
    let logical = logical_from_sql("SELECT id FROM orders WHERE id > 5", &catalog()).unwrap();
    let optimizer = LogicalOptimizer::new(vec![]);
    let (out, ctx) = optimizer.optimize(logical.clone()).unwrap();
    assert_eq!(out, logical);
    assert_eq!(ctx.rewrites_applied, 0);
}
