//! A1-2 unit tests: SQL → LogicalPlan → PhysicalPlan lowering,
//! focused on the `Aggregate` node plumbing added in A1-1.

use arrow_schema::{DataType, Field, Schema};
use pylon_plan::physical::physical_expr::PhysicalExpr;
use pylon_plan::physical::PhysicalPlan;
use pylon_plan::translate::{logical_from_sql, physical_from_logical, CatalogStub};
use std::sync::Arc;

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

fn sql_to_physical(sql: &str) -> PhysicalPlan {
    let logical = logical_from_sql(sql, &catalog()).unwrap();
    physical_from_logical(logical).unwrap()
}

#[test]
fn simple_count_star_lowers_to_aggregate() {
    let plan = sql_to_physical("SELECT region, COUNT(*) FROM orders GROUP BY region");
    let PhysicalPlan::Aggregate { input, group_by, aggs, schema } = plan else {
        panic!("expected Aggregate, got {plan:?}");
    };
    // The Aggregate's input is a bare Scan (no filter, no project).
    assert!(matches!(*input, PhysicalPlan::SeqScan { .. }));
    // One group_by column.
    assert_eq!(group_by.len(), 1);
    match &group_by[0] {
        PhysicalExpr::Column { field, .. } => assert_eq!(field.name(), "region"),
        other => panic!("expected Column, got {other:?}"),
    }
    // One aggregate: COUNT(*).
    assert_eq!(aggs.len(), 1);
    match &aggs[0] {
        PhysicalExpr::AggregateFunction { func, name, args, data_type, input_data_types } => {
            assert_eq!(func, "count");
            assert_eq!(name, "count");
            assert!(args.is_empty(), "COUNT(*) has no args");
            assert_eq!(*data_type, DataType::Int64);
            assert!(input_data_types.is_empty());
        }
        other => panic!("expected AggregateFunction, got {other:?}"),
    }
    // Output schema: region (Utf8) + count (Int64).
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "region");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "count");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
}

#[test]
fn count_with_column_preserves_arg() {
    let plan = sql_to_physical("SELECT region, COUNT(amount) FROM orders GROUP BY region");
    let PhysicalPlan::Aggregate { aggs, schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    match &aggs[0] {
        PhysicalExpr::AggregateFunction { func, name, args, input_data_types, .. } => {
            assert_eq!(func, "count");
            assert_eq!(name, "count_amount", "default field name = func_col");
            assert_eq!(args.len(), 1);
            assert_eq!(input_data_types, &vec![DataType::Float64]);
        }
        _ => panic!(),
    }
    assert_eq!(schema.field(1).name(), "count_amount");
}

#[test]
fn sum_uses_input_type_for_result() {
    // SUM on Float64 → Float64.
    let plan = sql_to_physical("SELECT region, SUM(amount) FROM orders GROUP BY region");
    let PhysicalPlan::Aggregate { aggs, schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    match &aggs[0] {
        PhysicalExpr::AggregateFunction { func, data_type, .. } => {
            assert_eq!(func, "sum");
            assert_eq!(*data_type, DataType::Float64);
        }
        _ => panic!(),
    }
    assert_eq!(schema.field(1).data_type(), &DataType::Float64);
}

#[test]
fn min_max_return_input_type() {
    let plan = sql_to_physical(
        "SELECT region, MIN(id) AS lo, MAX(id) AS hi FROM orders GROUP BY region",
    );
    let PhysicalPlan::Aggregate { aggs, schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    assert_eq!(aggs.len(), 2);
    match &aggs[0] {
        PhysicalExpr::AggregateFunction { func, name, data_type, .. } => {
            assert_eq!(func, "min");
            assert_eq!(name, "lo", "alias overwrites default field name");
            assert_eq!(*data_type, DataType::Int64);
        }
        _ => panic!(),
    }
    match &aggs[1] {
        PhysicalExpr::AggregateFunction { func, name, data_type, .. } => {
            assert_eq!(func, "max");
            assert_eq!(name, "hi");
            assert_eq!(*data_type, DataType::Int64);
        }
        _ => panic!(),
    }
    assert_eq!(schema.field(1).name(), "lo");
    assert_eq!(schema.field(2).name(), "hi");
}

#[test]
fn global_aggregate_lowers_without_group_by() {
    // SELECT COUNT(*) FROM orders — global aggregate, no GROUP BY.
    let plan = sql_to_physical("SELECT COUNT(*) FROM orders");
    let PhysicalPlan::Aggregate { input, group_by, aggs, .. } = plan else {
        panic!("expected Aggregate, got {plan:?}");
    };
    assert!(matches!(*input, PhysicalPlan::SeqScan { .. }));
    assert!(group_by.is_empty(), "no group_by columns for global aggregate");
    assert_eq!(aggs.len(), 1);
    match &aggs[0] {
        PhysicalExpr::AggregateFunction { func, .. } => assert_eq!(func, "count"),
        _ => panic!(),
    }
}

#[test]
fn aggregate_after_filter_preserves_filter_in_input() {
    // SELECT region, COUNT(*) FROM orders WHERE id > 10 GROUP BY region
    // Lowers to:
    //   Aggregate { input: Filter { input: Scan }, ... }
    let plan = sql_to_physical(
        "SELECT region, COUNT(*) FROM orders WHERE id > 10 GROUP BY region",
    );
    let PhysicalPlan::Aggregate { input, .. } = plan else {
        panic!("expected Aggregate, got {plan:?}");
    };
    let PhysicalPlan::Filter { input, .. } = *input else {
        panic!("expected Filter under Aggregate, got non-Filter");
    };
    let PhysicalPlan::SeqScan { table, .. } = *input else {
        panic!("expected Scan under Filter");
    };
    assert_eq!(table, "orders");
}

#[test]
fn multiple_aggregates_preserve_order() {
    let plan = sql_to_physical(
        "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total \
         FROM orders GROUP BY region",
    );
    let PhysicalPlan::Aggregate { aggs, schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    assert_eq!(aggs.len(), 2);
    let funcs: Vec<&str> = aggs
        .iter()
        .map(|a| match a {
            PhysicalExpr::AggregateFunction { func, .. } => func.as_str(),
            _ => "",
        })
        .collect();
    assert_eq!(funcs, vec!["count", "sum"]);
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["region", "cnt", "total"]);
}

#[test]
fn group_by_with_aliased_aggregate_field_names_match_schema() {
    // The Physical Aggregate's schema field list must align 1:1 with
    // group_by + aggs, in the same order they appear in the SQL.
    let plan = sql_to_physical(
        "SELECT region AS r, COUNT(*) AS cnt, SUM(amount) AS total \
         FROM orders GROUP BY region",
    );
    let PhysicalPlan::Aggregate { schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    // `region AS r` → output column `r` (the alias wins for output
    // naming). `COUNT(*) AS cnt` → `cnt`. `SUM(amount) AS total` →
    // `total`. The GROUP BY identity is still `region`; only the
    // output field gets renamed.
    assert_eq!(names, vec!["r", "cnt", "total"]);
}

#[test]
fn count_with_no_args_lowers_to_aggregate_with_empty_args() {
    // DISTINCT * is rejected; we test the COUNT(*) happy path stays
    // intact through lowering.
    let plan = sql_to_physical("SELECT region, COUNT(*) FROM orders GROUP BY region");
    let PhysicalPlan::Aggregate { aggs, .. } = plan else {
        panic!("expected Aggregate");
    };
    match &aggs[0] {
        PhysicalExpr::AggregateFunction { args, input_data_types, .. } => {
            assert!(args.is_empty());
            assert!(input_data_types.is_empty());
        }
        _ => panic!(),
    }
}

#[test]
fn lowering_propagates_filter_predicate_into_aggregate_input() {
    // Sanity: the Filter's predicate is preserved as a PhysicalExpr::BinaryOp
    // — lowering doesn't lose it.
    let plan = sql_to_physical(
        "SELECT region, COUNT(*) FROM orders WHERE amount > 5.0 GROUP BY region",
    );
    let PhysicalPlan::Aggregate { input, .. } = plan else {
        panic!("expected Aggregate");
    };
    let PhysicalPlan::Filter { predicate, .. } = *input else {
        panic!("expected Filter");
    };
    match &predicate {
        PhysicalExpr::BinaryOp { op, .. } => assert_eq!(op, ">"),
        other => panic!("expected BinaryOp, got {other:?}"),
    }
}

#[test]
fn non_aggregate_query_lowers_to_project_not_aggregate() {
    // Regression guard: no aggregates → no Aggregate node.
    let plan = sql_to_physical("SELECT region FROM orders");
    assert!(
        !matches!(plan, PhysicalPlan::Aggregate { .. }),
        "non-aggregate query should not lower to Aggregate"
    );
    assert!(matches!(plan, PhysicalPlan::Project { .. }));
}

#[test]
fn scan_with_filter_lowers_to_filter_scan() {
    // Plain Filter lowering — no aggregate — stays unchanged.
    let plan = sql_to_physical("SELECT region FROM orders WHERE id > 10");
    let PhysicalPlan::Project { input, .. } = plan else {
        panic!("expected Project");
    };
    assert!(matches!(*input, PhysicalPlan::Filter { .. }));
}
