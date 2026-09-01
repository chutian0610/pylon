//! A1-1 unit tests: SQL → LogicalPlan, with focus on the new
//! `Aggregate` node and GROUP BY translation.

use arrow_schema::{DataType, Field, Schema};
use pylon_plan::logical::{Expr as LExpr, LogicalPlan};
use pylon_plan::translate::{CatalogStub, logical_from_sql};
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

#[test]
fn no_group_by_returns_project_node() {
    let plan = logical_from_sql("SELECT region FROM orders", &catalog()).unwrap();
    match plan {
        LogicalPlan::Project { projections, .. } => {
            assert_eq!(projections.len(), 1);
            match &projections[0] {
                LExpr::Column(f) => assert_eq!(f.name(), "region"),
                other => panic!("expected Column, got {other:?}"),
            }
        }
        other => panic!("expected Project, got {other:?}"),
    }
}

#[test]
fn simple_count_star_groups() {
    let plan = logical_from_sql(
        "SELECT region, COUNT(*) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap();
    let agg = match plan {
        LogicalPlan::Aggregate { group_by, aggs, .. } => {
            assert_eq!(group_by.len(), 1, "one group_by col");
            assert_eq!(aggs.len(), 1, "one aggregate");
            (group_by, aggs)
        }
        other => panic!("expected Aggregate, got {other:?}"),
    };
    let (group_by, aggs) = agg;
    match &group_by[0] {
        LExpr::Column(f) => assert_eq!(f.name(), "region"),
        other => panic!("expected Column for group_by, got {other:?}"),
    }
    match &aggs[0] {
        LExpr::AggregateFunction {
            name,
            args,
            data_type,
            ..
        } => {
            assert_eq!(name, "count");
            assert!(args.is_empty(), "COUNT(*) has no args");
            assert_eq!(*data_type, DataType::Int64);
        }
        other => panic!("expected AggregateFunction, got {other:?}"),
    }
}

#[test]
fn count_with_column_keeps_arg_type() {
    let plan = logical_from_sql(
        "SELECT region, COUNT(amount) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap();
    let LogicalPlan::Aggregate { aggs, schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    match &aggs[0] {
        LExpr::AggregateFunction {
            func,
            name,
            args,
            input_data_types,
            ..
        } => {
            assert_eq!(func, "count", "function name");
            assert_eq!(name, "count_amount", "default field name");
            assert_eq!(args.len(), 1, "COUNT(amount) has 1 arg");
            assert_eq!(input_data_types, &vec![DataType::Float64]);
        }
        _ => panic!(),
    }
    // The output schema has region + count_amount (Int64).
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "region");
    assert_eq!(schema.field(1).name(), "count_amount");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
}

#[test]
fn sum_min_max_with_aliases() {
    let plan = logical_from_sql(
        "SELECT region, SUM(amount) AS total, MIN(id) AS lo, MAX(id) AS hi \
         FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap();
    let LogicalPlan::Aggregate { aggs, schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    assert_eq!(aggs.len(), 3);
    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(
        names,
        vec![
            "region".to_string(),
            "total".to_string(),
            "lo".to_string(),
            "hi".to_string()
        ],
        "aliased names take precedence over agg_name_col defaults"
    );
    let types: Vec<&DataType> = schema.fields().iter().map(|f| f.data_type()).collect();
    assert_eq!(
        types,
        vec![
            &DataType::Utf8,
            &DataType::Float64,
            &DataType::Int64,
            &DataType::Int64
        ]
    );
}

#[test]
fn no_alias_uses_agg_name_col_naming() {
    let plan = logical_from_sql(
        "SELECT region, SUM(amount) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap();
    let LogicalPlan::Aggregate { schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    assert_eq!(schema.field(1).name(), "sum_amount");
}

#[test]
fn count_star_uses_count_naming() {
    let plan = logical_from_sql(
        "SELECT region, COUNT(*) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap();
    let LogicalPlan::Aggregate { schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    assert_eq!(schema.field(1).name(), "count");
}

#[test]
fn global_aggregate_with_no_group_by_columns() {
    // `SELECT COUNT(*) FROM orders` — no GROUP BY, but still an
    // aggregation. The fragmenter treats this as a 1-task aggregate.
    let plan = logical_from_sql("SELECT COUNT(*) FROM orders", &catalog()).unwrap();
    let LogicalPlan::Aggregate { group_by, aggs, .. } = plan else {
        panic!("expected Aggregate, got non-aggregate plan");
    };
    assert!(group_by.is_empty());
    assert_eq!(aggs.len(), 1);
}

#[test]
fn non_grouped_column_in_projection_is_rejected() {
    // `id` is not in GROUP BY, not inside an aggregate → SQL semantics
    // violation. We reject at plan time.
    let err = logical_from_sql(
        "SELECT region, id, COUNT(*) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("GROUP BY"), "got: {err}");
}

#[test]
fn where_clause_combines_with_group_by() {
    let plan = logical_from_sql(
        "SELECT region, COUNT(*) FROM orders WHERE id > 10 GROUP BY region",
        &catalog(),
    )
    .unwrap();
    // Outer = Aggregate, inner = Filter, inner = Scan.
    let LogicalPlan::Aggregate { input, .. } = plan else {
        panic!("expected Aggregate");
    };
    let LogicalPlan::Filter { input, .. } = *input else {
        panic!("expected Filter under Aggregate, got non-Filter");
    };
    let LogicalPlan::Scan { table, .. } = *input else {
        panic!("expected Scan under Filter");
    };
    assert_eq!(table, "orders");
}

#[test]
fn group_by_all_is_rejected_in_m3_first_cut() {
    let err = logical_from_sql(
        "SELECT region, COUNT(*) FROM orders GROUP BY ALL",
        &catalog(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("GROUP BY ALL"), "got: {err}");
}

#[test]
fn window_function_is_rejected() {
    let err = logical_from_sql(
        "SELECT region, ROW_NUMBER() OVER (PARTITION BY region) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("window"),
        "window function should be rejected, got: {err}"
    );
}

#[test]
fn distinct_inside_aggregate_is_rejected() {
    let err = logical_from_sql(
        "SELECT region, COUNT(DISTINCT amount) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("DISTINCT"),
        "DISTINCT aggregate should be rejected, got: {err}"
    );
}

#[test]
fn unknown_aggregate_function_is_rejected() {
    let err = logical_from_sql(
        "SELECT region, MEDIAN(amount) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("MEDIAN") || err.to_string().contains("not supported"),
        "MEDIAN should be rejected, got: {err}"
    );
}

#[test]
fn sum_on_unsupported_type_is_rejected() {
    // region is Utf8; SUM(region) should fail.
    let err = logical_from_sql(
        "SELECT region, SUM(region) FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .to_lowercase()
            .contains("sum does not support"),
        "SUM on Utf8 should be rejected, got: {err}"
    );
}

#[test]
fn aggregate_in_group_by_is_rejected() {
    let err =
        logical_from_sql("SELECT COUNT(*) FROM orders GROUP BY COUNT(*)", &catalog()).unwrap_err();
    assert!(
        err.to_string()
            .contains("aggregate functions are not allowed in GROUP BY"),
        "got: {err}"
    );
}

#[test]
fn select_star_with_group_by_is_rejected() {
    let err = logical_from_sql("SELECT * FROM orders GROUP BY region", &catalog()).unwrap_err();
    assert!(
        err.to_string().contains("incompatible with GROUP BY"),
        "got: {err}"
    );
}

#[test]
fn aggregate_schema_is_post_aggregation() {
    // Used by upper operators (e.g., a Project above an Aggregate).
    // The Aggregate node must own its post-aggregation schema so that
    // a parent Project knows which fields exist.
    let plan = logical_from_sql(
        "SELECT region, COUNT(*) AS cnt FROM orders GROUP BY region",
        &catalog(),
    )
    .unwrap();
    let LogicalPlan::Aggregate { schema, .. } = plan else {
        panic!("expected Aggregate");
    };
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "region");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).name(), "cnt");
    assert_eq!(schema.field(1).data_type(), &DataType::Int64);
}
