//! Tests for the worker's `parse_agg_specs` helper (A1-4).
//!
//! The fragmenter emits `agg_specs` config values; the worker must
//! parse them into `AggSpec`s. We test the parser in isolation here;
//! the full pipeline integration is exercised by the E2E test added
//! in A1-5.

use pylon_runtime::ops::AggSpec;

// Re-implement the worker's parser here. (We can't import from a
// binary crate; the helper is a small pure function.)
fn parse_agg_specs(specs: &str) -> Vec<AggSpec> {
    let mut out = Vec::new();
    for spec in specs.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if let Some(inner) = spec
            .strip_prefix("count(")
            .and_then(|s| s.strip_suffix(")"))
        {
            if !inner.is_empty() {
                panic!("count() takes no arguments; got count({inner})");
            }
            out.push(AggSpec {
                func: "count".into(),
                arg_col: None,
                out_name: "count".into(),
            });
        } else if let Some((func, col)) = spec.split_once(':') {
            let func = func.trim().to_lowercase();
            let col = col.trim();
            if col.is_empty() {
                panic!("aggregate {func}() requires a column");
            }
            out.push(AggSpec {
                func,
                arg_col: Some(col.to_string()),
                out_name: spec.to_string(),
            });
        } else {
            panic!("malformed agg spec: {spec}");
        }
    }
    out
}

#[test]
fn count_star_only() {
    let aggs = parse_agg_specs("count()");
    assert_eq!(aggs.len(), 1);
    assert_eq!(aggs[0].func, "count");
    assert_eq!(aggs[0].arg_col, None);
    assert_eq!(aggs[0].out_name, "count");
}

#[test]
fn sum_min_max() {
    let aggs = parse_agg_specs("sum:amount;min:id;max:id");
    assert_eq!(aggs.len(), 3);
    assert_eq!(aggs[0].func, "sum");
    assert_eq!(aggs[0].arg_col, Some("amount".into()));
    assert_eq!(aggs[1].func, "min");
    assert_eq!(aggs[1].arg_col, Some("id".into()));
    assert_eq!(aggs[2].func, "max");
    assert_eq!(aggs[2].arg_col, Some("id".into()));
}

#[test]
fn mixed_count_and_sums() {
    let aggs = parse_agg_specs("count();sum:amount;count:qty");
    assert_eq!(aggs.len(), 3);
    assert_eq!(aggs[0].arg_col, None, "first is COUNT(*)");
    assert_eq!(aggs[1].func, "sum");
    assert_eq!(aggs[2].func, "count");
    assert_eq!(aggs[2].arg_col, Some("qty".into()));
}

#[test]
#[should_panic(expected = "count() takes no arguments")]
fn count_with_arg_panics() {
    parse_agg_specs("count(garbage)");
}

#[test]
#[should_panic(expected = "malformed agg spec")]
fn missing_colon_panics() {
    parse_agg_specs("sum");
}

#[test]
fn empty_string_yields_empty_vec() {
    let aggs = parse_agg_specs("");
    assert!(aggs.is_empty());
}

#[test]
fn whitespace_and_trailing_semicolon_ignored() {
    let aggs = parse_agg_specs("  count()  ;  sum:amount  ;");
    assert_eq!(aggs.len(), 2);
    assert_eq!(aggs[0].func, "count");
    assert_eq!(aggs[1].func, "sum");
}

#[test]
fn function_name_is_lowercased() {
    let aggs = parse_agg_specs("SUM:amount;Min:id");
    assert_eq!(aggs[0].func, "sum");
    assert_eq!(aggs[1].func, "min");
}
