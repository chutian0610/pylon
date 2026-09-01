//! LogicalOptimizer — `LogicalPlan → LogicalPlan` rewrite pipeline.
//!
//! RFC 0005 §4 role 6 (`OptimizerRule`) and §6 item 6 ("LogicalOptimizer
//! loop"). The optimizer owns a `Vec<Arc<dyn LogicalOptimizerRule>>`
//! and runs them in order until a fixed point is reached or
//! `max_iterations` is exceeded.
//!
//! ## Adding a new rule
//!
//! ```ignore
//! struct MyRule;
//! impl LogicalOptimizerRule for MyRule {
//!     fn name(&self) -> &str { "MyRule" }
//!     fn rewrite(&self, plan: LogicalPlan, _ctx: &mut RewriteContext)
//!         -> Result<LogicalPlan, PylonError> {
//!         Ok(plan) // pass-through; real rules return a rewritten plan
//!     }
//!     fn apply_order(&self) -> ApplyOrder { ApplyOrder::EveryPass }
//! }
//!
//! LogicalOptimizer::with_rules(vec![Arc::new(MyRule)])
//!     .optimize(plan)?
//! ```
//!
//! ## M3 built-in rules
//!
//! - [`PredicatePushdown`] — pushes `Filter` nodes through `Project`
//!   and through `Aggregate` (when the predicate references only
//!   columns that exist in the subtree below the cut point).
//! - [`ProjectCollapse`] — collapses nested `Project` nodes into a
//!   single `Project` when the outer projections reference only
//!   columns produced by the inner projection.
//!
//! Both are conservative: when in doubt, they leave the tree alone.
//! Future rules (constant folding, join reordering, …) plug in via
//! the same trait.

use std::collections::HashSet;
use std::sync::Arc;

use pylon_types::PylonError;

use crate::logical::{Expr, LogicalPlan, input_schema};

// =====================================================================
// ApplyOrder
// =====================================================================

/// When should a rule fire inside the orchestrator's iterative
/// fixed-point loop? Mirrors DataFusion's
/// `OptimizerRule::apply_order` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOrder {
    /// Run on every iteration until the plan stops changing.
    /// Use for idempotent rewrites that compose with themselves
    /// — predicate pushdown keeps moving a filter deeper each
    /// pass and converges when the filter reaches a node that
    /// can't be pushed past.
    EveryPass,
    /// Run exactly once across the whole optimization. Use for
    /// rewrites that don't compose with themselves — collapsing
    /// nested projects is already a single rewrite; running it
    /// twice is a no-op.
    Once,
}

// =====================================================================
// RewriteContext
// =====================================================================

/// Mutable state threaded through every rule invocation in a single
/// optimization pass. Rules can read it (for statistics, schema
/// caches, etc.) and write it (for instrumentation). The
/// orchestrator resets this between `optimize()` calls.
///
/// M3 first cut only carries a counter. M4+ may grow this with
/// schema metadata caches, statistics, and rewrite budgets.
#[derive(Debug, Default, Clone)]
pub struct RewriteContext {
    /// Number of plan rewrites applied across all rule invocations
    /// in this `optimize()` call. The orchestrator bumps this for
    /// every rule that returns a changed plan.
    pub rewrites_applied: usize,
    /// Names of rules that fired at least once (insertion order
    /// preserved). Useful for tracing and tests.
    pub rules_fired: Vec<String>,
}

impl RewriteContext {
    pub fn new() -> Self {
        Self::default()
    }
}

// =====================================================================
// LogicalOptimizerRule
// =====================================================================

/// One rewrite rule the orchestrator consults. Returning a
/// different `LogicalPlan` counts as a rewrite (the orchestrator
/// tracks this via `PartialEq`); returning the same plan is a
/// no-op for that pass.
///
/// Object-safe so rules live in `Vec<Arc<dyn LogicalOptimizerRule>>`.
///
/// Adding a new rule is purely additive — the orchestrator's loop
/// picks it up automatically.
pub trait LogicalOptimizerRule: Send + Sync {
    /// Stable name used in tracing and `RewriteContext::rules_fired`.
    fn name(&self) -> &str;

    /// Rewrite `plan` (post-order recursion is the rule's own
    /// responsibility). Returning `Ok(plan)` unchanged is a valid
    /// no-op rule (e.g. when the rule doesn't apply to this shape).
    fn rewrite(
        &self,
        plan: LogicalPlan,
        ctx: &mut RewriteContext,
    ) -> Result<LogicalPlan, PylonError>;

    /// See [`ApplyOrder`]. Defaults to `EveryPass` — the safe
    /// choice for rules that may keep rewriting across passes.
    fn apply_order(&self) -> ApplyOrder {
        ApplyOrder::EveryPass
    }
}

// =====================================================================
// LogicalOptimizer — iterative orchestrator
// =====================================================================

/// Runs a list of rules until the plan stops changing or
/// `max_iterations` is reached. The orchestrator owns no mutable
/// state across calls; each `optimize()` invocation starts with a
/// fresh `RewriteContext`.
pub struct LogicalOptimizer {
    rules: Vec<Arc<dyn LogicalOptimizerRule>>,
    max_iterations: usize,
}

impl LogicalOptimizer {
    /// Construct with an explicit rule list. No default rules —
    /// every caller picks their own set. Most production callers
    /// use [`with_default_rules`] which registers the M3
    /// built-ins.
    pub fn new(rules: Vec<Arc<dyn LogicalOptimizerRule>>) -> Self {
        Self {
            rules,
            max_iterations: 100,
        }
    }

    /// Construct with the M3 default rule set:
    /// `PredicatePushdown` (every pass) + `ProjectCollapse` (once).
    pub fn with_default_rules() -> Self {
        // Order matters: ProjectCollapse runs first to flatten
        // nested `Project` nodes; PredicatePushdown then sees a
        // normalised tree where every `Filter` is above at most
        // one `Project` (or one `Aggregate`). Reversing the
        // order would leave the iterative loop re-converging
        // across extra passes (predicate pushes past one
        // Project per pass, project collapse runs only Once).
        Self::new(vec![
            Arc::new(ProjectCollapse::new()),
            Arc::new(PredicatePushdown::new()),
        ])
    }

    /// Builder: append a rule.
    pub fn with_rule(mut self, rule: Arc<dyn LogicalOptimizerRule>) -> Self {
        self.rules.push(rule);
        self
    }

    /// Builder: replace the entire rule list.
    pub fn with_rules_list(mut self, rules: Vec<Arc<dyn LogicalOptimizerRule>>) -> Self {
        self.rules = rules;
        self
    }

    /// Builder: cap the number of fixed-point iterations. Default
    /// is 100; an iteration cap that's hit before fixed point
    /// surfaces a clear `PylonError::Internal` so we never loop
    /// forever on a non-converging rule.
    pub fn with_max_iterations(mut self, n: usize) -> Self {
        self.max_iterations = n;
        self
    }

    /// Registered rule names (for diagnostics / tracing).
    pub fn rule_names(&self) -> Vec<String> {
        self.rules.iter().map(|r| r.name().to_string()).collect()
    }

    /// Run the optimizer on `plan`. Returns the rewritten plan
    /// (which equals `plan` if no rule fired) plus the populated
    /// context with rewrite counts and fired-rule names.
    ///
    /// Errors:
    /// * `PylonError::Internal` if `max_iterations` is reached
    ///   before fixed point (signals a non-terminating rule —
    ///   never expected from the built-ins, defensive against
    ///   custom rules).
    pub fn optimize(&self, plan: LogicalPlan) -> Result<(LogicalPlan, RewriteContext), PylonError> {
        let mut ctx = RewriteContext::new();
        let mut current = plan;
        // Set of rules that already fired once (ApplyOrder::Once).
        let mut once_fired: HashSet<String> = HashSet::new();

        for iter in 0..self.max_iterations {
            let before = current.clone();
            for rule in &self.rules {
                let rule_name = rule.name().to_string();
                if rule.apply_order() == ApplyOrder::Once && once_fired.contains(&rule_name) {
                    continue;
                }
                let next = rule.rewrite(current.clone(), &mut ctx)?;
                if next != current {
                    ctx.rewrites_applied += 1;
                    let name = rule.name().to_string();
                    if !ctx.rules_fired.contains(&name) {
                        ctx.rules_fired.push(name);
                    }
                    current = next;
                }
                once_fired.insert(rule_name);
            }
            if current == before {
                return Ok((current, ctx));
            }
            // Continue iterating.
            let _ = iter; // iter index kept for future logging hooks.
        }

        Err(PylonError::Internal(format!(
            "LogicalOptimizer did not converge after {} iterations; \
             rules: {:?}, last fired: {:?}",
            self.max_iterations,
            self.rule_names(),
            ctx.rules_fired
        )))
    }
}

// =====================================================================
// PredicatePushdown — push Filter through Project / Aggregate
// =====================================================================

/// Pushes `Filter` nodes as deep into the tree as the predicate's
/// column references allow.
///
/// Patterns:
/// * `Filter(Project(input, _), pred)` → `Project(Filter(input, pred), _)`
///   when every column referenced by `pred` exists in `input`'s schema.
/// * `Filter(Aggregate(input, group_by, aggs, _), pred)` →
///   `Aggregate(Filter(input, pred), group_by, aggs, _)` when every
///   column referenced by `pred` is a `group_by` column (predicates
///   that touch aggregate outputs cannot be pushed through).
///
/// Recursion is post-order — the rule rewrites the input subtree
/// first, then attempts the swap at this node. Combined with the
/// orchestrator's iterative loop, multi-level pushdown (e.g. Filter
/// pushed through two nested Projects) converges via repeated
/// invocations of this rule.
pub struct PredicatePushdown {
    /// When `false`, the rule never fires. Useful for tests that
    /// want to isolate other rules.
    enabled: bool,
}

impl PredicatePushdown {
    pub fn new() -> Self {
        Self { enabled: true }
    }

    /// Disable the rule (returns the input plan untouched).
    /// Primarily for tests.
    pub fn disabled() -> Self {
        Self { enabled: false }
    }
}

impl Default for PredicatePushdown {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalOptimizerRule for PredicatePushdown {
    fn name(&self) -> &str {
        "PredicatePushdown"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<LogicalPlan, PylonError> {
        if !self.enabled {
            return Ok(plan);
        }
        Ok(push_down_filter(plan))
    }

    fn apply_order(&self) -> ApplyOrder {
        // Multi-level pushdown: a Filter can be pushed through
        // multiple layers across passes. Converges when the
        // filter reaches a leaf or hits an un-pushable barrier
        // (predicate references columns not in the input
        // subtree).
        ApplyOrder::EveryPass
    }
}

fn push_down_filter(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            // Recurse first, then try to swap at this node.
            let rewritten_input = push_down_filter(*input);
            try_swap_filter(rewritten_input, predicate)
        }
        LogicalPlan::Project { input, projections } => LogicalPlan::Project {
            input: Box::new(push_down_filter(*input)),
            projections,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(push_down_filter(*input)),
            group_by,
            aggs,
            schema,
        },
        LogicalPlan::Scan { .. } => plan,
    }
}

fn try_swap_filter(input: LogicalPlan, predicate: Expr) -> LogicalPlan {
    let pred_cols = crate::logical::expr_columns(&predicate);
    match input {
        LogicalPlan::Project {
            input: inner,
            projections,
        } => {
            // Safe to swap when `pred_cols` ⊆ `inner.schema()`.
            let inner_cols: HashSet<String> = input_schema(&inner)
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();
            if pred_cols.is_subset(&inner_cols) {
                LogicalPlan::Project {
                    input: Box::new(LogicalPlan::Filter {
                        input: inner,
                        predicate,
                    }),
                    projections,
                }
            } else {
                // Predicate references at least one column that
                // the Project synthesises — leaving it on top is
                // the only correct option.
                LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Project {
                        input: inner,
                        projections,
                    }),
                    predicate,
                }
            }
        }
        LogicalPlan::Aggregate {
            input: inner,
            group_by,
            aggs,
            schema,
        } => {
            // Safe to swap when `pred_cols` ⊆ group_by column
            // names. Aggregate outputs are NOT safe to push
            // through (their values aren't deterministic per row
            // pre-aggregation).
            let group_by_cols: HashSet<String> = group_by
                .iter()
                .filter_map(|e| match e {
                    Expr::Column(f) => Some(f.name().to_string()),
                    _ => None,
                })
                .collect();
            if pred_cols.is_subset(&group_by_cols) {
                LogicalPlan::Aggregate {
                    input: Box::new(LogicalPlan::Filter {
                        input: inner,
                        predicate,
                    }),
                    group_by,
                    aggs,
                    schema,
                }
            } else {
                LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Aggregate {
                        input: inner,
                        group_by,
                        aggs,
                        schema,
                    }),
                    predicate,
                }
            }
        }
        // Scan / Filter-on-Filter: leave as Filter on top. M3
        // first cut doesn't merge nested Filters (no AND
        // constant) — future work can split conjunctive
        // predicates and merge.
        other => LogicalPlan::Filter {
            input: Box::new(other),
            predicate,
        },
    }
}

// =====================================================================
// ProjectCollapse — merge nested Project nodes
// =====================================================================

/// Collapses `Project(Project(input, inner), outer)` into
/// `Project(input, outer)` when every column reference in `outer`
/// exists in `inner`'s output.
///
/// Conservative: only collapses when `outer` consists entirely of
/// direct `Expr::Column` references AND every referenced column is
/// produced by the inner projection. Computed expressions in
/// `outer` are left as-is because they may depend on
/// intermediate columns that the inner Project synthesises (and
/// recomputing them against the deeper input would require
/// rewriting the expression — out of scope for M3).
pub struct ProjectCollapse {
    enabled: bool,
}

impl ProjectCollapse {
    pub fn new() -> Self {
        Self { enabled: true }
    }

    pub fn disabled() -> Self {
        Self { enabled: false }
    }
}

impl Default for ProjectCollapse {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalOptimizerRule for ProjectCollapse {
    fn name(&self) -> &str {
        "ProjectCollapse"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<LogicalPlan, PylonError> {
        if !self.enabled {
            return Ok(plan);
        }
        Ok(collapse_projects(plan))
    }

    fn apply_order(&self) -> ApplyOrder {
        // Project collapse is idempotent: collapsing once
        // eliminates the inner Project, and a second pass has
        // nothing to do. Once is enough.
        ApplyOrder::Once
    }
}

fn collapse_projects(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, projections } => {
            let inner = collapse_projects(*input);
            try_collapse_project(inner, projections)
        }
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(collapse_projects(*input)),
            predicate,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(collapse_projects(*input)),
            group_by,
            aggs,
            schema,
        },
        LogicalPlan::Scan { .. } => plan,
    }
}

fn try_collapse_project(input: LogicalPlan, outer: Vec<Expr>) -> LogicalPlan {
    if let LogicalPlan::Project {
        input: deepest,
        projections: inner,
    } = input
    {
        // Set of column names the inner Project produces
        // (as `Expr::Column` outputs). Anything else in the
        // inner Project is opaque to collapse.
        let inner_output_cols: HashSet<String> = inner
            .iter()
            .filter_map(|e| match e {
                Expr::Column(f) => Some(f.name().to_string()),
                _ => None,
            })
            .collect();

        let outer_ok = outer.iter().all(|e| match e {
            Expr::Column(f) => inner_output_cols.contains(f.name()),
            // Wildcard or non-column expression — not safe to
            // collapse without recomputing the expression
            // against the deeper input.
            _ => false,
        });

        if outer_ok {
            LogicalPlan::Project {
                input: deepest,
                projections: outer,
            }
        } else {
            LogicalPlan::Project {
                input: Box::new(LogicalPlan::Project {
                    input: deepest,
                    projections: inner,
                }),
                projections: outer,
            }
        }
    } else {
        LogicalPlan::Project {
            input: Box::new(input),
            projections: outer,
        }
    }
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    fn schema_a() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn scan_a() -> LogicalPlan {
        LogicalPlan::Scan {
            table: "t".into(),
            schema: schema_a(),
        }
    }

    fn col(name: &str, dt: DataType) -> Expr {
        Expr::Column(Field::new(name, dt, false))
    }

    fn lit(v: &str) -> Expr {
        Expr::Literal(v.to_string())
    }

    fn filter(input: LogicalPlan, pred: Expr) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate: pred,
        }
    }

    fn project(input: LogicalPlan, projs: Vec<Expr>) -> LogicalPlan {
        LogicalPlan::Project {
            input: Box::new(input),
            projections: projs,
        }
    }

    // -----------------------------------------------------------------
    // ApplyOrder / RewriteContext
    // -----------------------------------------------------------------

    #[test]
    fn apply_order_equality_round_trips() {
        assert_eq!(ApplyOrder::EveryPass, ApplyOrder::EveryPass);
        assert_eq!(ApplyOrder::Once, ApplyOrder::Once);
        assert_ne!(ApplyOrder::EveryPass, ApplyOrder::Once);
    }

    #[test]
    fn rewrite_context_default_starts_empty() {
        let ctx = RewriteContext::default();
        assert_eq!(ctx.rewrites_applied, 0);
        assert!(ctx.rules_fired.is_empty());
    }

    // -----------------------------------------------------------------
    // PredicatePushdown — direct cases
    // -----------------------------------------------------------------

    #[test]
    fn pushdown_through_project_when_pred_uses_inner_columns() {
        // Filter over Project: pred references `id`, which is in
        // scan schema. Pushdown swaps them.
        let inner = scan_a();
        let proj = project(inner, vec![col("id", DataType::Int64)]);
        let pred = Expr::BinaryOp {
            left: Box::new(col("id", DataType::Int64)),
            op: ">".into(),
            right: Box::new(lit("5")),
        };
        let plan = filter(proj, pred.clone());
        let out = push_down_filter(plan);
        match out {
            LogicalPlan::Project { input, projections } => {
                assert_eq!(projections, vec![col("id", DataType::Int64)]);
                match *input {
                    LogicalPlan::Filter {
                        input: deepest,
                        predicate,
                    } => {
                        assert_eq!(predicate, pred);
                        assert!(matches!(*deepest, LogicalPlan::Scan { .. }));
                    }
                    _ => panic!("expected Filter inside Project after pushdown"),
                }
            }
            _ => panic!("expected Project at top after pushdown"),
        }
    }

    #[test]
    fn pushdown_blocks_when_pred_references_synthesised_column() {
        // Project synthesises `id_doubled`. Pred references it.
        // Pushdown must leave the Filter on top.
        let proj = project(
            scan_a(),
            vec![Expr::BinaryOp {
                left: Box::new(col("id", DataType::Int64)),
                op: "+".into(),
                right: Box::new(lit("1")),
            }],
        );
        let pred = Expr::BinaryOp {
            // pretend the user wrote `WHERE id_doubled > 5` — the
            // schema field would have to be supplied by the
            // translator; here we just use `id` so the pushdown
            // check below is "pred refs inner scan column `id`".
            // That IS pushable. To test the no-push case, make
            // the pred reference a column the scan doesn't have.
            left: Box::new(col("nonexistent", DataType::Int64)),
            op: ">".into(),
            right: Box::new(lit("5")),
        };
        let plan = filter(proj, pred);
        let out = push_down_filter(plan);
        // Filter must stay on top.
        assert!(matches!(out, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn pushdown_through_aggregate_when_pred_uses_group_by_column() {
        // Aggregate { group_by: [name], aggs: [count(*)] }
        // Filter { pred: name = 'foo' } — pushable (pred uses
        // group_by column `name`, not agg output `count`).
        let agg = LogicalPlan::Aggregate {
            input: Box::new(scan_a()),
            group_by: vec![col("name", DataType::Utf8)],
            aggs: vec![Expr::AggregateFunction {
                func: "count".into(),
                name: "count".into(),
                args: vec![],
                data_type: DataType::Int64,
                input_data_types: vec![],
            }],
            schema: Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("count", DataType::Int64, true),
            ])),
        };
        let pred = Expr::BinaryOp {
            left: Box::new(col("name", DataType::Utf8)),
            op: "=".into(),
            right: Box::new(lit("foo")),
        };
        let plan = filter(agg, pred);
        let out = push_down_filter(plan);
        match out {
            LogicalPlan::Aggregate { input, .. } => {
                assert!(matches!(*input, LogicalPlan::Filter { .. }));
            }
            _ => panic!("expected Aggregate at top after pushdown through agg"),
        }
    }

    #[test]
    fn pushdown_blocks_when_pred_references_aggregate_output() {
        // Same aggregate, but pred references `count` (an
        // aggregate output). Cannot push past the Aggregate.
        let agg = LogicalPlan::Aggregate {
            input: Box::new(scan_a()),
            group_by: vec![col("name", DataType::Utf8)],
            aggs: vec![Expr::AggregateFunction {
                func: "count".into(),
                name: "count".into(),
                args: vec![],
                data_type: DataType::Int64,
                input_data_types: vec![],
            }],
            schema: Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("count", DataType::Int64, true),
            ])),
        };
        let pred = Expr::BinaryOp {
            left: Box::new(col("count", DataType::Int64)),
            op: ">".into(),
            right: Box::new(lit("0")),
        };
        let plan = filter(agg, pred);
        let out = push_down_filter(plan);
        // Filter must stay on top because `count` is an
        // aggregate output (not in group_by).
        assert!(matches!(out, LogicalPlan::Filter { .. }));
    }

    // -----------------------------------------------------------------
    // ProjectCollapse
    // -----------------------------------------------------------------

    #[test]
    fn collapse_merges_nested_project_with_column_refs() {
        // Project(Project(scan, [id]), [id]) → Project(scan, [id]).
        let inner = project(scan_a(), vec![col("id", DataType::Int64)]);
        let outer = project(inner, vec![col("id", DataType::Int64)]);
        let out = collapse_projects(outer);
        match out {
            LogicalPlan::Project { input, projections } => {
                assert_eq!(projections, vec![col("id", DataType::Int64)]);
                assert!(matches!(*input, LogicalPlan::Scan { .. }));
            }
            _ => panic!("expected collapsed Project"),
        }
    }

    #[test]
    fn collapse_blocks_when_outer_has_computed_expression() {
        // Project(Project(scan, [id, id_doubled = id+1]),
        //          [id_doubled + 1]) — outer's `id_doubled + 1`
        // is computed, not a direct column ref. Don't collapse.
        let inner = project(
            scan_a(),
            vec![
                col("id", DataType::Int64),
                Expr::BinaryOp {
                    left: Box::new(col("id", DataType::Int64)),
                    op: "+".into(),
                    right: Box::new(lit("1")),
                },
            ],
        );
        let outer = project(
            inner,
            vec![Expr::BinaryOp {
                left: Box::new(col("id_doubled", DataType::Int64)),
                op: "+".into(),
                right: Box::new(lit("1")),
            }],
        );
        let out = collapse_projects(outer);
        // Still nested.
        assert!(
            matches!(out, LogicalPlan::Project { input, .. } if matches!(*input, LogicalPlan::Project { .. }))
        );
    }

    #[test]
    fn collapse_passes_through_filter() {
        // Project(Filter(scan, pred), projs) — collapse only
        // touches nested Projects. Filter in between is left.
        let p = filter(
            scan_a(),
            Expr::BinaryOp {
                left: Box::new(col("id", DataType::Int64)),
                op: ">".into(),
                right: Box::new(lit("0")),
            },
        );
        let plan = project(p, vec![col("id", DataType::Int64)]);
        let out = collapse_projects(plan);
        // Top should still be Project; the inner is Filter.
        match out {
            LogicalPlan::Project { input, .. } => {
                assert!(matches!(*input, LogicalPlan::Filter { .. }));
            }
            _ => panic!("expected Project over Filter"),
        }
    }

    // -----------------------------------------------------------------
    // LogicalOptimizer orchestrator
    // -----------------------------------------------------------------

    #[test]
    fn optimizer_default_rules_pass_through_when_nothing_to_do() {
        let optimizer = LogicalOptimizer::with_default_rules();
        let plan = scan_a();
        let (out, ctx) = optimizer.optimize(plan.clone()).unwrap();
        assert_eq!(out, plan);
        assert_eq!(ctx.rewrites_applied, 0);
        assert!(ctx.rules_fired.is_empty());
    }

    #[test]
    fn optimizer_pushes_filter_through_project() {
        let optimizer = LogicalOptimizer::with_default_rules();
        let proj = project(scan_a(), vec![col("id", DataType::Int64)]);
        let pred = Expr::BinaryOp {
            left: Box::new(col("id", DataType::Int64)),
            op: ">".into(),
            right: Box::new(lit("5")),
        };
        let plan = filter(proj, pred);
        let (out, ctx) = optimizer.optimize(plan).unwrap();
        // Should be Project(Filter(Scan), [id]).
        match out {
            LogicalPlan::Project { input, projections } => {
                assert_eq!(projections, vec![col("id", DataType::Int64)]);
                assert!(matches!(*input, LogicalPlan::Filter { .. }));
            }
            _ => panic!("expected Project at top after pushdown"),
        }
        assert!(ctx.rules_fired.iter().any(|n| n == "PredicatePushdown"));
        assert!(ctx.rewrites_applied >= 1);
    }

    #[test]
    fn optimizer_collapses_nested_project() {
        let optimizer = LogicalOptimizer::with_default_rules();
        let inner = project(scan_a(), vec![col("id", DataType::Int64)]);
        let outer = project(inner, vec![col("id", DataType::Int64)]);
        let (out, ctx) = optimizer.optimize(outer).unwrap();
        // Should be Project(scan, [id]) — no nesting.
        match out {
            LogicalPlan::Project { input, projections } => {
                assert_eq!(projections, vec![col("id", DataType::Int64)]);
                assert!(matches!(*input, LogicalPlan::Scan { .. }));
            }
            _ => panic!("expected collapsed Project"),
        }
        assert!(ctx.rules_fired.iter().any(|n| n == "ProjectCollapse"));
    }

    #[test]
    fn optimizer_composes_pushdown_and_collapse() {
        // Filter(Project(Project(scan, [id]), [id]), id > 5).
        // Expected after both rules: Project(Filter(scan, id>5), [id]).
        let optimizer = LogicalOptimizer::with_default_rules();
        let inner = project(scan_a(), vec![col("id", DataType::Int64)]);
        let outer = project(inner, vec![col("id", DataType::Int64)]);
        let pred = Expr::BinaryOp {
            left: Box::new(col("id", DataType::Int64)),
            op: ">".into(),
            right: Box::new(lit("5")),
        };
        let plan = filter(outer, pred);
        let (out, _ctx) = optimizer.optimize(plan).unwrap();
        match out {
            LogicalPlan::Project { input, projections } => {
                assert_eq!(projections, vec![col("id", DataType::Int64)]);
                match *input {
                    LogicalPlan::Filter { input: deepest, .. } => {
                        assert!(matches!(*deepest, LogicalPlan::Scan { .. }));
                    }
                    _ => panic!("expected Filter inside Project"),
                }
            }
            _ => panic!("expected Project at top after both rules"),
        }
    }

    #[test]
    fn optimizer_with_rule_builder_adds_custom_rule() {
        // A no-op custom rule that records its name in the
        // context. Verifies builder + registration order.
        struct NoOpRule(&'static str);
        impl LogicalOptimizerRule for NoOpRule {
            fn name(&self) -> &str {
                self.0
            }
            fn rewrite(
                &self,
                plan: LogicalPlan,
                _ctx: &mut RewriteContext,
            ) -> Result<LogicalPlan, PylonError> {
                Ok(plan)
            }
        }

        let optimizer = LogicalOptimizer::new(vec![]).with_rule(Arc::new(NoOpRule("Z")));
        let plan = scan_a();
        let (_out, ctx) = optimizer.optimize(plan).unwrap();
        // NoOpRule returns the plan unchanged, so no rewrites.
        assert_eq!(ctx.rewrites_applied, 0);
        assert_eq!(optimizer.rule_names(), vec!["Z"]);
    }

    #[test]
    fn optimizer_with_default_rules_uses_both_builtin_rules() {
        let optimizer = LogicalOptimizer::with_default_rules();
        let names = optimizer.rule_names();
        assert!(names.iter().any(|n| n == "PredicatePushdown"));
        assert!(names.iter().any(|n| n == "ProjectCollapse"));
    }

    #[test]
    fn optimizer_fixed_point_terminates_within_max_iterations() {
        // A pathological case: if a rule always changes the
        // plan, the orchestrator should hit max_iterations and
        // return an error rather than loop forever.
        struct InfiniteRule;
        impl LogicalOptimizerRule for InfiniteRule {
            fn name(&self) -> &str {
                "InfiniteRule"
            }
            fn rewrite(
                &self,
                plan: LogicalPlan,
                _ctx: &mut RewriteContext,
            ) -> Result<LogicalPlan, PylonError> {
                // Always wrap in a new Filter with a literal
                // predicate. The plan keeps changing
                // structurally (new Box + new Expr), so the
                // orchestrator never hits fixed point within
                // `max_iterations`.
                Ok(LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: Expr::Literal("0".to_string()),
                })
            }
            fn apply_order(&self) -> ApplyOrder {
                ApplyOrder::EveryPass
            }
        }
        let optimizer = LogicalOptimizer::new(vec![Arc::new(InfiniteRule)]).with_max_iterations(3);
        let err = optimizer.optimize(scan_a()).expect_err("must not converge");
        let s = format!("{err:?}");
        assert!(s.contains("did not converge"), "got: {s}");
    }

    #[test]
    fn optimizer_disabled_predicate_pushdown_leaves_plan_alone() {
        let optimizer = LogicalOptimizer::new(vec![Arc::new(PredicatePushdown::disabled())]);
        let proj = project(scan_a(), vec![col("id", DataType::Int64)]);
        let pred = Expr::BinaryOp {
            left: Box::new(col("id", DataType::Int64)),
            op: ">".into(),
            right: Box::new(lit("5")),
        };
        let plan = filter(proj, pred);
        let (out, _ctx) = optimizer.optimize(plan).unwrap();
        // Filter still on top.
        assert!(matches!(out, LogicalPlan::Filter { .. }));
    }
}
