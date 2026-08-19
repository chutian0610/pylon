//! `FragmenterRule` trait + boundary types (RFC 0005 §4 role 9, §6
//! item 4).
//!
//! The fragmenter (`pylon-coord::fragment`) consults a list of
//! `FragmenterRule` impls at every `ExecutionPlan` node it walks:
//! the first rule that returns `Some(strategy)` from `boundary_for`
//! cuts a stage boundary. The visitor emits the partitioned
//! `ExchangeSinkRpc` at the tail of the current stage and one
//! `ExchangeSource` per downstream partition followed by the
//! boundary op (built via `stage1_op_spec`) at the head of the next
//! stage.
//!
//! Adding a new rule is a single-file change: write an impl that
//! recognises the new op kind by `as_any()` downcast and returns a
//! `BoundaryStrategy`. HashJoin / Distinct / Window all fit this
//! shape and will plug in here when their `ExecutionPlan` types
//! land (M4+). The visitor does not need to change.
//!
//! Layering note: this trait deliberately does **not** depend on
//! `pylon-coord::OpSpec` (which would invert the dependency). Rules
//! return an abstract `BoundaryEmit { op_name, config_pairs }`
//! recipe; the coord-side fragmenter wraps it into an `OpSpec`.

use std::sync::Arc;

use pylon_types::PylonError;

use crate::physical::exec::ExecutionPlan;

/// Strategy the fragmenter uses when cutting a stage boundary at
/// this node. Each variant maps to a different `ExchangeSinkRpc`
/// configuration in the emitted `OpSpec`.
///
/// Mirrors the variants in RFC 0005 §4 role 9; `HashPartition` and
/// `Broadcast` carry the partition count + routing keys because the
/// worker-side exchange op needs them to encode / route rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundaryStrategy {
    /// Hash-partition both sides. Producer: `target_partitions`
    /// tasks each emit rows; rows with equal `keys` land in the same
    /// consumer task. `keys` are column names resolved against the
    /// upstream stage's output schema. Empty `keys` means
    /// round-robin by row position (rare; used by tests that
    /// exercise the framework without a real grouping column).
    ///
    /// This is the only strategy `Fragmenter` actually emits in M3.
    HashPartition {
        target_partitions: usize,
        keys: Vec<String>,
    },
    /// Broadcast. Producer side has 1 task (the broadcast source)
    /// that emits a full copy to every consumer; consumer side has
    /// `target_consumers` tasks each receiving the same stream.
    /// No `partition_keys`: the consumer-side fan-out is the
    /// broadcast op's job, not the producer's.
    Broadcast { target_consumers: usize },
    /// Single-partition gather — both sides have exactly one task.
    Single,
    /// Gather everything onto one consumer task (`N → 1`). The
    /// producer side keeps `partition_count` tasks; the consumer
    /// side collapses to 1.
    Gather,
    /// Like `Gather`, but pinned to a designated consumer worker
    /// (used by the final sink of a query so the dispatch step
    /// can route the final task to a known worker).
    GatherToOne,
}

impl BoundaryStrategy {
    /// Number of *consumer-side* partitions the strategy implies.
    /// The fragmenter emits this many `ExchangeSource` ops at the
    /// head of the next stage. For Broadcast this is
    /// `target_consumers` (one per consumer); for HashPartition
    /// `target_partitions`; for Single / Gather / GatherToOne `1`.
    pub fn partition_count(&self) -> usize {
        match self {
            BoundaryStrategy::HashPartition { target_partitions, .. } => *target_partitions,
            BoundaryStrategy::Broadcast { target_consumers } => *target_consumers,
            BoundaryStrategy::Single
            | BoundaryStrategy::Gather
            | BoundaryStrategy::GatherToOne => 1,
        }
    }

    /// Number of *producer-side* partitions. Identical to
    /// `partition_count()` for HashPartition (the legacy wire
    /// format treats both sides as N) and Broadcast (legacy treats
    /// broadcast as a hash-routed N; M4 may revise).
    pub fn producer_partition_count(&self) -> usize {
        match self {
            BoundaryStrategy::HashPartition { target_partitions, .. } => *target_partitions,
            BoundaryStrategy::Broadcast { target_consumers } => *target_consumers,
            BoundaryStrategy::Single
            | BoundaryStrategy::Gather
            | BoundaryStrategy::GatherToOne => 1,
        }
    }

    /// Stable name for log/OpSpec diagnostics. The fragmenter stores
    /// this string under `exchange_kind` on the emitted
    /// `ExchangeSinkRpc` OpSpec config so that workers can pick the
    /// right transport (`LocalChannel` vs `Flight`) without
    /// hard-coding a class of strategies.
    pub fn as_str(&self) -> &'static str {
        match self {
            BoundaryStrategy::HashPartition { .. } => "hash_partition",
            BoundaryStrategy::Broadcast { .. } => "broadcast",
            BoundaryStrategy::Single => "single",
            BoundaryStrategy::Gather => "gather",
            BoundaryStrategy::GatherToOne => "gather_to_one",
        }
    }

    /// Hash-routing keys (column names) for this strategy, or
    /// `None` for strategies that don't hash-route (Broadcast /
    /// Single / Gather / GatherToOne). The fragmenter stores these
    /// under `partition_keys` on the `ExchangeSinkRpc` OpSpec.
    pub fn partition_keys(&self) -> Option<&[String]> {
        match self {
            BoundaryStrategy::HashPartition { keys, .. } => Some(keys),
            _ => None,
        }
    }
}

/// What the rule wants emitted on the *downstream* side of a stage
/// boundary: one instance per partition, immediately after the
/// `ExchangeSource` of that partition. The fragmenter wraps each
/// `BoundaryEmit` into an `OpSpec { name, config: HashMap }`.
///
/// Splitting the rule's intent from the coord-side `OpSpec` type
/// keeps `pylon-plan` free of `pylon-coord` imports (RFC 0005 §1
/// module layout — engine crates must not depend on each other
/// sideways).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryEmit {
    pub op_name: String,
    pub config: Vec<(String, String)>,
}

impl BoundaryEmit {
    pub fn new(op_name: impl Into<String>) -> Self {
        Self {
            op_name: op_name.into(),
            config: Vec::new(),
        }
    }

    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.push((key.into(), value.into()));
        self
    }

    pub fn with_many<I, K, V>(mut self, items: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in items {
            self.config.push((k.into(), v.into()));
        }
        self
    }
}

/// One rule the fragmenter consults at every node. Returning
/// `Some(strategy)` from `boundary_for` causes the fragmenter to
/// cut a stage boundary; `None` defers to the next rule.
///
/// Object-safe so rules live in `Vec<Arc<dyn FragmenterRule>>`.
///
/// Adding a new boundary op (M4 HashJoin / Distinct / Window) is
/// purely an exercise of writing a new `impl FragmenterRule` and
/// registering it via `Fragmenter::with_rule(...)`. The fragmenter's
/// visitor code does not change.
pub trait FragmenterRule: Send + Sync {
    /// Stable name used in tracing and `Fragmenter::describe()`.
    fn name(&self) -> &str;

    /// Decide whether `node` is a stage boundary for *this* rule.
    /// First rule that returns `Some(_)` wins. The default rule
    /// order is registration order, so put more specific rules
    /// before generic ones.
    ///
    /// Returning `Some(strategy)` does **not** prevent other
    /// operators below this one from being walked — it just means
    /// the children of `node` continue in the *current* stage while
    /// the node's own OpSpec lives in the *next* stage.
    fn boundary_for(&self, node: &dyn ExecutionPlan) -> Option<BoundaryStrategy>;

    /// Default partition strategy. The fragmenter reads this when a
    /// rule returns `None` from `boundary_for` *and* the caller
    /// hasn't supplied an explicit strategy — used by the
    /// `with_strategy` builder on `Fragmenter`. (Most callers go
    /// through `boundary_for` directly and never hit this.)
    fn default_strategy(&self) -> BoundaryStrategy {
        BoundaryStrategy::HashPartition {
            target_partitions: 16,
            keys: Vec::new(),
        }
    }

    /// Build the per-partition downstream `OpSpec` (e.g.
    /// `Aggregate`, future `HashJoin` / `Distinct` / `Window`).
    /// Called once per partition at stage1 emission; the result is
    /// wrapped into an `OpSpec` by the fragmenter.
    ///
    /// Returning `Err` aborts the query with `PylonError::InvalidPlan`
    /// — the fragmenter never silently swallows a boundary it
    /// cannot emit.
    fn stage1_op_spec(&self, node: &dyn ExecutionPlan) -> Result<BoundaryEmit, PylonError>;
}

/// Built-in rule that recognises `AggregateExec` and cuts a hash
/// stage at every aggregate. This is the rule the legacy M3
/// fragmenter hard-coded into `visit_v2`; extracting it into a
/// `FragmenterRule` impl is the point of R3.
///
/// M3 only ships `AggregateExec`; once `HashJoinExec` /
/// `DistinctExec` / `WindowExec` land, sibling rules
/// (`HashJoinRule`, `DistinctRule`, `WindowRule`) plug in via
/// `Fragmenter::with_rule(...)` without editing the visitor.
pub struct AggregateFragmenterRule {
    pub target_partitions: usize,
}

impl AggregateFragmenterRule {
    pub fn new(target_partitions: usize) -> Self {
        Self { target_partitions }
    }
}

impl Default for AggregateFragmenterRule {
    fn default() -> Self {
        // Mirrors `FragmenterConfig::default_partition_count`.
        Self::new(16)
    }
}

impl FragmenterRule for AggregateFragmenterRule {
    fn name(&self) -> &str {
        "AggregateFragmenterRule"
    }

    fn boundary_for(&self, node: &dyn ExecutionPlan) -> Option<BoundaryStrategy> {
        use crate::physical::exec::AggregateExec;
        use crate::physical::expr::ColumnExpr;

        let agg = node.as_any().downcast_ref::<AggregateExec>()?;
        let keys: Vec<String> = agg
            .group_by
            .iter()
            .filter_map(|e| {
                e.as_any()
                    .downcast_ref::<ColumnExpr>()
                    .map(|c| c.field.name().to_string())
            })
            .collect();
        Some(BoundaryStrategy::HashPartition {
            target_partitions: self.target_partitions,
            keys,
        })
    }

    fn default_strategy(&self) -> BoundaryStrategy {
        BoundaryStrategy::HashPartition {
            target_partitions: self.target_partitions,
            keys: Vec::new(),
        }
    }

    fn stage1_op_spec(&self, node: &dyn ExecutionPlan) -> Result<BoundaryEmit, PylonError> {
        use crate::physical::exec::AggregateExec;
        use crate::physical::expr::{AggregateFunctionExpr, ColumnExpr};

        let agg = node.as_any().downcast_ref::<AggregateExec>().ok_or_else(|| {
            PylonError::Internal(format!(
                "AggregateFragmenterRule::stage1_op_spec: node '{}' is not AggregateExec",
                node.name()
            ))
        })?;

        // Match the byte-for-byte legacy format from
        // `pylon-coord::fragment::agg_spec_to_string_v2`. Any change
        // here is a wire-format break with the worker's op factory
        // (`pylon-worker::main::build_aggregate_op`).
        let group_cols: Vec<String> = agg
            .group_by
            .iter()
            .map(|e| match e.as_any().downcast_ref::<ColumnExpr>() {
                Some(c) => c.field.name().to_string(),
                None => "_".into(),
            })
            .collect();

        let agg_specs: Vec<String> = agg
            .aggs
            .iter()
            .map(|e| {
                if let Some(a) = e.as_any().downcast_ref::<AggregateFunctionExpr>() {
                    if a.func == "count" && a.args.is_empty() {
                        "count()".to_string()
                    } else {
                        let arg = match a.args.first() {
                            Some(c) => match c.as_any().downcast_ref::<ColumnExpr>() {
                                Some(cc) => cc.field.name().to_string(),
                                None => "*".into(),
                            },
                            None => "*".into(),
                        };
                        format!("{}:{}", a.name, arg)
                    }
                } else {
                    "?".to_string()
                }
            })
            .collect();

        Ok(BoundaryEmit::new("Aggregate")
            .with("group_by_cols", group_cols.join(","))
            .with("agg_specs", agg_specs.join(";")))
    }
}

/// Helper for tests / callers that want to know "would this rule
/// fire here?" without owning a `Fragmenter`. Useful in optimizer
/// passes that want to dry-run before planning.
pub fn rule_fires(
    rules: &[Arc<dyn FragmenterRule>],
    node: &dyn ExecutionPlan,
) -> Option<(usize, BoundaryStrategy)> {
    for (i, rule) in rules.iter().enumerate() {
        if let Some(s) = rule.boundary_for(node) {
            return Some((i, s));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    use crate::physical::exec::{AggregateExec, SeqScanExec};
    use crate::physical::expr::{AggregateFunctionExpr, ColumnExpr, PhysicalExpr};

    fn scan() -> Arc<dyn ExecutionPlan> {
        let s: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("c0", DataType::Int64, false)]));
        Arc::new(SeqScanExec::new("t", s))
    }

    fn agg(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let s = input.schema();
        let g: Vec<Arc<dyn PhysicalExpr>> =
            vec![Arc::new(ColumnExpr::new(0, s.field(0).clone()))];
        let a: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
            AggregateFunctionExpr::new(
                "count",
                "count_c0",
                vec![],
                DataType::Int64,
                vec![],
            ),
        )];
        Arc::new(AggregateExec::new(input, g, a, s))
    }

    #[test]
    fn boundary_strategy_partition_count_is_consistent() {
        assert_eq!(
            BoundaryStrategy::HashPartition {
                target_partitions: 8,
                keys: vec!["k".into()],
            }
            .partition_count(),
            8
        );
        assert_eq!(
            BoundaryStrategy::Broadcast { target_consumers: 4 }.partition_count(),
            4
        );
        assert_eq!(BoundaryStrategy::Single.partition_count(), 1);
        assert_eq!(BoundaryStrategy::Gather.partition_count(), 1);
        assert_eq!(BoundaryStrategy::GatherToOne.partition_count(), 1);
    }

    #[test]
    fn boundary_strategy_as_str_is_stable() {
        // Wire-format: workers may switch on this string. Changing
        // it is a breaking change for the worker op factory.
        assert_eq!(
            BoundaryStrategy::HashPartition {
                target_partitions: 1,
                keys: vec![],
            }
            .as_str(),
            "hash_partition"
        );
        assert_eq!(
            BoundaryStrategy::Broadcast { target_consumers: 1 }.as_str(),
            "broadcast"
        );
        assert_eq!(BoundaryStrategy::Single.as_str(), "single");
        assert_eq!(BoundaryStrategy::Gather.as_str(), "gather");
        assert_eq!(BoundaryStrategy::GatherToOne.as_str(), "gather_to_one");
    }

    #[test]
    fn boundary_strategy_partition_keys_returns_keys_for_hash() {
        let h = BoundaryStrategy::HashPartition {
            target_partitions: 2,
            keys: vec!["a".into(), "b".into()],
        };
        assert_eq!(h.partition_keys(), Some(&["a".to_string(), "b".to_string()][..]));
        assert_eq!(
            BoundaryStrategy::Broadcast { target_consumers: 1 }.partition_keys(),
            None
        );
        assert_eq!(BoundaryStrategy::Single.partition_keys(), None);
    }

    #[test]
    fn boundary_emit_builder_round_trips() {
        let e = BoundaryEmit::new("Aggregate")
            .with("k", "v")
            .with_many(vec![("a", "1"), ("b", "2")]);
        assert_eq!(e.op_name, "Aggregate");
        assert_eq!(
            e.config,
            vec![
                ("k".to_string(), "v".to_string()),
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ]
        );
    }

    #[test]
    fn aggregate_rule_fires_only_for_aggregate() {
        let rule = AggregateFragmenterRule::default();
        let scan_node: Arc<dyn ExecutionPlan> = scan();
        let agg_node: Arc<dyn ExecutionPlan> = agg(scan_node.clone());

        assert!(rule.boundary_for(scan_node.as_ref()).is_none());
        let s = rule
            .boundary_for(agg_node.as_ref())
            .expect("aggregate boundary");
        assert_eq!(
            s,
            BoundaryStrategy::HashPartition {
                target_partitions: 16,
                keys: vec!["c0".into()],
            }
        );
    }

    #[test]
    fn aggregate_rule_default_strategy_matches_constructor() {
        let r = AggregateFragmenterRule::new(7);
        assert_eq!(
            r.default_strategy(),
            BoundaryStrategy::HashPartition {
                target_partitions: 7,
                keys: vec![],
            }
        );
    }

    #[test]
    fn aggregate_rule_stage1_op_spec_carries_group_by_and_aggs() {
        let rule = AggregateFragmenterRule::default();
        let agg_node: Arc<dyn ExecutionPlan> = agg(scan());
        let emit = rule
            .stage1_op_spec(agg_node.as_ref())
            .expect("emit ok");
        assert_eq!(emit.op_name, "Aggregate");
        let cfg: std::collections::HashMap<String, String> =
            emit.config.into_iter().collect();
        assert_eq!(cfg.get("group_by_cols").map(String::as_str), Some("c0"));
        assert_eq!(cfg.get("agg_specs").map(String::as_str), Some("count()"));
    }

    #[test]
    fn aggregate_rule_stage1_op_spec_errors_on_wrong_node() {
        let rule = AggregateFragmenterRule::default();
        let scan_node: Arc<dyn ExecutionPlan> = scan();
        let err = rule
            .stage1_op_spec(scan_node.as_ref())
            .expect_err("non-aggregate must error");
        let s = format!("{err}");
        assert!(s.contains("AggregateFragmenterRule"), "got: {s}");
    }

    #[test]
    fn rule_fires_helper_returns_first_match() {
        let r1 = Arc::new(AggregateFragmenterRule::default());
        let r2 = Arc::new(AggregateFragmenterRule::new(8));
        let rules: Vec<Arc<dyn FragmenterRule>> = vec![r1.clone(), r2.clone()];
        let agg_node: Arc<dyn ExecutionPlan> = agg(scan());
        let (idx, strategy) =
            rule_fires(&rules, agg_node.as_ref()).expect("first rule fires");
        assert_eq!(idx, 0, "first rule wins (16, not 8)");
        assert_eq!(
            strategy,
            BoundaryStrategy::HashPartition {
                target_partitions: 16,
                keys: vec!["c0".into()],
            }
        );
    }

    #[test]
    fn rule_fires_helper_returns_none_when_no_rule_matches() {
        let rules: Vec<Arc<dyn FragmenterRule>> =
            vec![Arc::new(AggregateFragmenterRule::default())];
        let scan_node: Arc<dyn ExecutionPlan> = scan();
        assert!(rule_fires(&rules, scan_node.as_ref()).is_none());
    }

    #[test]
    fn aggregate_rule_default_partition_count_matches_legacy_default() {
        // M3 baseline: `FragmenterConfig::default_partition_count`
        // is 16. The rule must default to the same number so a
        // caller that constructs `Fragmenter::new(cfg)` (no
        // explicit rules) gets the same partition count as
        // `Fragmenter::with_rule(AggregateFragmenterRule::default())`.
        assert_eq!(AggregateFragmenterRule::default().target_partitions, 16);
    }
}
