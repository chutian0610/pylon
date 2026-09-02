//! `OpRegistry` — maps op-name strings (sent over the gRPC `OpSpec`)
//! to factory closures that build the corresponding `PipelineOp`.
//!
//! Replaces the `match name { "SeqScan" => … }` giant match in the
//! pre-R2 worker factory (RFC 0005 § 6 item 5). Adding a new op is
//! one `register(...)` line instead of editing a match arm.
//!
//! Mirrors Velox's `PlanNodeTranslator` (a global registry of
//! `PlanNode → Operator` translators). New ops register at
//! startup; the worker binary calls `registry().build(name, cfg, ctx, flight)`
//! per `OpSpec`.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use anyhow::{Result, anyhow};
use pylon_exchange::PylonFlightService;
use pylon_runtime::spill::SpillHandle;
use pylon_runtime::{PerTaskPool, PipelineOp};
use std::sync::Arc as StdArc;

/// Per-task execution context passed to every op factory. Carries
/// the knobs an op cannot get from its `OpSpec` config alone:
/// the task memory budget (from `TaskSpec.memory_budget_bytes`)
/// and the spill-checkpoint callback (RFC 0007 §3.5).
pub struct TaskContext {
    /// Per-task byte budget; `0` = unbounded (no-op pool).
    pub memory_budget: u64,
    /// Invoked by an op after it persists a spill checkpoint.
    /// The worker maps this to a `TASK_STALLED` ack so the coord
    /// holds a resume point for fault-tolerant re-dispatch.
    pub on_spill: Option<StdArc<dyn Fn(SpillHandle) + Send + Sync>>,
}

pub type OpFactory = dyn Fn(
        &HashMap<String, String>,
        &TaskContext,
        Arc<PylonFlightService>,
    ) -> Result<Box<dyn PipelineOp>>
    + Send
    + Sync;

pub struct OpRegistry {
    factories: HashMap<&'static str, Box<OpFactory>>,
}

impl OpRegistry {
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Register a factory under `name`. Chainable; the `register_*`
    /// helpers in this module call this in a single expression.
    pub fn register<F>(mut self, name: &'static str, factory: F) -> Self
    where
        F: Fn(
                &HashMap<String, String>,
                &TaskContext,
                Arc<PylonFlightService>,
            ) -> Result<Box<dyn PipelineOp>>
            + Send
            + Sync
            + 'static,
    {
        self.factories.insert(name, Box::new(factory));
        self
    }

    /// Look up + invoke a factory. Returns an error if `name` is
    /// unknown — that's the worker-side counterpart of the
    /// enum-variants-must-match invariant: a typo in the fragmenter
    /// is caught at runtime instead of silently dropping ops.
    pub fn build(
        &self,
        name: &str,
        config: &HashMap<String, String>,
        ctx: &TaskContext,
        flight_service: Arc<PylonFlightService>,
    ) -> Result<Box<dyn PipelineOp>> {
        let factory = self
            .factories
            .get(name)
            .ok_or_else(|| anyhow!("unknown op: {name}"))?;
        factory(config, ctx, flight_service)
    }
}

/// Static singleton. Allocated once on first call to
/// [`registry()`] (typically in `main()`); threads return `&'static`
/// thereafter.
static REGISTRY: OnceLock<OpRegistry> = OnceLock::new();

/// Returns the global `OpRegistry`, building it lazily on first
/// call. The first call registers every op the worker knows how to
/// run; subsequent calls return the cached registry.
pub fn registry() -> &'static OpRegistry {
    REGISTRY.get_or_init(build_default_registry)
}

fn build_default_registry() -> OpRegistry {
    use pylon_runtime::ops::{
        ExchangeSinkRpc, ExchangeSourceOp, FilterOp, HashAggregateOp, PartitionFilterOp, ProjectOp,
        SeqScanOp,
    };

    let get = |cfg: &HashMap<String, String>, k: &str| -> Result<String> {
        cfg.get(k)
            .cloned()
            .ok_or_else(|| anyhow!("missing config key {k}"))
    };

    OpRegistry::new()
        .register("SeqScan", move |cfg, _ctx, _flight| {
            Ok(Box::new(SeqScanOp::new(get(cfg, "path")?, 8192)))
        })
        .register("Filter", move |cfg, _ctx, _flight| {
            Ok(Box::new(FilterOp::new(
                get(cfg, "col")?,
                get(cfg, "op")?,
                get(cfg, "literal")?,
            )))
        })
        .register("Project", move |cfg, _ctx, _flight| {
            let cols: Vec<String> = get(cfg, "cols")?
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let schema = Arc::new(arrow_schema::Schema::empty());
            Ok(Box::new(ProjectOp::new(cols, schema)))
        })
        .register("PartitionFilter", move |cfg, _ctx, _flight| {
            Ok(Box::new(
                PartitionFilterOp::new(get(cfg, "col")?, &get(cfg, "literal")?)?,
            ))
        })
        .register("ExchangeSource", move |cfg, _ctx, flight| {
            let desc = pylon_exchange::FlightDescriptor(get(cfg, "descriptor")?);
            Ok(Box::new(ExchangeSourceOp::new(desc, flight)))
        })
        .register("ExchangeSinkRpc", move |cfg, _ctx, _flight| {
            let descs: Vec<pylon_exchange::FlightDescriptor> = get(cfg, "descriptors")?
                .split(';')
                .filter(|s| !s.is_empty())
                .map(|s| pylon_exchange::FlightDescriptor(s.to_string()))
                .collect();
            let flight_addrs: Vec<String> = get(cfg, "target_flight_addrs")?
                .split(';')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if flight_addrs.len() != descs.len() {
                anyhow::bail!(
                    "ExchangeSinkRpc: target_flight_addrs ({}) and descriptors ({}) length mismatch",
                    flight_addrs.len(),
                    descs.len()
                );
            }
            let partition_keys: Vec<String> = get(cfg, "partition_keys")?
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let targets: Vec<pylon_runtime::ops::RpcTarget> = flight_addrs
                .into_iter()
                .zip(descs)
                .map(|(flight_addr, descriptor)| pylon_runtime::ops::RpcTarget {
                    flight_addr,
                    descriptor,
                })
                .collect();
            Ok(Box::new(
                ExchangeSinkRpc::new_partitioned(targets, partition_keys),
            ))
        })
        .register("Aggregate", move |cfg, ctx, _flight| {
            let group_by_cols: Vec<String> = get(cfg, "group_by_cols")?
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let aggregates = parse_agg_specs(&get(cfg, "agg_specs")?)?;
            // Schema::empty() — the op derives it on first input batch.
            let schema = Arc::new(arrow_schema::Schema::empty());
            // M4.S7: honor the task's memory budget — a tight budget
            // is what makes the spill → TASK_STALLED checkpoint path
            // actually fire in production.
            let mut op = if ctx.memory_budget > 0 {
                HashAggregateOp::with_pool(
                    group_by_cols,
                    aggregates,
                    schema,
                    PerTaskPool::new(ctx.memory_budget as usize),
                )
            } else {
                HashAggregateOp::new(group_by_cols, aggregates, schema)
            };
            // RFC 0007 §3.5: report every spill checkpoint upstream.
            if let Some(cb) = ctx.on_spill.clone() {
                op = op.with_on_spill(cb);
            }
            // RFC 0007 §3.5: a coord-retried task carries the spill
            // handle of the stalled attempt; the op resumes from it
            // at no_more_input instead of restarting from scratch.
            if let Some(key) = cfg.get("spill_handle").filter(|k| !k.is_empty()) {
                let handle = pylon_runtime::spill::SpillHandle {
                    path: std::path::PathBuf::from(key),
                    bytes: 0,
                    seq: 0,
                };
                op = op.with_pending_resume(handle);
            }
            Ok(Box::new(op))
        })
}

/// Parse `agg_specs` config value into `AggSpec`s. Taken verbatim
/// from the legacy `build_op`; lifted here so the closure body
/// stays short.
fn parse_agg_specs(specs: &str) -> Result<Vec<pylon_runtime::ops::AggSpec>> {
    use pylon_runtime::ops::AggSpec;
    let mut out = Vec::new();
    for spec in specs.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if let Some(inner) = spec
            .strip_prefix("count(")
            .and_then(|s| s.strip_suffix(")"))
        {
            if !inner.is_empty() {
                anyhow::bail!("count() takes no arguments; got count({inner})");
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
                anyhow::bail!("aggregate {func}() requires a column");
            }
            out.push(AggSpec {
                func,
                arg_col: Some(col.to_string()),
                out_name: spec.to_string(),
            });
        } else {
            anyhow::bail!("malformed agg spec: {spec}");
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_config() -> HashMap<String, String> {
        HashMap::new()
    }

    fn empty_task_context() -> TaskContext {
        TaskContext {
            memory_budget: 0,
            on_spill: None,
        }
    }

    #[test]
    fn registry_unknown_op_is_an_error() {
        let r = OpRegistry::new();
        let result = r.build(
            "DoesNotExist",
            &empty_config(),
            &empty_task_context(),
            build_flight_stub(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn registry_build_default_covers_every_m3_op() {
        // build the singleton (runs once).
        let r = registry();
        // 7 ops are registered; if a new operator is added to
        // `build_default_registry` without adding it here, this
        // test fails on the next PR-review pass.
        let names: Vec<&'static str> = vec![
            "SeqScan",
            "Filter",
            "Project",
            "PartitionFilter",
            "ExchangeSource",
            "ExchangeSinkRpc",
            "Aggregate",
        ];
        assert_eq!(
            r.factories
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            names.into_iter().collect::<std::collections::BTreeSet<_>>(),
        );
    }

    // A dummy flight service for tests that don't actually exercise
    // exchange. The real `PylonFlightService` is fine to construct
    // empty — it has no required fields.
    fn build_flight_stub() -> Arc<PylonFlightService> {
        Arc::new(PylonFlightService::new())
    }
}
