//! Plan-level metadata published by each operator.
//!
//! RFC 0005 § 4 role 5 (`ExecutionPlan` trait) and role 10
//! (`PlanProperties`). Mirrors DataFusion's `ExecutionPlan::properties()`
//! shape: a snapshot of `distribution` / `boundedness` / `emission`
//! built once per node, never recomputed unless children change.

use std::sync::Arc;

use crate::physical::expr::PhysicalExpr;

/// How the operator's output rows are split across partitions. The
/// coordinator's `Fragmenter` reads this off every `PhysicalPlan`
/// node to decide where (if anywhere) to cut an exchange.
#[derive(Debug, Clone)]
pub enum Distribution {
    /// Single partition (Sink-side; no fan-out).
    Single,
    /// Round-robin broadcast (data is irrelevant to which partition).
    RoundRobin { partition_count: usize },
    /// Hash partitioning on `keys`. Rows with equal `keys` hash land
    /// in the same partition. The dispatcher is responsible for
    /// picking the partition count actually materialized at runtime
    /// (the `partition_count` here is a hint, not a hard limit).
    Hash {
        keys: Vec<Arc<dyn PhysicalExpr>>,
        partition_count: usize,
    },
    /// All operators receive a copy. Used for small build-side joins
    /// (broadcast exchange) — not yet emitted by the fragmenter in M3.
    Broadcast,
    /// Partitioning declared unknown — costs a downstream
    /// `RepartitionExec` if a downstream op requires a specific shape.
    Unknown { estimated_count: usize },
}

/// Whether the operator ever stops producing rows. Streaming sources
/// (Kafka, file-tail) flip this to `Unbounded`. All M3 ops are
/// `Bounded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Boundedness {
    Bounded,
    Unbounded,
}

/// How the operator emits rows: incrementally as they arrive (stateful
/// backpressure), final-only (e.g. aggregate), or both (e.g. some
/// hash join probes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmissionType {
    Incremental,
    Final,
    Both,
}

/// Immutable snapshot of an operator's static metadata. Constructed
/// once per (immutable plan) and reused by the coordinator's
/// fragmenter, the scheduler, and (future) CBO.
#[derive(Debug, Clone)]
pub struct PlanProperties {
    pub distribution: Distribution,
    pub output_ordering: Option<Vec<Arc<dyn PhysicalExpr>>>,
    pub boundedness: Boundedness,
    pub emission: EmissionType,
}

impl PlanProperties {
    /// Default for a leaf scan: single-partition, unordered, bounded,
    /// incremental emission.
    pub fn leaf_scan() -> Self {
        Self {
            distribution: Distribution::Single,
            output_ordering: None,
            boundedness: Boundedness::Bounded,
            emission: EmissionType::Incremental,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_scan_defaults_match_expectations() {
        let p = PlanProperties::leaf_scan();
        assert!(matches!(p.distribution, Distribution::Single));
        assert!(p.output_ordering.is_none());
        assert_eq!(p.boundedness, Boundedness::Bounded);
        assert_eq!(p.emission, EmissionType::Incremental);
    }
}
