//! Stage — fragmenter output.
//!
//! A Stage corresponds to one "shuffle-free" unit of the plan. Stages are
//! connected by Exchanges (HashPartition / Broadcast / Gather).
//!
//! `StageDag` is the ordered list of all stages; the Fragmenter is
//! responsible for the layout (and guarantees the dependency graph is a DAG).

use std::collections::HashMap;

pub const DEFAULT_PARTITION_COUNT: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StageId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distribution {
    /// One task only — gather-style
    Single,
    /// N tasks — hash or range partitioned
    Partitioned(usize),
    /// All workers receive a full copy — typically for small build side of join
    Broadcast,
}

impl Distribution {
    pub fn partition_count(&self) -> usize {
        match self {
            Distribution::Single => 1,
            Distribution::Partitioned(n) => *n,
            Distribution::Broadcast => 1, // source-side only (one producer)
        }
    }

    pub fn is_broadcast(&self) -> bool {
        matches!(self, Distribution::Broadcast)
    }
}

/// Operator specification in serializable form.
///
/// M2: a name + a flat config map. M3+ will use gRPC protobuf fields.
#[derive(Debug, Clone)]
pub struct OpSpec {
    pub name: String,
    pub config: HashMap<String, String>,
}

impl OpSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: HashMap::new(),
        }
    }

    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }
}

/// A Fragment = a chain of `OpSpec`s plus the distribution pattern at
/// which this fragment produces output for downstream stages.
#[derive(Debug, Clone)]
pub struct Fragment {
    pub ops: Vec<OpSpec>,
    pub distribution: Distribution,
}

impl Fragment {
    pub fn new(distribution: Distribution) -> Self {
        Self {
            ops: Vec::new(),
            distribution,
        }
    }

    pub fn with_op(mut self, op: OpSpec) -> Self {
        self.ops.push(op);
        self
    }
}

#[derive(Debug, Clone)]
pub struct Stage {
    pub id: StageId,
    pub fragment: Fragment,
    pub partition_count: usize,
    pub memory_budget_bytes: usize,
    pub upstream: Vec<StageId>,
    pub downstream: Vec<StageId>,
}

impl Stage {
    pub fn new(id: StageId, fragment: Fragment) -> Self {
        Self {
            id,
            partition_count: fragment.distribution.partition_count(),
            fragment,
            memory_budget_bytes: 256 * 1024 * 1024, // 256 MiB default
            upstream: Vec::new(),
            downstream: Vec::new(),
        }
    }

    pub fn with_partition_count(mut self, n: usize) -> Self {
        self.partition_count = n;
        self
    }

    pub fn with_memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget_bytes = bytes;
        self
    }
}

/// Stage DAG — Fragmenter output. Stages in `stages` are in topological
/// order (sources first, sinks last).
#[derive(Debug, Clone, Default)]
pub struct StageDag {
    pub stages: Vec<Stage>,
}

impl StageDag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stage(mut self, stage: Stage) -> Self {
        self.stages.push(stage);
        self
    }

    pub fn tasks_total(&self) -> usize {
        self.stages.iter().map(|s| s.partition_count).sum()
    }

    /// Returns stages in topological order (assumes `stages` already sorted).
    pub fn in_topo_order(&self) -> &[Stage] {
        &self.stages
    }
}
