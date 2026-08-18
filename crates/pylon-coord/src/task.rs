//! Task — Stage × Partition.
//!
//! One TaskSpec is the unit the coordinator schedules to a worker. The
//! worker constructs a `Pipeline` (from the Fragment) and runs N Drivers
//! on it (M2 has DriverCount == 1).
//!
//! ExchangeSpec is the wiring between tasks: each source/sink links to
//! the matching end on a peer task.

use crate::stage::{Fragment, Stage, StageId};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(pub u64);

impl TaskId {
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        TaskId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Partition(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeKind {
    /// Hash/range partitioned shuffle
    Partitioned,
    /// One-to-all broadcast
    Broadcast,
    /// All-to-one gather
    Gather,
    /// Same-worker intra-pipeline (no network)
    Local,
}

/// Wire routing for one side of an exchange.
#[derive(Debug, Clone)]
pub struct ExchangeSpec {
    pub kind: ExchangeKind,
    pub target_worker: SocketAddr,
    pub target_partition: Partition,
    pub source_partition: Partition,
}

#[derive(Debug, Clone)]
pub struct TaskSpec {
    pub id: TaskId,
    pub query_id: u64,
    pub stage_id: StageId,
    pub partition: Partition,
    pub fragment: Fragment,
    pub sources: Vec<ExchangeSpec>,
    pub sinks: Vec<ExchangeSpec>,
    pub memory_budget_bytes: usize,
}

impl TaskSpec {
    pub fn from_stage(stage: &Stage, query_id: u64, partition: Partition) -> Self {
        Self {
            id: TaskId::generate(),
            query_id,
            stage_id: stage.id,
            partition,
            fragment: stage.fragment.clone(),
            sources: Vec::new(),
            sinks: Vec::new(),
            memory_budget_bytes: stage.memory_budget_bytes,
        }
    }
}
