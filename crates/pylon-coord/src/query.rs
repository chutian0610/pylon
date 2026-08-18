//! Query — the top-level abstraction for one SQL submission.
//!
//! Lives in coordinator; not transmitted to workers.

use crate::stage::StageDag;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueryId(pub u64);

impl QueryId {
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        QueryId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    /// Received SQL, fragmenter hasn't finished
    Pending,
    /// Stages being scheduled on workers
    Running,
    /// All tasks reported done
    Done,
    /// At least one task failed; downstream cancelled
    Failed,
    /// User-requested cancel
    Cancelled,
}

#[derive(Debug)]
pub struct Query {
    pub id: QueryId,
    pub sql: String,
    pub state: QueryState,
    pub stage_dag: StageDag,
    pub submitted_at: Instant,
}

impl Query {
    pub fn new(sql: String, stage_dag: StageDag) -> Self {
        Self {
            id: QueryId::generate(),
            sql,
            state: QueryState::Pending,
            stage_dag,
            submitted_at: Instant::now(),
        }
    }
}
