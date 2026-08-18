//! Scheduler — assigns TaskSpec → WorkerAddr.
//!
//! Pipelined scheduling semantics: tasks are dispatched eagerly (no
//! stage barriers). The Scheduler decides *where* each task runs; when
//! is decided by `Driver.run()` itself (immediately, when scheduled).
//!
//! Capacity is the only constraint considered in M2. M3+ adds:
//!  - hash-affinity (reuse intermediate state locality)
//!  - cost-based partition count
//!  - placement constraints (colocate joins)

use crate::stage::StageDag;
use crate::task::{Partition, TaskSpec};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerId(pub u64);

impl WorkerId {
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        WorkerId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone)]
pub struct WorkerCapacity {
    pub max_drivers: usize,
    pub max_memory: usize,
}

impl WorkerCapacity {
    /// M2 default: `min(2 × ncpu, 16)` drivers; 4 GiB soft memory cap.
    pub fn default_for_ncpu(ncpu: usize, total_memory_bytes: usize) -> Self {
        Self {
            max_drivers: (2 * ncpu).min(16),
            max_memory: (4 * 1024 * 1024 * 1024).min(total_memory_bytes / 2),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerAddr {
    pub id: WorkerId,
    pub socket: SocketAddr,
    pub capacity: WorkerCapacity,
    pub in_flight: usize,
}

impl WorkerAddr {
    pub fn can_admit(&self) -> bool {
        self.in_flight < self.capacity.max_drivers
    }
}

pub trait Scheduler: Send + Sync {
    /// Pipeline dispatcher: assigns all task slots across all stages to
    /// workers. Returns one entry per task.
    fn assign(
        &self,
        dag: &StageDag,
        workers: &[WorkerAddr],
        query_id: u64,
    ) -> Vec<(TaskSpec, WorkerId)>;
}

/// Default M2 strategy: bin-packing to the least-loaded worker that
/// can_admit(). Pipelined (no stage barriers) — every task is dispatched
/// as soon as it is enumerated.
#[derive(Debug, Default)]
pub struct CapacityScheduler;

impl Scheduler for CapacityScheduler {
    fn assign(
        &self,
        dag: &StageDag,
        workers: &[WorkerAddr],
        query_id: u64,
    ) -> Vec<(TaskSpec, WorkerId)> {
        use std::cell::RefCell;
        let mut out = Vec::new();

        // Use a single-writer, no-mutex model for `in_flight` updates.
        // RefCell is fine because we're sync.
        thread_local! {}
        let workers_mut: RefCell<Vec<WorkerAddr>> = RefCell::new(workers.to_vec());

        for stage in dag.in_topo_order() {
            for p in 0..stage.partition_count {
                let partition = Partition(p);
                let target = {
                    let mut w = workers_mut.borrow_mut();
                    w.iter_mut()
                        .filter(|w| w.can_admit())
                        .min_by_key(|w| w.in_flight)
                        .map(|w| {
                            w.in_flight += 1;
                            w.id
                        })
                };

                if let Some(worker_id) = target {
                    let task = TaskSpec::from_stage(stage, query_id, partition);
                    out.push((task, worker_id));
                } else {
                    // All workers busy: leave this task for the next round.
                    // Coordinator rejects the query in M2 first cut, M4 adds retry.
                    tracing::warn!(
                        target: "pylon::coord::scheduler",
                        stage_id = stage.id.0,
                        partition = p,
                        "all workers busy; task unassigned"
                    );
                }
            }
        }
        out
    }
}
