//! Coordinator — query staging and scheduling.
//!
//! Implements the upper three Trino-aligned abstraction levels:
//!   Query  — one per SQL submission
//!   Stage  — one per fragment (shuffle boundary)
//!   Task   — Stage × Partition, scheduled to a worker
//!
//! The coordinator owns `StageDag` (output of Fragmenter) and uses a
//! `Scheduler` impl to assign `TaskSpec` instances to workers.
//!
//! Note: this M2 milestone only contains the data structures and
//! scheduler logic. The gRPC / HTTP transport and `pylon-coord` binary
//! come in M2 weeks 1-2.

pub mod query;
pub mod stage;
pub mod task;
pub mod scheduler;
pub mod discovery;
pub mod fragment;

pub use query::{Query, QueryId, QueryState};
pub use stage::{
    Fragment, OpSpec, Stage, StageDag, StageId, Distribution, DEFAULT_PARTITION_COUNT,
};
pub use task::{ExchangeKind, ExchangeSpec, Partition, TaskId, TaskSpec};
pub use scheduler::{
    CapacityScheduler, Scheduler, WorkerAddr, WorkerCapacity, WorkerId,
};
pub use discovery::{Discovery, RegisteredWorker};
pub use fragment::{Fragmenter, FragmenterConfig};
