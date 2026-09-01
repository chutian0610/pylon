//! Pylon pipeline runtime — Trino-aligned execution model.
//!
//! Hierarchy:
//!   Pipeline     (op chain + state bridges, owned by one Driver)
//!   Driver       (single execution unit, single-thread poll loop)
//!   PipelineOp   (Velox-aligned operator interface: needs_input / add_input /
//!                 get_output / no_more_input / is_finished / is_blocked / close)
//!
//! See RFC-0002 for the layer model and RFC 0005 § 7.1 R5-pre for the
//! drop-history of the per-op-tokio-task legacy mode.

pub mod bridge;
pub mod driver;
pub mod error;
pub mod memory_pool;
pub mod op;
pub mod ops;
pub mod pipeline;
pub mod spill;

pub use bridge::{DummyBridge, StateBridge, StateChange};
pub use driver::{Driver, DriverId};
pub use error::RuntimeError;
pub use memory_pool::{NoopMemoryPool, PerTaskPool};
pub use op::PipelineOp;
pub use pipeline::{Pipeline, PipelineId, run_pipeline_single_thread};
pub use spill::{SpillHandle, SpillManager, Spillable};
