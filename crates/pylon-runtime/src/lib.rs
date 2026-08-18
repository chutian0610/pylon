//! Pylon pipeline runtime — Trino-aligned execution model.
//!
//! Hierarchy:
//!   Pipeline     (op chain + state bridges, shared across drivers)
//!   Driver       (single execution unit, single-threaded model in M5+,
//!                 tokio per-op-task fallback in M2)
//!   PipelineOp   (Velox-aligned operator interface: needs_input / add_input /
//!                 get_output / no_more_input / is_finished / is_blocked / close)
//!
//! See RFC-0002 for the layer model.

pub mod op;
pub mod pipeline;
pub mod driver;
pub mod bridge;
pub mod error;
pub mod ops;

pub use op::PipelineOp;
pub use pipeline::{Pipeline, PipelineId, run_pipeline_per_op_task};
pub use driver::{Driver, DriverId, DriverMode};
pub use bridge::{DummyBridge, StateBridge, StateChange};
pub use error::RuntimeError;
