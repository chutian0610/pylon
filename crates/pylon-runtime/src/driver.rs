//! Driver: a single-threaded (or tokio-fused) execution unit for a Pipeline.
//!
//! This is the Trino-aligned abstraction:
//! - One Pipeline can have M parallel Drivers
//! - Each Driver walks the same Pipeline's op chain (potentially shared state)
//! - Driver is **single-threaded** per Trino semantics
//!
//! ## M2 default: per-op-as-tokio-task
//! We don't actually achieve "single-thread fused" in M2 yet. To keep M2
//! async-friendly (Flight, async-Parquet reads), Driver::run() defaults to
//! `DriverMode::PerOpTokioTask`, which is the legacy model where each
//! PipelineOp spawns its own tokio task and ops communicate via mpsc.
//!
//! ## M5+ target: SingleThreadLoop
//! Will replace per-op-task with a true single-thread driver loop where ops
//! are called directly (no mpsc) and only `tokio::task::block_in_place` is
//! used for CPU kernels. Tracked by RFC-0005 (future).

use crate::pipeline::{Pipeline, run_pipeline_per_op_task};
use pylon_types::RecordBatch;
use pylon_types::Result as PylonResult;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum DriverMode {
    /// M2 default: each PipelineOp becomes a tokio::spawn task;
    /// ops communicate via bounded mpsc. Async-friendly but no fusion.
    #[default]
    PerOpTokioTask,
    /// M5+: single-thread poll loop calling ops directly. Trino/Velox model.
    /// Currently a stub that delegates to PerOpTokioTask.
    SingleThreadLoop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DriverId(pub u64);

impl DriverId {
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        DriverId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// One Driver = one execution unit for one slice of work over a Pipeline.
///
/// In M2 we have driver_count == 1 per Pipeline (no multi-driver fan-in yet).
/// The struct gives the right name and shape so M5+ can up driver_count
/// without touching call sites.
pub struct Driver {
    pub id: DriverId,
    pub pipeline: Arc<Pipeline>,
    pub mode: DriverMode,
}

impl Driver {
    pub fn new(pipeline: Arc<Pipeline>) -> Self {
        Self {
            id: DriverId::generate(),
            pipeline,
            mode: DriverMode::default(),
        }
    }

    pub fn with_mode(mut self, mode: DriverMode) -> Self {
        self.mode = mode;
        self
    }

    /// Run this driver to completion.
    ///
    /// M2 default: `PerOpTokioTask` — the Pipeline's ops are spawned as
    /// concurrent tokio tasks, wired via bounded mpsc channels. M5+:
    /// `SingleThreadLoop` will run a synchronous driver loop in a
    /// dedicated OS thread / `block_in_place`.
    pub async fn run(
        self,
        input: Option<mpsc::Receiver<RecordBatch>>,
    ) -> PylonResult<mpsc::Receiver<RecordBatch>> {
        info!(
            target: "pylon::driver",
            driver_id = self.id.0,
            pipeline_id = self.pipeline.id.0,
            ops = self.pipeline.op_count(),
            mode = ?self.mode,
            "driver starting"
        );
        match self.mode {
            DriverMode::PerOpTokioTask => {
                run_pipeline_per_op_task(self.pipeline, input).await
            }
            DriverMode::SingleThreadLoop => {
                // TODO(M5+): implement single-thread fused driver.
                // For now, fall back so M2 keeps working.
                run_pipeline_per_op_task(self.pipeline, input).await
            }
        }
    }
}

// Re-export PipelineId from pipeline module for the runtime crate root.
pub use crate::pipeline::PipelineId as _PipelineId;
