//! Driver — single-thread poll loop over one [`Pipeline`].
//!
//! One driver = one Pipeline on one OS thread. All ops live in a
//! `Vec<Box<dyn PipelineOp>>`; the loop polls them sequentially and
//! never holds a `Mutex` across an `await` inside ops. This matches
//! Velox's `runInternal` shape and is the only execution mode we
//! ship in M3+ (`RFC 0005 § 7.1 R5-pre`).
//!
//! M1/M2's per-op-as-tokio-task path (`SharedPerOpTask` enum arm +
//! `run_per_op_task_legacy` + `run_legacy_op`) was removed in
//! R5-pre: it was dead code in any test that the repo runs today.

use crate::pipeline::{run_pipeline_single_thread, Pipeline};
use pylon_types::Result as PylonResult;
use pylon_types::RecordBatch;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tracing::info;

/// Stable per-driver identifier. Used by the engine trace layer
/// and (future) by stats collection; not wire-serialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DriverId(pub u64);

impl DriverId {
    /// Monotonic counter. One [`Driver`] per call. `Arc<Pipeline>`
    /// is gone — once a driver owns its pipeline by value, only the
    /// external producer holds a `Receiver` to push batches in.
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        DriverId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// One per-pipeline execution unit. Owns the pipeline by value; the
/// runtime guarantees no concurrent driver-task sharing of ops.
pub struct Driver {
    pub id: DriverId,
    pipeline: Pipeline,
}

impl Driver {
    pub fn new(pipeline: Pipeline) -> Self {
        Self {
            id: DriverId::generate(),
            pipeline,
        }
    }

    pub fn id(&self) -> DriverId {
        self.id
    }

    pub fn pipeline_id(&self) -> crate::pipeline::PipelineId {
        self.pipeline.id
    }

    /// Drive one pipeline to completion. `external_input` is the
    /// optional ingress feed (e.g. coord-supplied initial batches);
    /// returns the egress receiver — the caller collects final
    /// batches from it.
    pub async fn run(
        self,
        external_input: Option<mpsc::Receiver<RecordBatch>>,
    ) -> PylonResult<mpsc::Receiver<RecordBatch>> {
        info!(
            target: "pylon::driver",
            driver_id = self.id.0,
            pipeline_id = self.pipeline.id.0,
            ops = self.pipeline.ops.len(),
            "driver starting"
        );
        let id = self.pipeline.id;
        run_pipeline_single_thread(self.pipeline, external_input, id).await
    }
}
