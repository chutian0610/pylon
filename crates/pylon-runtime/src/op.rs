//! PipelineOp — the canonical operator trait (Velox-aligned).
//!
//! Semantically maps to Velox's `velox::exec::Operator`:
//! - `needs_input`: is the op ready for the next input batch?
//! - `add_input`:   push a batch into the op
//! - `get_output`:  pull one batch (if any) for downstream
//! - `no_more_input`: signal end of upstream
//! - `is_finished`:   is the op fully drained?
//! - `is_blocked`:    is the op waiting on external I/O? (new in this RFC)
//! - `close`:         release resources
//!
//! See RFC-0002 §Execution Unit Hierarchy for the M2/M5 evolution story.

use async_trait::async_trait;
use futures::future::BoxFuture;
use pylon_types::RecordBatch;
use pylon_types::Result;

#[async_trait]
pub trait PipelineOp: Send + Sync {
    fn name(&self) -> &'static str;

    /// Source ops: false (no upstream).
    /// Stateful ops: true if room to consume more.
    /// Default: true (assume room).
    async fn needs_input(&self) -> bool {
        true
    }

    async fn add_input(&mut self, _batch: RecordBatch) -> Result<()> {
        unimplemented!("source ops don't have an upstream; sink ops should override")
    }

    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }

    async fn no_more_input(&mut self) -> Result<()> {
        Ok(())
    }

    async fn is_finished(&self) -> bool {
        false
    }

    /// Trino/Velox style: if this op is currently blocked on external I/O,
    /// return `Some(future)` that resolves when the op is unblocked. The
    /// single-thread driver loop will then await this future before retrying.
    ///
    /// M2 default impl: `None` (assume never blocked). Op implementations
    /// override when they have async I/O beyond the standard mpsc channels.
    async fn is_blocked(&self) -> Result<Option<BoxFuture<'static, ()>>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
