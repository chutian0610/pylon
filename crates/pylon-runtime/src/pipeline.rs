//! Pipeline: chain of operators + shared state bridges (Trino-aligned).
//!
//! Velox-style single-thread fused driver: ops are owned
//! `Vec<Box<dyn PipelineOp>>` (no Mutex). The driver polls them sequentially
//! in one async task — ops do NOT take locks across awaits; instead the
//! driver awaits the op's async method directly, no contention.

use crate::bridge::StateBridge;
use crate::op::PipelineOp;
use pylon_types::RecordBatch;
use pylon_types::Result as PylonResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, info};

pub const DEFAULT_CHANNEL_CAPACITY: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PipelineId(pub u64);

impl PipelineId {
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        PipelineId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Owned pipeline: ops are `Box<dyn PipelineOp>` and live directly on the
/// driver thread, no Mutex. Shared state bridges are referenced by `Arc`
/// since multiple drivers may need to read them.
pub struct Pipeline {
    pub id: PipelineId,
    pub ops: Vec<Box<dyn PipelineOp>>,
    pub state_bridges: Vec<Arc<dyn StateBridge>>,
}

impl Pipeline {
    pub fn new(ops: Vec<Box<dyn PipelineOp>>) -> Self {
        Self {
            id: PipelineId::generate(),
            ops,
            state_bridges: Vec::new(),
        }
    }

    pub fn with_bridge(mut self, b: Arc<dyn StateBridge>) -> Self {
        self.state_bridges.push(b);
        self
    }

    pub fn op_count(&self) -> usize {
        self.ops.len()
    }
}

// DriverId is in driver.rs to keep this file focused on Pipeline data.

/// Run the pipeline as a single tokio task running a Velox-style
/// single-thread poll loop. M3+ default.
///
/// The pipeline must be owned outright: ops are mutably called in
/// sequence, no lock needed because no two ops run concurrently.
///
/// Loop structure (per iteration):
///   1. Feed external_input → op[0].input_buf
///   2. Cascade op[i].output_buf → op[i+1].input_buf for each i
///   3. Drive each op (in order):
///      3a. if upstream_done path is satisfied → call op.no_more_input()
///      3b. drain op.input_buf into op.add_input
///      3c. drain op.get_output into op.output_buf OR final_tx (terminal op)
///      3d. check is_finished → mark op_states[i].finished = true
///   4. If all ops finished → break
///   5. Collect is_blocked futures across ops; if any, await the first
///   6. Yield to scheduler if no progress was made
///
/// External input is consumed only by op[0]. The driver logically closes
/// external_input when its mpsc Receiver reports `is_closed` (drop of producer).
pub async fn run_pipeline_single_thread(
    pipeline: Pipeline,
    external_input: Option<mpsc::Receiver<RecordBatch>>,
    pipeline_id: PipelineId,
) -> PylonResult<mpsc::Receiver<RecordBatch>> {
    let n_ops = pipeline.ops.len();
    if n_ops == 0 {
        let (_tx, rx) = mpsc::channel::<RecordBatch>(1);
        return Ok(rx);
    }
    let (final_tx, final_rx) = mpsc::channel::<RecordBatch>(DEFAULT_CHANNEL_CAPACITY);
    let is_source_stage = external_input.is_none();
    let mut external_input = external_input;

    let Pipeline { ops, .. } = pipeline;

    // Per-op bookkeeping. Lives only inside this function (single-thread,
    // so no Mutex needed; we mutate freely).
    struct OpState {
        input_buf: Vec<RecordBatch>,
        output_buf: Vec<RecordBatch>, // only used for non-terminal ops
        upstream_notified: bool,
        finished: bool,
    }
    let mut op_states: Vec<OpState> = (0..n_ops)
        .map(|_| OpState {
            input_buf: Vec::new(),
            output_buf: Vec::new(),
            upstream_notified: false,
            finished: false,
        })
        .collect();
    let mut ops: Vec<Box<dyn PipelineOp>> = ops;

    info!(
        target: "pylon::pipeline",
        pipeline_id = pipeline_id.0,
        ops = n_ops,
        mode = "SingleThreadLoop",
        is_source_stage,
        "single-thread pipeline started"
    );

    loop {
        let mut progressed = false;

        // ----- Step 1: external input → op[0].input_buf -----
        let mut external_closed = false;
        if let Some(rx) = external_input.as_mut() {
            while let Ok(batch) = rx.try_recv() {
                op_states[0].input_buf.push(batch);
                progressed = true;
            }
            if rx.is_closed() {
                external_closed = true;
            }
        }
        if external_closed {
            external_input = None;
        }

        // ----- Step 2: inter-op cascade -----
        //
        // We now cascade as part of step 3 (per-op, after op[i] emits), so this
        // step is just a fallback for batches that arrived from a previous iter
        // and were never delivered (e.g. ops that finished but their output was
        // never cascaded). For correctness we still run it once per iter, but
        // it should be a no-op when step 3 has done its work.
        if n_ops > 1 {
            for i in 0..n_ops.saturating_sub(1) {
                let drain = op_states[i].output_buf.len();
                if drain > 0 {
                    let drained: Vec<RecordBatch> = std::mem::take(&mut op_states[i].output_buf);
                    op_states[i + 1].input_buf.extend(drained);
                    progressed = true;
                }
            }
        }

        // ----- Step 3: drive each op in sequence -----
        for i in 0..n_ops {
            // Pre-compute the op's upstream readiness info before mutating,
            // because we'll mut-borrow op_states[i] below.
            let upstream_ready = if i == 0 {
                external_input.is_none()
            } else {
                // op_states[i-1].finished checked via separate borrow
                op_states[i - 1].finished
            };

            // No `let st = ...` here — direct op_states[i] access for 3a, then
            // release borrow before 3c.
            if op_states[i].finished {
                continue;
            }
            let op = &mut ops[i];

            // 3a. drain input buffer FIRST (cascade in step 2 just populated it)
            let mut fed = 0usize;
            while let Some(batch) = op_states[i].input_buf.pop() {
                op.add_input(batch).await?;
                fed += 1;
            }
            if fed > 0 {
                progressed = true;
            }

            // 3b. no_more_input signal — only AFTER we've drained input.
            if !op_states[i].upstream_notified {
                let ready = if i == 0 {
                    external_input.is_none()
                } else {
                    upstream_ready
                };
                if ready {
                    op.no_more_input().await?;
                    op_states[i].upstream_notified = true;
                    progressed = true;
                }
            }

            // 3c. drain output
            let is_last = i + 1 == n_ops;
            loop {
                let next_out = op.get_output().await?;
                match next_out {
                    Some(batch) => {
                        if is_last {
                            // terminal op: deliver to final_tx
                            if final_tx.send(batch).await.is_err() {
                                // downstream consumer dropped
                                debug!(
                                    target: "pylon::pipeline",
                                    "final_tx closed; aborting driver"
                                );
                                return Ok(final_rx);
                            }
                        } else {
                            op_states[i + 1].input_buf.push(batch);
                        }
                        progressed = true;
                    }
                    None => break,
                }
            }

            // 3d. finished?
            if op.is_finished().await {
                op_states[i].finished = true;
                info!(
                    target: "pylon::pipeline",
                    pipeline_id = pipeline_id.0,
                    op_index = i,
                    name = %op.name(),
                    "op finished"
                );
            }
        }

        // ----- Step 4: pipeline finished? -----
        if op_states.iter().all(|s| s.finished) {
            break;
        }

        // ----- Step 5: blocked futures -----
        let mut blocked: Vec<BoxFuture<'static, ()>> = Vec::new();
        for (i, op) in ops.iter_mut().enumerate() {
            if op_states[i].finished {
                continue;
            }
            if let Some(fut) = op.is_blocked().await? {
                blocked.push(fut);
            }
        }
        if !blocked.is_empty() {
            // Wait for any one to resolve. select_all picks the fastest.
            let _ = futures::future::select_all(blocked).await;
            debug!(
                target: "pylon::pipeline",
                pipeline_id = pipeline_id.0,
                "a blocked future resolved; retrying ops"
            );
            // We made progress (an op just became unblocked);
            // skip the cooperative yield below and let the next
            // tick drain the now-cascadable batches.
            continue;
        }

        // ----- Step 6: no progress → cooperative yield -----
        if !progressed {
            tokio::task::yield_now().await;
        }
    }

    info!(
        target: "pylon::pipeline",
        pipeline_id = pipeline_id.0,
        "single-thread pipeline done"
    );
    Ok(final_rx)
}

use futures::future::BoxFuture;
