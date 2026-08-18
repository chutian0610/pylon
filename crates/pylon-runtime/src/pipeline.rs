//! Pipeline: a chain of operators with shared state — Trino alignment.
//!
//! In our model:
//! - Pipeline owns the operator chain (shared across drivers)
//! - Pipeline holds StateBridges (e.g. HashJoinBridge for shared build state)
//! - M Drivers process the pipeline concurrently (M=1 in M2 default)
//!
//! Pipeline data lives on the worker (coordinator never owns an actual Pipeline).
//! Coordinator owns `Fragment` (the spec) and constructs `TaskSpec` from it.

use crate::bridge::StateBridge;
use crate::op::PipelineOp;
use pylon_types::Result as PylonResult;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

pub const DEFAULT_CHANNEL_CAPACITY: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PipelineId(pub u64);

impl PipelineId {
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        PipelineId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

pub struct Pipeline {
    pub id: PipelineId,
    pub ops: Vec<Arc<Mutex<Box<dyn PipelineOp>>>>,
    pub state_bridges: Vec<Arc<dyn StateBridge>>,
}

impl Pipeline {
    pub fn new(ops: Vec<Box<dyn PipelineOp>>) -> Self {
        Self {
            id: PipelineId::generate(),
            ops: ops.into_iter().map(|op| Arc::new(Mutex::new(op))).collect(),
            state_bridges: Vec::new(),
        }
    }

    pub fn with_bridge(mut self, bridge: Arc<dyn StateBridge>) -> Self {
        self.state_bridges.push(bridge);
        self
    }

    pub fn op_count(&self) -> usize {
        self.ops.len()
    }
}

/// Default M2 driver mode: each op is a separate tokio task.
/// N ops = N concurrent tasks per pipeline. Data flows through mpsc channels.
///
/// This is functionally equivalent to the M1 driver, but the abstraction
/// (Pipeline) is now Trino-aligned: Pipeline=op chain, Driver=runs the chain.
pub async fn run_pipeline_per_op_task(
    pipeline: Arc<Pipeline>,
    input: Option<mpsc::Receiver<pylon_types::RecordBatch>>,
) -> PylonResult<mpsc::Receiver<pylon_types::RecordBatch>> {
    use pylon_types::RecordBatch;

    let n_ops = pipeline.ops.len();
    if n_ops == 0 {
        let (_tx, rx) = mpsc::channel::<RecordBatch>(1);
        return Ok(rx);
    }
    let (final_tx, final_rx) = mpsc::channel::<RecordBatch>(DEFAULT_CHANNEL_CAPACITY);

    let mut joins: JoinSet<()> = JoinSet::new();
    let mut next_input: Option<mpsc::Receiver<RecordBatch>> = input;

    for (i, op) in pipeline.ops.iter().cloned().enumerate() {
        let (tx, rx) = mpsc::channel::<RecordBatch>(DEFAULT_CHANNEL_CAPACITY);
        let is_last = i + 1 == n_ops;
        let op_input = next_input.take();
        let output_tx = if is_last { final_tx.clone() } else { tx };

        joins.spawn(async move {
            if let Err(e) = run_op(op, op_input, output_tx).await {
                warn!(target: "pylon::pipeline", error = %e, "op exited with error");
            }
        });

        if !is_last {
            next_input = Some(rx);
        }
    }

    tokio::spawn(async move {
        while let Some(res) = joins.join_next().await {
            if let Err(e) = res {
                warn!(target: "pylon::pipeline", error = %e, "op task panicked");
            }
        }
        debug!(target: "pylon::pipeline", "all ops exited");
    });

    info!(
        target: "pylon::pipeline",
        pipeline_id = pipeline.id.0,
        ops = n_ops,
        mode = "PerOpTokioTask",
        "pipeline started"
    );
    Ok(final_rx)
}

async fn run_op(
    op: Arc<Mutex<Box<dyn PipelineOp>>>,
    mut input: Option<mpsc::Receiver<pylon_types::RecordBatch>>,
    output: mpsc::Sender<pylon_types::RecordBatch>,
) -> PylonResult<()> {
    use pylon_types::RecordBatch;

    let mut upstream_done = false;
    loop {
        // Step 1: feed input if available and op needs it
        if !upstream_done {
            let needs = {
                let g = op.lock().await;
                g.needs_input().await
            };
            if needs {
                if let Some(rx) = input.as_mut() {
                    match rx.try_recv() {
                        Ok(batch) => {
                            let mut g = op.lock().await;
                            g.add_input(batch).await?;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {}
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            let mut g = op.lock().await;
                            g.no_more_input().await?;
                            upstream_done = true;
                        }
                    }
                } else {
                    upstream_done = true; // source op
                }
            }
        }

        // Step 2: drain op's output
        {
            let mut g = op.lock().await;
            if let Some(batch) = g.get_output().await? {
                drop(g);
                if output.send(batch).await.is_err() {
                    return Ok(()); // downstream closed
                }
                continue;
            }
        }

        // Step 3: finished?
        {
            let g = op.lock().await;
            if g.is_finished().await {
                return Ok(());
            }
        }

        // Step 4: blocked? (Trino-style is_blocked)
        // For M2 default mode, just yield; real driver would poll a future.
        {
            let g = op.lock().await;
            if g.is_blocked().await?.is_some() {
                drop(g);
                tokio::task::yield_now().await;
                continue;
            }
        }

        // Step 5: idle yield
        tokio::task::yield_now().await;
    }
}

