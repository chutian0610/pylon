//! Pipeline: a chain of operators with shared state — Trino alignment.

use crate::bridge::StateBridge;
use crate::op::PipelineOp;
use pylon_types::Result as PylonResult;
use pylon_types::{PylonError, RecordBatch};
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
    pub fn with_bridge(mut self, b: Arc<dyn StateBridge>) -> Self {
        self.state_bridges.push(b); self
    }
    pub fn op_count(&self) -> usize { self.ops.len() }
}

pub async fn run_pipeline_per_op_task(
    pipeline: Arc<Pipeline>,
    input: Option<mpsc::Receiver<RecordBatch>>,
) -> PylonResult<mpsc::Receiver<RecordBatch>> {
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
            if let Err(e) = res { warn!(target: "pylon::pipeline", panic = %e, "task panic"); }
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
    mut input: Option<mpsc::Receiver<RecordBatch>>,
    output: mpsc::Sender<RecordBatch>,
) -> PylonResult<()> {
    let mut upstream_done = false;

    loop {
        // Phase A: feed input (lock→unlock with no holds across await)
        if !upstream_done {
            let needs = {
                let g = op.lock().await;
                g.needs_input().await
            };
            if needs {
                if let Some(rx) = input.as_mut() {
                    let recv = rx.try_recv();
                    match recv {
                        Ok(batch) => {
                            let name = op.lock().await.name().to_string();
                            op.lock().await.add_input(batch).await
                                .map_err(|e| PylonError::Internal(format!("{name} add_input: {e}")))?;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {}
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            let name = op.lock().await.name().to_string();
                            op.lock().await.no_more_input().await
                                .map_err(|e| PylonError::Internal(format!("{name} no_more_input: {e}")))?;
                            upstream_done = true;
                        }
                    }
                } else {
                    upstream_done = true;
                }
            }
        }

        // Phase B: drain output
        let maybe_out = {
            let mut g = op.lock().await;
            g.get_output().await
        };
        match maybe_out {
            Ok(Some(batch)) => {
                if output.send(batch).await.is_err() {
                    return Ok(());
                }
                continue;
            }
            Ok(None) => {}
            Err(e) => {
                let name = op.lock().await.name().to_string();
                return Err(PylonError::Internal(format!("{name} get_output: {e}")).into());
            }
        }

        // Phase C: finished?
        {
            let g = op.lock().await;
            if g.is_finished().await { return Ok(()); }
        }

        tokio::task::yield_now().await;
    }
}
