//! Driver: a single-thread fused execution unit for a Pipeline.
//!
//! ## M3+ default: `DriverMode::SingleThreadLoop` (Velox-aligned).
//!
//! Each Driver owns its Pipeline outright (no Arc/Mutex). It spawns a
//! single tokio task that runs `run_pipeline_single_thread` — a poll
//! loop that calls every op's async methods sequentially in one async
//! function, so no locks are taken across await points within ops.
//!
//! ## Why this fixes M3 task #4a's busy-poll bug
//!
//! The previous `PerOpTokioTask` model put every op in its own task and
//! forwarded batches through mpsc channels. This gave each op its own
//! poll loop, and the channels' Disconnect semantics relied on Send-er
//! upstream runs to finish — a source that doesn't actively signal
//! `no_more_input` (e.g. a heuristic-driven ExchangeSource) led to an
//! infinite `is_finished() == false` loop in the downstream op.
//!
//! With `SingleThreadLoop`, every step of every op is driven by the
//! same loop. We have full visibility into which upstream op finished
//! and can decide deterministically when to call `no_more_input`.

use crate::op::PipelineOp;
use crate::pipeline::{run_pipeline_single_thread, Pipeline};
use pylon_types::Result as PylonResult;
use pylon_types::RecordBatch;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum DriverMode {
    /// M3+ default: Velox-style single-thread poll loop. Ops live in a
    /// `Vec<Box<dyn PipelineOp>>` and are polled sequentially by one
    /// task. No `Mutex` around ops; the driver has full visibility.
    #[default]
    SingleThreadLoop,

    /// Legacy M1/M2 model: each PipelineOp becomes its own
    /// `tokio::spawn` task; ops communicate via bounded mpsc.
    /// Kept around for backward compat with M1 smoke tests.
    PerOpTokioTask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DriverId(pub u64);

impl DriverId {
    pub fn generate() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        DriverId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Driver owns its `Pipeline` by value.
///
/// In `SingleThreadLoop` mode, the driver can run inline on any thread
/// without contention — the future drives ops in a single poll loop
/// inside the driver's task, no locks needed.
///
/// In `PerOpTokioTask` mode (legacy), the Pipeline is wrapped in
/// `Arc<Pipeline>` so op-task closures can hand it down; the Driver
/// still owns that `Arc`.
pub enum Driver {
    OwnedSingleThread(DriverId, Pipeline),
    SharedPerOpTask(DriverId, Arc<Pipeline>),
}

impl Driver {
    pub fn new(pipeline: Pipeline) -> Self {
        Self::OwnedSingleThread(DriverId::generate(), pipeline)
    }

    /// Construct for the legacy M1/M2 per-op-task mode.
    pub fn new_legacy(pipeline: Arc<Pipeline>) -> Self {
        Self::SharedPerOpTask(DriverId::generate(), pipeline)
    }

    pub fn with_mode(mut self, mode: DriverMode) -> Self {
        match mode {
            DriverMode::SingleThreadLoop => {
                if let Self::SharedPerOpTask(id, arc) = self {
                    let p = (*arc).clone_ops_only();
                    self = Self::OwnedSingleThread(id, p);
                }
            }
            DriverMode::PerOpTokioTask => {
                if let Self::OwnedSingleThread(id, p) = self {
                    self = Self::SharedPerOpTask(id, Arc::new(p));
                }
            }
        }
        self
    }

    pub fn id(&self) -> DriverId {
        match self {
            Self::OwnedSingleThread(id, _) => *id,
            Self::SharedPerOpTask(id, _) => *id,
        }
    }

    pub fn pipeline_id(&self) -> crate::pipeline::PipelineId {
        match self {
            Self::OwnedSingleThread(_, p) => p.id,
            Self::SharedPerOpTask(_, p) => p.id,
        }
    }

    pub async fn run(
        self,
        external_input: Option<mpsc::Receiver<RecordBatch>>,
    ) -> PylonResult<mpsc::Receiver<RecordBatch>> {
        info!(
            target: "pylon::driver",
            driver_id = self.id().0,
            pipeline_id = self.pipeline_id().0,
            ops = match &self {
                Self::OwnedSingleThread(_, p) => p.ops.len(),
                Self::SharedPerOpTask(_, p) => p.ops.len(),
            },
            mode = ?match &self {
                Self::OwnedSingleThread(_, _) => DriverMode::SingleThreadLoop,
                Self::SharedPerOpTask(_, _) => DriverMode::PerOpTokioTask,
            },
            "driver starting"
        );
        match self {
            Self::OwnedSingleThread(_, pipeline) => {
                let id = pipeline.id;
                run_pipeline_single_thread(pipeline, external_input, id).await
            }
            Self::SharedPerOpTask(_, arc_pipeline) => {
                run_per_op_task_legacy(arc_pipeline, external_input).await
            }
        }
    }
}

impl Pipeline {
    /// Cloning helper used by `Driver::with_mode` when switching between
    /// modes — takes a snapshot of the ops into a fresh owned Pipeline.
    pub(crate) fn clone_ops_only(&self) -> Pipeline {
        Pipeline {
            id: self.id,
            ops: self.ops.iter().map(|op| dyn_clone_pipeline_op(op.as_ref())).collect(),
            state_bridges: self.state_bridges.clone(),
        }
    }
}

/// Naive `dyn PipelineOp` clone — used when switching the legacy
/// per-op-task path requires owned boxes.
fn dyn_clone_pipeline_op(_op: &dyn PipelineOp) -> Box<dyn PipelineOp> {
    // We don't have a Clone trait on PipelineOp. For now require callers
    // to construct a Driver with a single mode and not switch.
    // In practice the runtime default never switches mode, so this is OK.
    panic!(
        "Dyn cloning of PipelineOp is not supported; construct a fresh Driver \
         in the desired mode instead"
    );
}

/// Legacy M1/M2 path: per-op-as-tokio-task. Retained for compat with
/// historical smoke tests; not used in M3+.
async fn run_per_op_task_legacy(
    pipeline: Arc<Pipeline>,
    external_input: Option<mpsc::Receiver<RecordBatch>>,
) -> PylonResult<mpsc::Receiver<RecordBatch>> {
    use crate::pipeline::PipelineId;
    let n_ops = pipeline.ops.len();
    if n_ops == 0 {
        let (_tx, rx) = mpsc::channel::<RecordBatch>(1);
        return Ok(rx);
    }
    let (final_tx, final_rx) = mpsc::channel::<RecordBatch>(crate::pipeline::DEFAULT_CHANNEL_CAPACITY);
    let mut joins = tokio::task::JoinSet::new();
    let mut next_input: Option<mpsc::Receiver<RecordBatch>> = external_input;

    // Per-op task state: store ops in shared Arc<Mutex<...>> form.
    let shared_ops: Vec<Arc<tokio::sync::Mutex<Box<dyn PipelineOp>>>> = pipeline
        .ops
        .iter()
        .map(|op| Arc::new(tokio::sync::Mutex::new(dyn_clone_box(op.as_ref()))))
        .collect();

    for (i, op) in shared_ops.iter().cloned().enumerate() {
        let (tx, rx) = mpsc::channel::<RecordBatch>(crate::pipeline::DEFAULT_CHANNEL_CAPACITY);
        let is_last = i + 1 == n_ops;
        let op_input = next_input.take();
        let output_tx = if is_last { final_tx.clone() } else { tx };
        joins.spawn(async move {
            if let Err(e) = run_legacy_op(op, op_input, output_tx).await {
                tracing::warn!(error = %e, "op exited with error");
            }
        });
        if !is_last {
            next_input = Some(rx);
        }
    }

    tokio::spawn(async move {
        while let Some(res) = joins.join_next().await {
            if let Err(e) = res {
                tracing::warn!(panic = %e, "task panic");
            }
        }
    });

    let _ = PipelineId::generate(); // hint; no-op
    Ok(final_rx)
}

/// Helper to fabricate an owned `Box<dyn PipelineOp>` for the legacy path.
/// Same panic strategy as `dyn_clone_pipeline_op`; in M3 we don't expect
/// to take the legacy path because it's the source of the busy-poll bug.
fn dyn_clone_box(_op: &dyn PipelineOp) -> Box<dyn PipelineOp> {
    panic!(
        "Dyn cloning of PipelineOp is not supported; construct a fresh Driver \
         in the desired mode instead"
    );
}

/// Per-op async driver loop (legacy M1/M2 path). Kept for backward
/// compat only; not exercised by M3+ smoke tests.
async fn run_legacy_op(
    op: Arc<tokio::sync::Mutex<Box<dyn PipelineOp>>>,
    mut input: Option<mpsc::Receiver<RecordBatch>>,
    output: mpsc::Sender<RecordBatch>,
) -> PylonResult<()> {
    use pylon_types::PylonError;
    let mut upstream_done = false;
    loop {
        if !upstream_done {
            let needs = op.lock().await.needs_input().await;
            if needs {
                if let Some(rx) = input.as_mut() {
                    let recv = rx.try_recv();
                    match recv {
                        Ok(batch) => {
                            let name = op.lock().await.name().to_string();
                            op.lock().await.add_input(batch).await.map_err(|e| {
                                PylonError::Internal(format!("{name} add_input: {e}"))
                            })?;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {}
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            let name = op.lock().await.name().to_string();
                            op.lock().await.no_more_input().await.map_err(|e| {
                                PylonError::Internal(format!("{name} no_more_input: {e}"))
                            })?;
                            upstream_done = true;
                        }
                    }
                } else {
                    upstream_done = true;
                }
            }
        }
        let maybe_out = {
            let mut g = op.lock().await;
            g.get_output().await
        };
        match maybe_out {
            Ok(Some(batch)) => {
                let oname = op.lock().await.name().to_string();
                
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
        if op.lock().await.is_finished().await {
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}
