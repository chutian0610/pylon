//! Morsel-driven push 执行器（ADR-0002 / ADR-0006）。
//!
//! [`PipelineDriver`] 驱动一条 [`Pipeline`]：
//!
//! ```text
//!  tokio IO 任务（每个 ScanSplit 一个）─ 打开 Morsel 流 ─▶ 有界队列 ─▶ 专用计算线程 × N
//!                                                                                │ 认领 Morsel
//!                                                                                ▼
//!                                                                    ops（Project …）→ Sink（LIMIT）
//! ```
//!
//! - IO 侧：tokio 任务打开 TableFormat 给出的 [`ScanSplit`](pylon_table::ScanSplit) 并
//!   预取 Morsel；队列满时在 `send().await` 处让出，背压由此传导到 IO 侧。
//! - 有界队列：IO → 计算的唯一边界（ADR-0006 的核心契约）。
//! - 计算侧：固定数量的专用 std 线程从队列认领 Morsel，沿算子序列推送到 Sink；算子内无 await。
//! - Sink：收集结果并执行 LIMIT；达到上限后置停止标志，IO 与计算两侧提前终止。
//! - Morsel 目标大小为行数与字节双目标（`morsel_rows` / `morsel_bytes`），超目标的
//!   Morsel 会被切成多个再入队。

mod pipeline;
mod source;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use arrow::array::RecordBatch;
use async_channel::Receiver;
use pylon_plan::{Operator, Pipeline};

use pipeline::Sink;

/// 一条 Pipeline 的驱动器：持有并行度与 Morsel 目标等执行配置。
#[derive(Debug, Clone)]
pub struct PipelineDriver {
    /// 专用计算线程数，默认 = 可用并行度。
    pub compute_threads: usize,
    /// tokio IO 线程数，默认 = min(可用并行度, 4)。
    pub io_threads: usize,
    /// Morsel 目标行数。
    pub morsel_rows: usize,
    /// Morsel 目标字节数（估算值，超出则按行切片）。
    pub morsel_bytes: usize,
    /// IO → 计算 有界队列容量（以 Morsel 计）。
    pub queue_depth: usize,
}

impl Default for PipelineDriver {
    fn default() -> Self {
        let cores = thread::available_parallelism().map_or(1, |n| n.get());
        Self {
            compute_threads: cores,
            io_threads: cores.clamp(1, 4),
            morsel_rows: 8192,
            morsel_bytes: 4 << 20,
            queue_depth: cores * 2,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("failed to read {split}: {source}")]
    Scan {
        split: String,
        #[source]
        source: pylon_table::TableError,
    },
    #[error("operator failed: {0}")]
    Operator(String),
    #[error("internal executor error: {0}")]
    Internal(String),
}

/// 首个错误的落点；任一侧出错即置 `stop`，两侧据此退出。
type Failure = Arc<Mutex<Option<ExecError>>>;

fn record_failure(failure: &Failure, stop: &AtomicBool, err: ExecError) {
    failure
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get_or_insert(err);
    stop.store(true, Ordering::Release);
}

impl PipelineDriver {
    /// 同步驱动一条 Pipeline，返回结果 Morsel 列表（顺序不保证）。
    ///
    /// 内部自建 tokio runtime 承担 IO，并派生专用计算线程；调用方无需在异步上下文中。
    pub fn run(&self, pipeline: &Pipeline) -> Result<Vec<RecordBatch>, ExecError> {
        if pipeline.limit == Some(0) || pipeline.scan.splits.is_empty() {
            return Ok(Vec::new());
        }

        let stop = Arc::new(AtomicBool::new(false));
        let failure: Failure = Arc::default();
        let (tx, rx) = async_channel::bounded::<RecordBatch>(self.queue_depth.max(1));

        // ---- IO 侧：每个扫描单元一个 tokio 任务 ----
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.io_threads.max(1))
            .thread_name("pylon-io")
            .enable_all()
            .build()
            .map_err(|e| ExecError::Internal(format!("failed to start IO runtime: {e}")))?;

        let io_tasks: Vec<_> = pipeline
            .scan
            .splits
            .iter()
            .map(|split| {
                let tx = tx.clone();
                let stop = stop.clone();
                let failure = failure.clone();
                let split_desc = split.describe();
                let morsel = source::MorselConfig {
                    rows: self.morsel_rows,
                    bytes: self.morsel_bytes,
                };
                let open = split.open(self.morsel_rows);
                runtime.spawn(async move {
                    if let Err(err) = source::pump(open, morsel, &tx, &stop).await {
                        record_failure(
                            &failure,
                            &stop,
                            ExecError::Scan {
                                split: split_desc,
                                source: err,
                            },
                        );
                    }
                })
            })
            .collect();
        // 发送端只由 IO 任务持有：全部结束后队列关闭，计算线程据此自然退出
        drop(tx);

        // ---- 计算侧：固定数量的专用线程 ----
        let ops: Arc<[Operator]> = pipeline.ops.clone().into();
        let sink = Arc::new(Sink::new(pipeline.limit));
        let mut workers = Vec::with_capacity(self.compute_threads.max(1));
        for i in 0..self.compute_threads.max(1) {
            let worker = {
                let rx = rx.clone();
                let ops = ops.clone();
                let sink = sink.clone();
                let stop = stop.clone();
                let failure = failure.clone();
                move || compute_loop(rx, &ops, &sink, &stop, &failure)
            };
            match thread::Builder::new()
                .name(format!("pylon-compute-{i}"))
                .spawn(worker)
            {
                Ok(handle) => workers.push(handle),
                Err(e) => {
                    record_failure(
                        &failure,
                        &stop,
                        ExecError::Internal(format!("failed to spawn compute thread: {e}")),
                    );
                    break;
                }
            }
        }
        drop(rx);

        for worker in workers {
            if worker.join().is_err() {
                record_failure(
                    &failure,
                    &stop,
                    ExecError::Internal("compute thread panicked".into()),
                );
            }
        }
        // 计算线程全部退出 ⇒ 所有接收端已释放 ⇒ 阻塞在 send 的 IO 任务立即失败并返回
        runtime.block_on(async {
            for task in io_tasks {
                if task.await.is_err() {
                    record_failure(
                        &failure,
                        &stop,
                        ExecError::Internal("IO task panicked".into()),
                    );
                }
            }
        });
        drop(runtime);

        if let Some(err) = failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            return Err(err);
        }
        Ok(sink.take_morsels())
    }
}

/// 计算线程主循环：认领 Morsel → 依次过算子 → 推入 Sink。全程同步，无 await。
fn compute_loop(
    rx: Receiver<RecordBatch>,
    ops: &[Operator],
    sink: &Sink,
    stop: &AtomicBool,
    failure: &Failure,
) {
    while !stop.load(Ordering::Acquire) {
        let Ok(mut morsel) = rx.recv_blocking() else {
            break;
        };
        for op in ops {
            match pipeline::eval(op, &morsel) {
                Ok(next) => morsel = next,
                Err(e) => {
                    record_failure(failure, stop, e);
                    return;
                }
            }
        }
        if sink.push(morsel) {
            stop.store(true, Ordering::Release);
            return;
        }
    }
}
