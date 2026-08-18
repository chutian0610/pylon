//! `pylon-worker` — connects to a coordinator over gRPC and runs TaskSpec instances.

use anyhow::{Context, Result};
use futures::StreamExt;
use pylon_proto::pylon::{TaskRequest, TaskResponse, TaskState};
use pylon_proto::worker_client::WorkerClient;
use pylon_exchange::{FlightDescriptor, PylonFlightService};
use pylon_runtime::ops::{
    ExchangeSinkOp, ExchangeSourceOp, FilterOp, PartitionFilterOp, ProjectOp, SeqScanOp,
};
use pylon_runtime::{Driver, DriverMode, Pipeline, PipelineOp};
use std::sync::Arc;
use tracing::{info, warn};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    init_tracing();

    // Single in-process Flight service shared by all tasks in this worker.
    // M3 first cut: every task's ExchangeSink/Source wires into this. Real
    // peer-to-peer Flight RPC belongs to M3 task #4.
    let flight_service = Arc::new(PylonFlightService::new());

    run(flight_service).await
}

async fn run(flight_service: Arc<PylonFlightService>) -> Result<()> {
    let coord_addr = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("PYLON_COORDINATOR").ok())
        .unwrap_or_else(|| "http://127.0.0.1:9090".to_string());

    info!("pylon-worker connecting to coord at {coord_addr}");
    let mut client = WorkerClient::connect(coord_addr.clone())
        .await
        .with_context(|| format!("connect to coord {coord_addr}"))?;

    let (out_tx, out_rx) = tokio::sync::mpsc::channel::<TaskResponse>(32);
    let out_stream = tokio_stream::wrappers::ReceiverStream::new(out_rx);

    let response = client
        .open_session(out_stream)
        .await
        .with_context(|| "open_session")?;
    let mut incoming = response.into_inner();

    info!("session opened; awaiting TaskRequest stream");

    while let Some(task_req_msg) = incoming.next().await {
        let task_req_msg = task_req_msg.with_context(|| "decode TaskRequest")?;
        let task_id = task_req_msg.spec.as_ref().map(|s| s.id).unwrap_or(0);
        info!(task_id, "got task request");
        match run_task(task_req_msg, flight_service.clone()).await {
            Ok(batches) => {
                eprintln!("[W1] matching Ok with {} batches", batches.len());
                let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                let mut emitted = 0u64;
                for batch in batches {
                    let bytes = encode_batch_ipc(&batch).unwrap_or_default();
                    let resp = TaskResponse {
                        task_id,
                        state: TaskState::TaskRunning as i32,
                        rows_emitted: batch.num_rows() as u64,
                        batch: bytes,
                        message: String::new(),
                    };
                    eprintln!("[W1] sending TaskResponse task_id={} state_running rows={}", task_id, batch.num_rows());
                    if out_tx.send(resp).await.is_err() {
                        eprintln!("[W1] coord stream closed mid-batch (task_id={task_id})");
                        warn!("coord stream closed mid-batch");
                        return Ok(());
                    }
                    emitted += batch.num_rows() as u64;
                }
                // M3: emit DONE marker so coord can advance to next stage
                let done_resp = TaskResponse {
                    task_id,
                    state: TaskState::TaskDone as i32,
                    rows_emitted: emitted,
                    batch: Vec::new(),
                    message: String::new(),
                };
                if out_tx.send(done_resp).await.is_err() {
                    return Ok(());
                }
                info!(task_id, total_rows, "task done");
            }
            Err(e) => {
                let fail = TaskResponse {
                    task_id,
                    state: TaskState::TaskFailed as i32,
                    rows_emitted: 0,
                    batch: Vec::new(),
                    message: format!("{e:?}"),
                };
                let _ = out_tx.send(fail).await;
                warn!(task_id, "task failed: {e:?}");
            }
        }
    }
    info!("coord closed incoming stream; worker exits");
    Ok(())
}

async fn run_task(req: TaskRequest, flight_service: Arc<PylonFlightService>) -> Result<Vec<arrow_array::RecordBatch>> {
    let spec = req.spec.context("task spec missing")?;
    let fragment = spec.fragment.as_ref().context("fragment missing")?;
    eprintln!("[W1] run_task ops={}", fragment.ops.iter().map(|o| o.name.as_str()).collect::<Vec<_>>().join(","));
    let ops = build_ops(fragment, flight_service.clone())?;
    eprintln!("[W1] run_task built ops, count={}", ops.len());
    let pipeline = Arc::new(Pipeline::new(ops));
    let driver = Driver::new(pipeline).with_mode(DriverMode::PerOpTokioTask);

    let mut output = driver.run(None).await?;
    let mut collected = Vec::new();
    while let Some(batch) = output.recv().await {
        eprintln!("[W1] run_task got output batch rows={}", batch.num_rows());
        collected.push(batch);
    }
    eprintln!("[W1] run_task output channel closed, collected={}", collected.iter().map(|b| b.num_rows()).sum::<usize>());
    Ok(collected)
}

fn build_ops(
    fragment: &pylon_proto::pylon::Fragment,
    flight_service: Arc<PylonFlightService>,
) -> Result<Vec<Box<dyn PipelineOp>>> {
    let mut ops: Vec<Box<dyn PipelineOp>> = Vec::new();
    for op_spec in &fragment.ops {
        ops.push(build_op(&op_spec.name, &op_spec.config, flight_service.clone())?);
    }
    Ok(ops)
}

fn build_op(
    name: &str,
    config: &std::collections::HashMap<String, String>,
    flight_service: Arc<PylonFlightService>,
) -> Result<Box<dyn PipelineOp>> {
    let get = |k: &str| -> Result<String> {
        config.get(k).cloned().ok_or_else(|| anyhow::anyhow!("op {name} missing config key {k}"))
    };
    match name {
        "SeqScan" => Ok(Box::new(SeqScanOp::new(get("path")?, 8192))),
        "Filter" => Ok(Box::new(FilterOp::new(get("col")?, get("op")?, get("literal")?))),
        "Project" => {
            let cols: Vec<String> = get("cols")?.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
            let schema = Arc::new(arrow_schema::Schema::empty());
            Ok(Box::new(ProjectOp::new(cols, schema)))
        }
        "PartitionFilter" => Ok(Box::new(PartitionFilterOp::new(get("col")?, &get("literal")?)?)),
        "ExchangeSink" => {
            let desc = FlightDescriptor(get("descriptor")?);
            Ok(Box::new(ExchangeSinkOp::new(desc, flight_service.clone())))
        }
        "ExchangeSource" => {
            let desc = FlightDescriptor(get("descriptor")?);
            Ok(Box::new(ExchangeSourceOp::new(desc, flight_service)))
        }
        other => Err(anyhow::anyhow!("unknown op: {other}")),
    }
}

fn encode_batch_ipc(_batch: &arrow_array::RecordBatch) -> Result<Vec<u8>> {
    Ok(vec![])
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("pylon=info")),
        )
        .try_init();
}
