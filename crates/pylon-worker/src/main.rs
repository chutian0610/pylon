//! `pylon-worker` — connects to a coordinator over gRPC and runs TaskSpec instances.

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::StreamExt;
use pylon_exchange::{FlightServerImpl, PylonFlightClient, PylonFlightService};
use pylon_proto::pylon::{RegisterWorkerRequest, TaskRequest, TaskResponse, TaskState};
use pylon_proto::worker_client::WorkerClient;
use pylon_runtime::{Driver, Pipeline, PipelineOp};
use std::sync::Arc;
use tonic::Request;
use tracing::{info, warn};

mod op_registry;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    init_tracing();

    // M3 B-1: parse --flight-addr (host:port the worker listens on
    // for Arrow Flight). Defaults to 127.0.0.1:0 (kernel-assigned).
    let flight_addr: String = std::env::args()
        .skip(1)
        .find(|a| a == "--flight-addr")
        .and_then(|_| {
            let pos = std::env::args()
                .position(|a| a == "--flight-addr")
                .expect("flag found");
            std::env::args().nth(pos + 1)
        })
        .or_else(|| std::env::var("PYLON_FLIGHT_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:0".to_string());
    let grpc_local_addr: String = std::env::args()
        .skip(1)
        .find(|a| a == "--grpc-addr")
        .and_then(|_| {
            let pos = std::env::args()
                .position(|a| a == "--grpc-addr")
                .expect("flag found");
            std::env::args().nth(pos + 1)
        })
        .or_else(|| std::env::var("PYLON_GRPC_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:0".to_string());

    // Single in-process Flight service shared by all tasks in this worker.
    // M3 B-1: now also served as Arrow Flight RPC server.
    let flight_service = Arc::new(PylonFlightService::new());

    // M3 B-1: resolve the Flight port. We bind to it ourselves so
    // we can read back the kernel-assigned port (when --flight-addr
    // is "127.0.0.1:0"). The actual tonic server runs in a
    // background task.
    let flight_listen = flight_addr
        .parse::<std::net::SocketAddr>()
        .with_context(|| format!("parse flight addr {flight_addr}"))?;
    let flight_listener = tokio::net::TcpListener::bind(flight_listen)
        .await
        .with_context(|| format!("bind flight addr {flight_listen}"))?;
    let bound_flight_addr = flight_listener
        .local_addr()
        .context("flight listener local_addr")?;
    info!(
        requested = %flight_listen,
        bound = %bound_flight_addr,
        "pylon-worker Flight listener bound"
    );
    let flight_server = FlightServerImpl::new(flight_service.clone());
    let incoming_flight = tokio_stream::wrappers::TcpListenerStream::new(flight_listener);
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(flight_server))
            .serve_with_incoming(incoming_flight)
            .await
        {
            warn!("flight server exited: {e}");
        }
    });

    run(
        flight_service,
        bound_flight_addr.to_string(),
        grpc_local_addr,
    )
    .await
}

async fn run(
    flight_service: Arc<PylonFlightService>,
    flight_addr: String,
    grpc_addr: String,
) -> Result<()> {
    let coord_addr = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("PYLON_COORDINATOR").ok())
        .unwrap_or_else(|| "http://127.0.0.1:9090".to_string());

    info!("pylon-worker connecting to coord at {coord_addr}");
    let mut client = WorkerClient::connect(coord_addr.clone())
        .await
        .with_context(|| format!("connect to coord {coord_addr}"))?;

    // M3 B-1: register worker with coord → get back worker_id.
    let reg = client
        .register_worker(RegisterWorkerRequest {
            flight_addr: flight_addr.clone(),
            grpc_addr: grpc_addr.clone(),
        })
        .await
        .with_context(|| "register_worker")?
        .into_inner();
    let worker_id = reg.worker_id;
    info!(
        worker_id,
        flight_addr = %flight_addr,
        grpc_addr = %grpc_addr,
        "registered with coord"
    );

    let (out_tx, out_rx) = tokio::sync::mpsc::channel::<TaskResponse>(32);
    let out_stream = tokio_stream::wrappers::ReceiverStream::new(out_rx);

    // M3 B-1: pass worker_id as metadata so coord can pair this
    // session with the prior RegisterWorker (and look up flight_addr).
    let mut req = Request::new(out_stream);
    req.metadata_mut().insert(
        "x-pylon-worker-id",
        worker_id.to_string().parse().expect("ascii worker_id"),
    );
    let response = client
        .open_session(req)
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
                        spill_handle: String::new(),
                    };
                    if out_tx.send(resp).await.is_err() {
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
                    spill_handle: String::new(),
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
                    spill_handle: String::new(),
                };
                let _ = out_tx.send(fail).await;
                warn!(task_id, "task failed: {e:?}");
            }
        }
    }
    info!("coord closed incoming stream; worker exits");
    Ok(())
}

async fn run_task(
    req: TaskRequest,
    flight_service: Arc<PylonFlightService>,
) -> Result<Vec<arrow_array::RecordBatch>> {
    let spec = req.spec.context("task spec missing")?;
    let fragment = spec.fragment.as_ref().context("fragment missing")?;

    let ops = build_ops(fragment, flight_service.clone())?;
    let pipeline = Pipeline::new(ops);
    let driver = Driver::new(pipeline); // default mode = SingleThreadLoop

    let mut output = driver.run(None).await?;
    let mut collected = Vec::new();
    while let Some(batch) = output.recv().await {
        collected.push(batch);
    }
    Ok(collected)
}

fn build_ops(
    fragment: &pylon_proto::pylon::Fragment,
    flight_service: Arc<PylonFlightService>,
) -> Result<Vec<Box<dyn PipelineOp>>> {
    let mut ops: Vec<Box<dyn PipelineOp>> = Vec::new();
    for op_spec in &fragment.ops {
        ops.push(build_op(
            &op_spec.name,
            &op_spec.config,
            flight_service.clone(),
        )?);
    }
    Ok(ops)
}

/// Build a single `PipelineOp` from its `OpSpec` name + config.
/// R2.2.c (RFC 0005 § 6 item 5): the legacy 60-line match is gone;
/// every operator registers itself in `op_registry::registry()`
/// instead, and this function is now a 1-line dispatch.
fn build_op(
    name: &str,
    config: &std::collections::HashMap<String, String>,
    flight_service: Arc<PylonFlightService>,
) -> Result<Box<dyn PipelineOp>> {
    op_registry::registry().build(name, config, flight_service)
}

/// Encode a RecordBatch as Arrow IPC streaming bytes (one schema
/// message + one RecordBatch message + EOS). M3 B-3.5: replaces the
/// M2 placeholder that emitted `vec![]`.
///
/// The "endpoint" passed to PylonFlightClient is a fake in-process
/// string — the client just uses the descriptor as a key into the
/// buffer; no actual Flight RPC happens here (coord reads the bytes
/// out of `TaskResponse.batch` and decodes them locally).
fn encode_batch_ipc(batch: &arrow_array::RecordBatch) -> Result<Vec<u8>> {
    // The worker is already running inside a tokio runtime (from
    // `#[tokio::main]`), so we drive the async client via
    // `Handle::current().block_on`. We use a dedicated single-thread
    // runtime for the bytes to avoid holding the main runtime
    // blocked — but in M3 first cut the volumes are tiny so the
    // simpler approach is fine.
    // Use a fresh per-batch runtime to avoid re-entering the worker's outer runtime.
    std::thread::scope(|s| {
        s.spawn(|| -> Result<Vec<u8>> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow::anyhow!("encode_batch_ipc runtime: {e}"))?;
            runtime.block_on(async move {
                let client =
                    PylonFlightClient::connect("in-process://worker".into(), "task-batch".into())
                        .await?;
                client.send(batch.clone()).await?;
                client.close().await?;
                Ok::<Vec<u8>, anyhow::Error>(client.take_bytes().await)
            })
        })
        .join()
        .unwrap()
    })
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
