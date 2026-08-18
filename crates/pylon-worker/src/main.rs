//! `pylon-worker` — connects to a coordinator over gRPC and runs TaskSpec instances.

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use futures::StreamExt;
use tonic::Request;
use pylon_proto::pylon::{
    RegisterWorkerRequest, TaskRequest, TaskResponse, TaskState,
};
use pylon_proto::worker_client::WorkerClient;
use pylon_exchange::{FlightDescriptor, FlightServerImpl, PylonFlightService};
use pylon_runtime::ops::{
    AggSpec, ExchangeSinkOp, ExchangeSourceOp, FilterOp, HashAggregateOp,
    PartitionFilterOp, ProjectOp, SeqScanOp,
};
use pylon_runtime::{Driver, DriverMode, Pipeline, PipelineOp};
use std::sync::Arc;
use tracing::{info, warn};

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

    run(flight_service, bound_flight_addr.to_string(), grpc_local_addr).await
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

    let ops = build_ops(fragment, flight_service.clone())?;
    let pipeline = Pipeline::new(ops);
    let driver = Driver::new(pipeline);  // default mode = SingleThreadLoop

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
        ops.push(build_op(&op_spec.name, &op_spec.config, flight_service.clone())?);
    }
    Ok(ops)
}

/// Parse the `agg_specs` config value into a list of `AggSpec`.
/// Accepts semicolon-separated entries, each either `count()`
/// (for `COUNT(*)`) or `func:col` (e.g. `sum:amount`).
fn parse_agg_specs(specs: &str) -> Result<Vec<AggSpec>> {
    let mut out = Vec::new();
    for spec in specs.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if let Some(inner) = spec.strip_prefix("count(").and_then(|s| s.strip_suffix(")")) {
            if !inner.is_empty() {
                anyhow::bail!("count() takes no arguments; got count({inner})");
            }
            out.push(AggSpec {
                func: "count".into(),
                arg_col: None,
                out_name: "count".into(),
            });
        } else if let Some((func, col)) = spec.split_once(':') {
            let func = func.trim().to_lowercase();
            let col = col.trim();
            if col.is_empty() {
                anyhow::bail!("aggregate {func}() requires a column");
            }
            out.push(AggSpec {
                func,
                arg_col: Some(col.to_string()),
                out_name: spec.to_string(),
            });
        } else {
            anyhow::bail!("malformed agg spec: {spec}");
        }
    }
    Ok(out)
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
            // A2-1: two modes.
            //   - Partitioned: config has `descriptors` (semicolon-joined),
            //     `n_partitions`, and `partition_keys`. The op routes
            //     each row by hash of the partition-key column values
            //     to one of N descriptors.
            //   - Single: config has `descriptor` (A1 behavior).
            if let Some(descs_str) = config.get("descriptors") {
                let descriptors: Vec<FlightDescriptor> = descs_str
                    .split(';')
                    .filter(|s| !s.is_empty())
                    .map(|s| FlightDescriptor(s.to_string()))
                    .collect();
                let partition_keys: Vec<String> = get("partition_keys")?
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                Ok(Box::new(ExchangeSinkOp::new_partitioned(
                    descriptors,
                    partition_keys,
                    flight_service.clone(),
                )))
            } else {
                let desc = FlightDescriptor(get("descriptor")?);
                Ok(Box::new(ExchangeSinkOp::new(desc, flight_service.clone())))
            }
        }
        "ExchangeSource" => {
            let desc = FlightDescriptor(get("descriptor")?);
            Ok(Box::new(ExchangeSourceOp::new(desc, flight_service)))
        }
        "Aggregate" => {
            // M3 A1-4 wiring. The fragmenter emits two config keys:
            //   group_by_cols: comma-separated column names
            //   agg_specs:     semicolon-separated, each entry is
            //                  either "count()" (for COUNT(*)) or
            //                  "func:col" (e.g. "sum:amount", "min:id").
            // The post-aggregate schema isn't carried in the OpSpec
            // (M3 first cut); the op derives it lazily on the first
            // input batch.
            let group_by_cols: Vec<String> = get("group_by_cols")?
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let aggregates = parse_agg_specs(&get("agg_specs")?)?;
            // Schema::empty() so the op derives it on first input batch.
            let schema = std::sync::Arc::new(arrow_schema::Schema::empty());
            Ok(Box::new(HashAggregateOp::new(group_by_cols, aggregates, schema)))
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
