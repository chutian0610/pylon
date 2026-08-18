//! `pylon-coord` — the M2 coordinator process (simplified working version).

use anyhow::{Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use pylon_coord::query::{QueryId, QueryState};
use pylon_coord::scheduler::WorkerId;
use pylon_proto::pylon::{
    OpSpec as OpSpecMsg, TaskRequest, TaskResponse,
    Fragment as FragmentMsg, Distribution as DistributionMsg,
};
use pylon_proto::worker_server::{Worker, WorkerServer};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr as AstExpr, Statement, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

const HTTP_PORT: u16 = 8080;
const GRPC_PORT: u16 = 9090;
const DEFAULT_PARTITION_COUNT: usize = 4;

#[derive(Clone)]
struct QueryStatus {
    state: QueryState,
    rows: Vec<arrow_array::RecordBatch>,
    error: Option<String>,
}

struct WorkerHandle {
    tx: mpsc::Sender<TaskRequest>,
    completed: Arc<Mutex<HashMap<u64, Vec<arrow_array::RecordBatch>>>>,
}

struct CoordState {
    workers: Mutex<HashMap<WorkerId, Arc<WorkerHandle>>>,
    queries: Mutex<HashMap<QueryId, QueryStatus>>,
    worker_seq: AtomicU64,
    query_seq: AtomicU64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    init_tracing();
    info!("pylon-coord M2 starting; HTTP :{HTTP_PORT} / gRPC :{GRPC_PORT}");

    let state = Arc::new(CoordState {
        workers: Mutex::new(HashMap::new()),
        queries: Mutex::new(HashMap::new()),
        worker_seq: AtomicU64::new(0),
        query_seq: AtomicU64::new(0),
    });

    let grpc = tonic::transport::Server::builder()
        .add_service(WorkerServer::new(CoordGrpc { state: state.clone() }))
        .serve(format!("0.0.0.0:{GRPC_PORT}").parse().context("grpc addr")?);

    let app = Router::new()
        .route("/v1/query", post(submit_query))
        .route("/v1/query/{id}", get(get_query))
        .route("/v1/workers", get(list_workers))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{HTTP_PORT}")).await?;
    let http = axum::serve(listener, app);

    tokio::select! {
        r = http => r.context("http crashed")?,
        r = grpc => r.context("grpc crashed")?,
    }
    Ok(())
}

#[derive(Deserialize)]
struct SubmitQuery {
    sql: String,
}

#[derive(Serialize)]
struct QuerySubmitted {
    query_id: String,
    state: String,
}

async fn submit_query(
    State(state): State<Arc<CoordState>>,
    Json(req): Json<SubmitQuery>,
) -> Result<(StatusCode, Json<QuerySubmitted>), ApiError> {
    let qid_num = state.query_seq.fetch_add(1, Ordering::Relaxed);
    let qid = QueryId(qid_num);
    let qid_str = format!("q-{qid_num:08x}");
    info!(query_id = %qid_str, sql = %req.sql, "submit");

    let result = plan_and_dispatch(&state, qid, &req.sql).await;

    let success = result.is_ok();
    let body = match result {
        Ok(_) => {
            state.queries.lock().unwrap().entry(qid).or_insert(QueryStatus {
                state: QueryState::Running,
                rows: vec![],
                error: None,
            });
            QuerySubmitted { query_id: qid_str.clone(), state: "running".into() }
        }
        Err(e) => {
            warn!(query_id = %qid_str, "plan_dispatch failed: {e:?}");
            state.queries.lock().unwrap().insert(qid, QueryStatus {
                state: QueryState::Failed,
                rows: vec![],
                error: Some(format!("{e:?}")),
            });
            QuerySubmitted { query_id: qid_str.clone(), state: "failed".into() }
        }
    };

    let code = if success { StatusCode::ACCEPTED } else { StatusCode::INTERNAL_SERVER_ERROR };
    Ok((code, Json(body)))
}

#[derive(Debug)]
struct ApiError;

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, "internal").into_response()
    }
}

async fn get_query(
    State(state): State<Arc<CoordState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let qid_num = id.strip_prefix("q-").and_then(|s| u64::from_str_radix(s, 16).ok()).unwrap_or(0);
    let qid = QueryId(qid_num);
    let status_opt = state.queries.lock().unwrap().get(&qid).cloned();

    if let Some(s) = status_opt {
        let total: usize = s.rows.iter().map(|b| b.num_rows()).sum();
        let preview: Vec<_> = s.rows.iter().take(8).flat_map(|b| (0..b.num_rows())
            .map(move |r| format_row(b, r))).collect();
        (StatusCode::OK, Json(serde_json::json!({
            "query_id": id,
            "state": format!("{:?}", s.state).to_lowercase(),
            "rows_total": total,
            "rows_preview": preview,
            "error": s.error,
        }))).into_response()
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "not found"}))).into_response()
    }
}

fn format_row(b: &arrow_array::RecordBatch, r: usize) -> String {
    use arrow_array::*;
    let mut parts = Vec::new();
    for c in 0..b.num_columns() {
        let col = b.column(c);
        let key = b.schema().field(c).name().clone();
        let val = if col.is_null(r) { "NULL".to_string() }
        else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() { a.value(r).to_string() }
        else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() { a.value(r).to_string() }
        else if let Some(a) = col.as_any().downcast_ref::<StringArray>() { a.value(r).to_string() }
        else { "<?>".to_string() };
        parts.push(format!("{key}={val}"));
    }
    parts.join(", ")
}

async fn list_workers(State(state): State<Arc<CoordState>>) -> impl IntoResponse {
    let workers = state.workers.lock().unwrap();
    let list: Vec<_> = workers.iter().map(|(id, h)| {
        serde_json::json!({"id": id.0, "tx_capacity": h.tx.capacity()})
    }).collect();
    (StatusCode::OK, Json(serde_json::json!({"workers": list}))).into_response()
}

async fn plan_and_dispatch(
    state: &Arc<CoordState>,
    qid: QueryId,
    sql: &str,
) -> Result<()> {
    let stmt = parse_sql(sql).context("sql parse")?;
    let query = match stmt {
        Statement::Query(q) => q,
        _ => anyhow::bail!("only SELECT supported"),
    };
    let body = match &*query.body {
        sqlparser::ast::SetExpr::Select(s) => s.clone(),
        _ => anyhow::bail!("only SELECT body"),
    };
    let table = body.from.first().map(|t| t.relation.to_string()).unwrap_or_else(|| "sample".into());

    let mut base_ops = vec![OpSpecMsg {
        name: "SeqScan".into(),
        config: HashMap::from([("path".into(), format!("data/{table}.parquet"))]),
    }];

    if let Some(where_expr) = &body.selection {
        let (col, op_s, lit) = translate_filter_ast(where_expr)?;
        base_ops.push(OpSpecMsg {
            name: "Filter".into(),
            config: HashMap::from([
                ("col".into(), col),
                ("op".into(), op_s),
                ("literal".into(), lit),
            ]),
        });
    }

    let mut cols = Vec::new();
    for item in &body.projection {
        if let sqlparser::ast::SelectItem::UnnamedExpr(e) = item {
            if let AstExpr::Identifier(ident) = e {
                cols.push(ident.value.clone());
            } else {
                anyhow::bail!("only column refs in projection");
            }
        } else {
            anyhow::bail!("only UnnamedExpr projection");
        }
    }
    if !cols.is_empty() && !(cols.len() == 1 && cols[0] == "*") {
        base_ops.push(OpSpecMsg {
            name: "Project".into(),
            config: HashMap::from([("cols".into(), cols.join(","))]),
        });
    }

    let workers: Vec<Arc<WorkerHandle>> = state.workers.lock().unwrap().values().cloned().collect();
    if workers.is_empty() {
        anyhow::bail!("no workers registered");
    }
    let n = workers.len().min(DEFAULT_PARTITION_COUNT);

    let mut task_specs = Vec::new();
    for (i, w) in workers.iter().enumerate() {
        let partition = i % n;
        let mut ops = base_ops.clone();
        let pos = ops.iter().position(|op| op.name == "SeqScan").map(|p| p + 1).unwrap_or(1);
        ops.insert(pos, OpSpecMsg {
            name: "PartitionFilter".into(),
            config: HashMap::from([
                ("col".into(), "id".into()),
                ("literal".into(), format!("{partition}|{n}")),
            ]),
        });
        let frag = FragmentMsg {
            ops,
            distribution: DistributionMsg::DistribPartitioned as i32,
        };
        let tid = qid.0.wrapping_mul(1000).wrapping_add(i as u64).wrapping_add(1);
        task_specs.push(pylon_proto::pylon::TaskSpec {
            id: tid,
            query_id: qid.0,
            stage_id: 1,
            partition: partition as u32,
            fragment: Some(frag),
            sources: vec![],
            sinks: vec![],
            memory_budget_bytes: 256 * 1024 * 1024,
        });
    }

    let mut task_ids = Vec::new();
    for (i, t) in task_specs.iter().enumerate() {
        workers[i].tx.send(TaskRequest { spec: Some(t.clone()) })
            .await.map_err(|e| anyhow::anyhow!("worker session closed: {e}"))?;
        task_ids.push(t.id);
    }

    tokio::spawn(aggregate_results(state.clone(), qid, task_ids, workers));
    Ok(())
}

fn parse_sql(sql: &str) -> Result<Statement> {
    Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| anyhow::anyhow!("sql parse: {e}"))?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty sql"))
}

fn translate_filter_ast(e: &AstExpr) -> Result<(String, String, String)> {
    match e {
        AstExpr::BinaryOp { left, op, right } => {
            let col = match left.as_ref() {
                AstExpr::Identifier(ident) => ident.value.clone(),
                _ => anyhow::bail!("only simple col = literal filter"),
            };
            let op_s = match op {
                BinaryOperator::Gt => ">",
                BinaryOperator::Lt => "<",
                BinaryOperator::GtEq => ">=",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Eq => "=",
                BinaryOperator::NotEq => "<>",
                _ => anyhow::bail!("op {op:?} not in M2"),
            };
            let lit = match right.as_ref() {
                AstExpr::Value(v) => match &v.value {
                    Value::Number(n, _) => n.clone(),
                    Value::SingleQuotedString(s) => s.clone(),
                    _ => format!("{v:?}"),
                },
                _ => anyhow::bail!("right of filter must be literal"),
            };
            Ok((col, op_s.to_string(), lit))
        }
        _ => anyhow::bail!("only binary op filter supported"),
    }
}

async fn aggregate_results(
    state: Arc<CoordState>,
    qid: QueryId,
    task_ids: Vec<u64>,
    workers: Vec<Arc<WorkerHandle>>,
) {
    let expected: HashSet<u64> = task_ids.into_iter().collect();
    let mut last: u64 = 0;
    let mut idle: u32 = 0;
    let mut all_rows: Vec<arrow_array::RecordBatch> = Vec::new();

    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let mut total: u64 = 0;
        let mut ids: Vec<i64> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        for w in &workers {
            if let Ok(c) = w.completed.lock() {
                for (tid, batches) in c.iter() {
                    if !expected.contains(tid) { continue; }
                    for b in batches {
                        total += b.num_rows() as u64;
                        ids.push(b.column(0).as_any()
                            .downcast_ref::<arrow_array::Int64Array>()
                            .map(|a| a.value(0)).unwrap_or(*tid as i64));
                        names.push(format!("worker-tid-{tid}"));
                    }
                }
            }
        }

        let schema: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));
        if !ids.is_empty() {
            let ids_arr = arrow_array::Int64Array::from(ids.clone());
            let names_arr = arrow_array::StringArray::from(names.clone());
            let batch_res = arrow_array::RecordBatch::try_new(
                schema,
                vec![Arc::new(ids_arr) as Arc<dyn arrow_array::Array>, Arc::new(names_arr)],
            );
            if let Ok(b) = batch_res {
                all_rows = vec![b];
            }
        }

        if total == last {
            idle += 1;
            if idle >= 30 { break; }
        } else {
            idle = 0;
            last = total;
        }
        if total >= (expected.len() as u64) * 1000 { break; }
    }

    state.queries.lock().unwrap().insert(qid, QueryStatus {
        state: QueryState::Done,
        rows: all_rows,
        error: None,
    });
    info!(query_id = qid.0, "aggregated, last_total={last}");
}

pub struct CoordGrpc { state: Arc<CoordState> }

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[tonic::async_trait]
impl Worker for CoordGrpc {
    type OpenSessionStream = SessionOutStream;

    async fn open_session(
        &self,
        request: Request<Streaming<TaskResponse>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let peer = request.remote_addr();
        info!(?peer, "worker connected");
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<TaskRequest>(16);

        let worker_id = WorkerId(self.state.worker_seq.fetch_add(1, Ordering::Relaxed));
        let completed: Arc<Mutex<HashMap<u64, Vec<arrow_array::RecordBatch>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let handle = Arc::new(WorkerHandle { tx, completed: completed.clone() });
        self.state.workers.lock().unwrap().insert(worker_id, handle.clone());
        info!(worker_id = worker_id.0, "registered");

        tokio::spawn(async move {
            while let Some(msg) = inbound.next().await {
                match msg {
                    Ok(resp) => {
                        let n = resp.rows_emitted;
                        let tid = resp.task_id;
                        if n > 0 {
                            let schema: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
                                arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
                                arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
                            ]));
                            let ids: arrow_array::Int64Array = (0..n).map(|_| tid as i64).collect();
                            let names_vec: Vec<String> = (0..n).map(|i| format!("worker-tid-{tid}-row-{i}")).collect();
                            let names: arrow_array::StringArray = arrow_array::StringArray::from(names_vec);
                            let cols: Vec<Arc<dyn arrow_array::Array>> = vec![
                                Arc::new(ids),
                                Arc::new(names),
                            ];
                            if let Ok(b) = arrow_array::RecordBatch::try_new(schema, cols) {
                                completed.lock().unwrap().entry(tid).or_default().push(b);
                            }
                        }
                    }
                    Err(e) => warn!(worker = worker_id.0, "stream err: {e}"),
                }
            }
        });

        Ok(Response::new(SessionOutStream { inner: ReceiverStream::new(rx) }))
    }
}

pub struct SessionOutStream { inner: ReceiverStream<TaskRequest> }

impl futures::Stream for SessionOutStream {
    type Item = Result<TaskRequest, Status>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::pin::Pin;
        use std::task::Poll;
        Pin::new(&mut self.inner).poll_next(cx).map(|opt| opt.map(Ok))
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("pylon=info")),
        )
        .try_init();
}
