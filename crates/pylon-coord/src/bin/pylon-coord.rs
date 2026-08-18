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
use pylon_coord::fragment::{Fragmenter, FragmenterConfig};
use pylon_coord::scheduler::WorkerId;
use pylon_plan::translate::{logical_from_sql, physical_from_logical, CatalogStub};
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
use tracing::{debug, info, warn};

const HTTP_PORT: u16 = 8080;
const GRPC_PORT: u16 = 9090;
const DEFAULT_PARTITION_COUNT: usize = 4;

struct QueryStatus {
    state: QueryState,
    rows: Vec<arrow_array::RecordBatch>,
    schema: Option<arrow_schema::SchemaRef>,
    error: Option<String>,
    /// M3 B-3.5: task IDs of dispatched tasks.
    /// - `stage0_task_id`: the single stage0 task (for non-aggregate
    ///   queries, the result comes from this task).
    /// - `stage1_task_ids`: per-partition stage1 tasks (for aggregate
    ///   queries, the result is the union of these).
    stage0_task_id: Option<u64>,
    stage1_task_ids: Vec<u64>,
}

impl Clone for QueryStatus {
    fn clone(&self) -> Self {
        Self {
            state: self.state,
            rows: self.rows.clone(),
            schema: self.schema.clone(),
            error: self.error.clone(),
            stage0_task_id: self.stage0_task_id,
            stage1_task_ids: self.stage1_task_ids.clone(),
        }
    }
}

struct WorkerHandle {
    tx: mpsc::Sender<TaskRequest>,
    completed: Arc<Mutex<HashMap<u64, Vec<arrow_array::RecordBatch>>>>,
    /// Arrow Flight host:port registered via `RegisterWorker`. `None`
    /// for M2-style workers that didn't call the new RPC. The
    /// fragmenter uses this to fill `ExchangeSpec.target_worker`
    /// when dispatching a cross-worker shuffle.
    flight_addr: Option<String>,
}

struct CoordState {
    workers: Mutex<HashMap<WorkerId, Arc<WorkerHandle>>>,
    queries: Mutex<HashMap<QueryId, QueryStatus>>,
    worker_seq: AtomicU64,
    query_seq: AtomicU64,
    /// M3 B-1: worker discovery (RegisterWorker registrations +
    /// flight_addr store). See `pylon_coord::discovery`.
    discovery: pylon_coord::Discovery,
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
        discovery: pylon_coord::Discovery::new(),
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

    // M3 B-3.5: insert the entry BEFORE plan_and_dispatch so the
    // polling task spawned inside it can read stage0_task_id.
    state.queries.lock().unwrap().entry(qid).or_insert(QueryStatus {
        state: QueryState::Running,
        rows: vec![],
        schema: None,
        error: None,
        stage0_task_id: None,
        stage1_task_ids: vec![],
    });
    let result = plan_and_dispatch(state.clone(), qid, &req.sql).await;

    let success = result.is_ok();
    let body = match result {
        Ok(_) => QuerySubmitted { query_id: qid_str.clone(), state: "running".into() },
        Err(e) => {
            warn!(query_id = %qid_str, "plan_dispatch failed: {e:?}");
            let mut qmap = state.queries.lock().unwrap();
            if let Some(q) = qmap.get_mut(&qid) {
                q.state = QueryState::Failed;
                q.error = Some(format!("{e:?}"));
            }
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
        serde_json::json!({
            "id": id.0,
            "tx_capacity": h.tx.capacity(),
            "flight_addr": h.flight_addr,
        })
    }).collect();
    (StatusCode::OK, Json(serde_json::json!({"workers": list}))).into_response()
}

async fn plan_and_dispatch(
    state: Arc<CoordState>,
    qid: QueryId,
    sql: &str,
) -> Result<()> {
    // 1. Parse SQL to PhysicalPlan
    let stmt = parse_sql(sql).context("sql parse")?;
    let query = match stmt {
        Statement::Query(q) => q,
        _ => anyhow::bail!("only SELECT supported"),
    };
    let body = match &*query.body {
        sqlparser::ast::SetExpr::Select(s) => s.clone(),
        _ => anyhow::bail!("only SELECT body"),
    };

    let table = body.from.first()
        .map(|t| t.relation.to_string())
        .unwrap_or_else(|| "sample".into());

    let mut columns: Vec<String> = Vec::new();
    for item in &body.projection {
        if let sqlparser::ast::SelectItem::UnnamedExpr(e) = item {
            match e {
                AstExpr::Identifier(ident) => {
                    columns.push(ident.value.clone());
                }
                // Skip aggregate functions / wildcards in the
                // projection — pylon-plan's logical_from_sql
                // handles those. We just want to know which input
                // columns are needed.
                _ => {}
            }
        } else if let sqlparser::ast::SelectItem::Wildcard(_) = item {
            // SELECT * — include all columns
        } else {
            // SELECT ... AS alias or QualifiedWildcard — let
            // pylon-plan parse; we don't need to extract column
            // names manually.
        }
    }

    let filter_pred: Option<(String, String, String)> = if let Some(w) = &body.selection {
        Some(translate_filter_ast(w)?)
    } else {
        None
    };

    // 2. Get registered workers
    let workers: Vec<Arc<WorkerHandle>> = state.workers.lock().unwrap().values().cloned().collect();
    if workers.is_empty() {
        anyhow::bail!("no workers registered");
    }
    let n_partitions = workers.len().min(DEFAULT_PARTITION_COUNT).max(1);

    // 3. Build the DAG via Fragmenter. For M3 B-3.5: use the
    //    registered workers' flight_addrs to route the partitioned
    //    ExchangeSinkRpc in stage0 across the workers.
    let qid_u64 = qid.0;
    let worker_flight_addrs: Vec<String> = workers
        .iter()
        .filter_map(|w| w.flight_addr.clone())
        .collect();
    // Build a minimal catalog + parse the SQL to a PhysicalPlan.
    let mut catalog = CatalogStub::new();
    catalog.register(
        &table,
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("amount", arrow_schema::DataType::Float64, false),
        ])),
        &format!("data/{table}.parquet"),
    );
    // M3 B-3.5: pass the original SQL straight to pylon-plan's
    // translator so aggregate / GROUP BY clauses survive. (The
    // `columns` / `filter_pred` extraction above was just for the
    // legacy in-process dispatch path; with the Fragmenter we don't
    // need to re-synthesize the SQL here.)
    let _ = (columns, filter_pred.as_ref());
    let logical_plan = match logical_from_sql(sql, &catalog) {
        Ok(p) => p,
        Err(e) => {
            return Err(anyhow::anyhow!("logical plan: {e:?}"));
        }
    };
    let physical_plan = match physical_from_logical(logical_plan) {
        Ok(p) => {
                    p
        }
        Err(e) => {
            return Err(anyhow::anyhow!("physical plan: {e:?}"));
        }
    };
    // Use 2 partitions for M3 first cut cross-worker demo.
    let fragmenter = Fragmenter::new(FragmenterConfig { default_partition_count: 2 });
    let dag = match fragmenter.fragment_with_workers(&physical_plan, qid_u64, &worker_flight_addrs) {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!("fragment: {e:?}"));
        }
    };
    info!(query_id = qid_u64, stages = dag.stages.len(), "fragmented plan");
    let (stage0_ops, stage1_tasks) = split_dag_for_dispatch(&dag);

    let stage0 = pylon_proto::pylon::TaskSpec {
        id: qid_u64.wrapping_mul(1000).wrapping_add(1),
        query_id: qid_u64,
        stage_id: 1,
        partition: 0,                    // Single task per worker; concurrency on workers side
        fragment: Some(pylon_proto::pylon::Fragment {
            ops: stage0_ops.clone(),
            distribution: pylon_proto::pylon::Distribution::DistribSingle as i32,
        }),
        sources: vec![],
        sinks: vec![],
        memory_budget_bytes: 256 * 1024 * 1024,
    };
    // (Stage1 dispatch happens in the spawned task below, per-partition.)

    // 4. Dispatch Stage 0 to ONE worker (M3 first cut: only the
    //    first worker scans the data; the cross-worker shuffle is
    //    driven by stage0's ExchangeSinkRpc targeting each worker's
    //    flight_addr).
    if let Some(w) = workers.first() {
        w.tx
            .send(TaskRequest { spec: Some(stage0.clone()) })
            .await
            .map_err(|e| anyhow::anyhow!("worker stage0 send: {e}"))?;
        info!(stage = 0, worker = 0, "stage0 dispatched");
        // Save stage0 task ID for the polling task to drain.
        {
            let mut qmap = state.queries.lock().unwrap();
            info!(stage = 0, qid = ?qid, qmap_len = qmap.len(), keys = ?qmap.keys().collect::<Vec<_>>(), "save stage0_task_id");
            if let Some(q) = qmap.get_mut(&qid) {
                q.stage0_task_id = Some(stage0.id);
                info!(stage = 0, task_id = stage0.id, "saved stage0_task_id");
            } else {
                info!(stage = 0, qid = ?qid, "qmap.get_mut returned None");
            }
        }
    }

    let expected_stage0_acks = 1;
    let state_for_send = state.clone();
    let workers_snapshot = workers.clone();
    let stage1_tasks_clone = stage1_tasks.clone();
    tokio::spawn(async move {
        wait_for_stage_done_inner(
            state.clone(),
            qid_u64,
            1u64,
            expected_stage0_acks,
        ).await;
        // After Stage 0: dispatch each stage1 partition task to a
        // worker (round-robin: partition p → worker p % n_workers).
        if stage1_tasks_clone.is_empty() {
            info!(stage = 1, "no stage1 tasks (non-aggregate query)");
        } else {
            let n_workers = workers_snapshot.len().max(1);
            let mut dispatched_ids: Vec<u64> = Vec::new();
            for (p, partition_ops) in stage1_tasks_clone.iter().enumerate() {
                let worker_idx = p % n_workers;
                let worker = match workers_snapshot.get(worker_idx) {
                    Some(w) => w.clone(),
                    None => {
                        warn!(partition = p, "no worker for partition");
                        continue;
                    }
                };
                let stage1_task_id = qid_u64
                    .wrapping_mul(1000)
                    .wrapping_add(2)
                    .wrapping_add(p as u64);
                let task_spec = pylon_proto::pylon::TaskSpec {
                    id: stage1_task_id,
                    query_id: qid_u64,
                    stage_id: 2,
                    partition: p as u32,
                    fragment: Some(pylon_proto::pylon::Fragment {
                        ops: partition_ops.clone(),
                        distribution: pylon_proto::pylon::Distribution::DistribSingle as i32,
                    }),
                    sources: vec![],
                    sinks: vec![],
                    memory_budget_bytes: 256 * 1024 * 1024,
                };
                if worker.tx
                    .send(TaskRequest { spec: Some(task_spec.clone()) })
                    .await
                    .is_err()
                {
                    warn!(partition = p, worker = worker_idx, "stage1 send failed");
                    continue;
                }
                info!(stage = 1, partition = p, worker = worker_idx, task_id = stage1_task_id, "stage1 dispatched");
                dispatched_ids.push(stage1_task_id);
            }
            // Save the dispatched task IDs for the polling task below
            // to know which task IDs to drain from the workers'
            // completed maps.
            {
                let mut qmap = state_for_send.queries.lock().unwrap();
                if let Some(q) = qmap.get_mut(&pylon_coord::QueryId(qid_u64)) {
                    q.stage1_task_ids = dispatched_ids.clone();
                }
            }
        }

        // After Stage 1 dispatch: poll for completion and aggregate
        // stage1 results from all dispatched task IDs.
        let qid_inner = qid_u64;
        let state_inner = state_for_send.clone();
        // Read the dispatched task IDs we saved earlier.
        let (stage0_task_id, stage1_task_ids): (Option<u64>, Vec<u64>) = {
            let qmap = state_inner.queries.lock().unwrap();
            let result = qmap
                .get(&pylon_coord::QueryId(qid_inner))
                .map(|q| (q.stage0_task_id, q.stage1_task_ids.clone()))
                .unwrap_or((None, vec![]));
            info!(stage0_task_id = ?result.0, stage1_task_ids = ?result.1, "polling: read task ids");
            result
        };
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            let mut all_batches: Vec<arrow_array::RecordBatch> = Vec::new();
            let mut schema: Option<arrow_schema::SchemaRef> = None;
            let qid_q = pylon_coord::QueryId(qid_inner);
            // Collect all task IDs to drain (stage0 + stage1).
            let mut task_ids: Vec<u64> = Vec::new();
            if let Some(t) = stage0_task_id { task_ids.push(t); }
            task_ids.extend(stage1_task_ids.iter().copied());
            {
                let workers_lock = state_inner.workers.lock().unwrap();
                let mut seen = 0usize;
                for w in workers_lock.values() {
                    let comp = w.completed.lock().unwrap();
                    for tid in &task_ids {
                        if let Some(batches) = comp.get(tid) {
                            for b in batches {
                                if schema.is_none() {
                                    schema = Some(b.schema());
                                }
                                seen += b.num_rows();
                                all_batches.push(b.clone());
                            }
                        }
                    }
                }
                info!(
                    rows = seen,
                    batches = all_batches.len(),
                    "aggregated task results"
                );
            }
            let mut qmap = state_inner.queries.lock().unwrap();
            if let Some(s) = qmap.get_mut(&qid_q) {
                s.rows = all_batches;
                s.schema = schema;
                s.state = pylon_coord::query::QueryState::Done;
            }
        });
    });

    Ok(())
}

fn build_stage0_ops(
    table: &str,
    _columns: &[String],
    filter: Option<&(String, String, String)>,
    n_partitions: usize,
    query_id: u64,
) -> Vec<pylon_proto::pylon::OpSpec> {
    use std::collections::HashMap;
    let mut ops = vec![pylon_proto::pylon::OpSpec {
        name: "SeqScan".into(),
        config: HashMap::from([("path".into(), format!("data/{table}.parquet"))]),
    }];
    // PartitionFilter: id % n_partitions == 0 (single-partition M3 first cut)
    ops.push(pylon_proto::pylon::OpSpec {
        name: "PartitionFilter".into(),
        config: HashMap::from([
            ("col".into(), "id".into()),
            ("literal".into(), format!("0|{n_partitions}")),
        ]),
    });
    if let Some((col, op_s, lit)) = filter {
        ops.push(pylon_proto::pylon::OpSpec {
            name: "Filter".into(),
            config: HashMap::from([
                ("col".into(), col.clone()),
                ("op".into(), op_s.clone()),
                ("literal".into(), lit.clone()),
            ]),
        });
    }
    // ExchangeSink at end of Stage 0 — pushes to stage1/task0 via in-process Flight
    let sink_desc = format!("pylon://query/{query_id}/stage/2/task/0");
    ops.push(pylon_proto::pylon::OpSpec {
        name: "ExchangeSink".into(),
        config: HashMap::from([("descriptor".into(), sink_desc)]),
    });
    ops
}

fn build_stage1_ops(
    columns: &[String],
    query_id: u64,
) -> Vec<pylon_proto::pylon::OpSpec> {
    use std::collections::HashMap;
    let mut ops = Vec::new();
    // ExchangeSource first
    let source_desc = format!("pylon://query/{query_id}/stage/2/task/0");
    ops.push(pylon_proto::pylon::OpSpec {
        name: "ExchangeSource".into(),
        config: HashMap::from([("descriptor".into(), source_desc)]),
    });
    // Project (M3: keep columns as before)
    if !columns.is_empty() && !(columns.len() == 1 && columns[0] == "*") {
        ops.push(pylon_proto::pylon::OpSpec {
            name: "Project".into(),
            config: HashMap::from([("cols".into(), columns.join(","))]),
        });
    }
    ops
}


/// Split a Fragmenter-produced StageDag into (stage0 ops, per-partition
/// stage1 task op lists). M3 B-3.5: stage0 is always 1 task (the
/// Fragmenter emits a single stage0 task with N
/// ExchangeSink[Rpc] targets). Stage 1 has N partitioned tasks;
/// each is `[ExchangeSource, Aggregate]` (the Fragmenter emits them
/// as a flat `stage1_ops` list with the [source, agg] pair layout).
fn split_dag_for_dispatch(
    dag: &pylon_coord::StageDag,
) -> (Vec<pylon_proto::pylon::OpSpec>, Vec<Vec<pylon_proto::pylon::OpSpec>>) {
    let to_proto = |op: &pylon_coord::stage::OpSpec| pylon_proto::pylon::OpSpec {
        name: op.name.clone(),
        config: op.config.clone(),
    };
    let stage0_ops: Vec<pylon_proto::pylon::OpSpec> =
        dag.stages[0].fragment.ops.iter().map(&to_proto).collect();
    let stage1_ops = &dag.stages[1].fragment.ops;
    let n_partitions = dag.stages[1].partition_count;
    let mut tasks: Vec<Vec<pylon_proto::pylon::OpSpec>> = Vec::with_capacity(n_partitions);
    if n_partitions == 0 {
        return (stage0_ops, tasks);
    }
    // Layout: N sources first, then N aggregates. Slice into N pairs.
    let half = stage1_ops.len() / 2;
    let n = half.min(n_partitions);
    for i in 0..n {
        let pair = vec![to_proto(&stage1_ops[i]), to_proto(&stage1_ops[half + i])];
        tasks.push(pair);
    }
    (stage0_ops, tasks)
}

/// Wait for stage N to complete (sleep-based heuristic in M3 first cut).
/// Future: tick on TaskState::TaskDone ACKs from workers.
async fn wait_for_stage_done_inner(
    _state: Arc<CoordState>,
    query_id: u64,
    stage_id: u64,
    _expected_count: usize,
) {
    // M3 first cut: sleep a fixed duration
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    info!(query_id, stage = stage_id, "stage acked (M3 sleep heuristic)");
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
        schema: None,
        error: None,
        stage0_task_id: None,
        stage1_task_ids: vec![],
    });
    info!(query_id = qid.0, "aggregated, last_total={last}");
}

pub struct CoordGrpc { state: Arc<CoordState> }

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[tonic::async_trait]
impl Worker for CoordGrpc {
    type OpenSessionStream = SessionOutStream;

    async fn register_worker(
        &self,
        request: tonic::Request<pylon_proto::pylon::RegisterWorkerRequest>,
    ) -> Result<tonic::Response<pylon_proto::pylon::RegisterWorkerResponse>, tonic::Status> {
        let req = request.into_inner();
        if req.flight_addr.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "flight_addr is required",
            ));
        }
        let worker_id = self
            .state
            .worker_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let reg = self.state.discovery.register(
            worker_id,
            req.flight_addr,
            req.grpc_addr,
        );
        info!(
            worker_id = reg.worker_id,
            flight_addr = %reg.flight_addr,
            "worker registered via RegisterWorker"
        );
        Ok(tonic::Response::new(
            pylon_proto::pylon::RegisterWorkerResponse {
                worker_id: reg.worker_id,
            },
        ))
    }

    async fn open_session(
        &self,
        request: Request<Streaming<TaskResponse>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let peer = request.remote_addr();
        // M3 B-1: if the worker passed x-pylon-worker-id (returned by
        // RegisterWorker), pair the session with the prior
        // registration and use that worker_id. Otherwise fall back
        // to the M2 auto-assign path (no flight_addr).
        let header_worker_id: Option<u64> = request
            .metadata()
            .get("x-pylon-worker-id")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        let pre_registered = header_worker_id
            .and_then(|id| self.state.discovery.lookup(id));

        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<TaskRequest>(16);

        let (worker_id, flight_addr) = match pre_registered {
            Some(reg) => {
                info!(?peer, registered_worker_id = reg.worker_id, flight_addr = %reg.flight_addr, "worker connected (registered)");
                (WorkerId(reg.worker_id), Some(reg.flight_addr.clone()))
            }
            None => {
                let id = WorkerId(self.state.worker_seq.fetch_add(1, Ordering::Relaxed));
                info!(?peer, worker_id = id.0, "worker connected (M2 auto-assign)");
                (id, None)
            }
        };
        let completed: Arc<Mutex<HashMap<u64, Vec<arrow_array::RecordBatch>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let handle = Arc::new(WorkerHandle {
            tx,
            completed: completed.clone(),
            flight_addr,
        });
        self.state.workers.lock().unwrap().insert(worker_id, handle.clone());
        info!(worker_id = worker_id.0, "registered");

        tokio::spawn(async move {
            while let Some(msg) = inbound.next().await {
                match msg {
                    Ok(resp) => {
                        let tid = resp.task_id;
                        // M3 B-3.5: decode the real Arrow IPC streaming
                        // bytes from the worker. A single response
                        // carries one full IPC stream (schema + N
                        // batches + EOS), so we may decode multiple
                        // RecordBatches per response.
                        if !resp.batch.is_empty() {
                            match decode_ipc_stream(&resp.batch) {
                                Ok(batches) if !batches.is_empty() => {
                                    let n: usize =
                                        batches.iter().map(|b| b.num_rows()).sum();
                                    debug!(worker = worker_id.0, task_id = tid, batches = batches.len(), rows = n, "decoded IPC stream");
                                    completed
                                        .lock()
                                        .unwrap()
                                        .entry(tid)
                                        .or_default()
                                        .extend(batches.clone());
                                    let stored = completed.lock().unwrap().get(&tid).map(|v| v.len()).unwrap_or(0);
                                    debug!(worker = worker_id.0, task_id = tid, stored_batches = stored, "stored in completed");
                                }
                                Ok(_) => {
                                    // Empty stream (just schema + EOS). Ignore.
                                }
                                Err(e) => {
                                    warn!(
                                        worker = worker_id.0,
                                        task_id = tid,
                                        error = %e,
                                        "failed to decode TaskResponse.batch IPC stream"
                                    );
                                }
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

/// Decode an Arrow IPC streaming payload (schema + N RecordBatch
/// messages + EOS) into the contained RecordBatches. M3 B-3.5.
fn decode_ipc_stream(bytes: &[u8]) -> anyhow::Result<Vec<arrow_array::RecordBatch>> {
    use arrow_ipc::reader::StreamReader;
    let cursor = std::io::Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)?;
    reader.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
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
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("pylon=info")),
        )
        .try_init();
}
