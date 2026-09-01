//! `pylon-coord` — the M2 coordinator process (simplified working version).

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use futures::StreamExt;
use pylon_coord::fragment::{Fragmenter, FragmenterConfig};
use pylon_coord::query::{QueryId, QueryState};
use pylon_coord::scheduler::WorkerId;
use pylon_plan::translate::{CatalogStub, logical_from_sql, physical_from_logical};
use pylon_proto::pylon::{TaskRequest, TaskResponse};
use pylon_proto::worker_server::{Worker, WorkerServer};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr as AstExpr, Statement, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, trace, warn};

const HTTP_PORT: u16 = 8080;
const GRPC_PORT: u16 = 9090;
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
    /// M3-tail #1 (RFC 0005 R7): per-(query, stage) ack tracking +
    /// notifier. The dispatcher's stage barrier awaits
    /// `wait_for_stage_done` instead of `tokio::time::sleep`.
    state_machine: Arc<pylon_coord::QueryStateMachine>,
    /// Reverse index: `task_id → (query_id, stage_id)`. Populated
    /// when the dispatcher emits a `TaskRequest`; consulted by the
    /// `OpenSession` inbound handler to translate a `TaskResponse`
    /// into a `QueryStateMachine::ack_task` call.
    task_locs: Mutex<HashMap<u64, (pylon_coord::QueryId, pylon_coord::StageId)>>,
    /// RFC 0007 §3.5 retry path: the last TaskSpec sent for each
    /// task id. The retry watcher clones the spec, injects the
    /// spill handle, and re-dispatches without rebuilding the DAG.
    task_specs: Mutex<HashMap<u64, pylon_proto::pylon::TaskSpec>>,
}

/// Poisoning-tolerant mutex lock (C5.6). The coord is a
/// long-lived service: a panic that poisons a `CoordState` mutex
/// must degrade that query's state consistency, not wedge every
/// future request behind an `.unwrap()`.
fn lock_ok<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
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
        state_machine: pylon_coord::QueryStateMachine::new(),
        task_locs: Mutex::new(HashMap::new()),
        task_specs: Mutex::new(HashMap::new()),
    });

    let grpc = tonic::transport::Server::builder()
        .add_service(WorkerServer::new(CoordGrpc {
            state: state.clone(),
        }))
        .serve(
            format!("0.0.0.0:{GRPC_PORT}")
                .parse()
                .context("grpc addr")?,
        );

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
    state
        .queries
        .lock()
        .unwrap()
        .entry(qid)
        .or_insert(QueryStatus {
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
        Ok(_) => QuerySubmitted {
            query_id: qid_str.clone(),
            state: "running".into(),
        },
        Err(e) => {
            warn!(query_id = %qid_str, "plan_dispatch failed: {e:?}");
            let mut qmap = lock_ok(&state.queries);
            if let Some(q) = qmap.get_mut(&qid) {
                q.state = QueryState::Failed;
                q.error = Some(format!("{e:?}"));
            }
            QuerySubmitted {
                query_id: qid_str.clone(),
                state: "failed".into(),
            }
        }
    };

    let code = if success {
        StatusCode::ACCEPTED
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
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
    let qid_num = id
        .strip_prefix("q-")
        .and_then(|s| u64::from_str_radix(s, 16).ok())
        .unwrap_or(0);
    let qid = QueryId(qid_num);
    let status_opt = lock_ok(&state.queries).get(&qid).cloned();

    if let Some(s) = status_opt {
        let total: usize = s.rows.iter().map(|b| b.num_rows()).sum();
        let preview: Vec<_> = s
            .rows
            .iter()
            .take(8)
            .flat_map(|b| (0..b.num_rows()).map(move |r| format_row(b, r)))
            .collect();
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "query_id": id,
                "state": format!("{:?}", s.state).to_lowercase(),
                "rows_total": total,
                "rows_preview": preview,
                "error": s.error,
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "not found"})),
        )
            .into_response()
    }
}

fn format_row(b: &arrow_array::RecordBatch, r: usize) -> String {
    use arrow_array::*;
    let mut parts = Vec::new();
    for c in 0..b.num_columns() {
        let col = b.column(c);
        let key = b.schema().field(c).name().clone();
        let val = if col.is_null(r) {
            "NULL".to_string()
        } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            a.value(r).to_string()
        } else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
            a.value(r).to_string()
        } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            a.value(r).to_string()
        } else {
            "<?>".to_string()
        };
        parts.push(format!("{key}={val}"));
    }
    parts.join(", ")
}

async fn list_workers(State(state): State<Arc<CoordState>>) -> impl IntoResponse {
    let workers = lock_ok(&state.workers);
    let list: Vec<_> = workers
        .iter()
        .map(|(id, h)| {
            serde_json::json!({
                "id": id.0,
                "tx_capacity": h.tx.capacity(),
                "flight_addr": h.flight_addr,
            })
        })
        .collect();
    (StatusCode::OK, Json(serde_json::json!({"workers": list}))).into_response()
}

/// RFC 0007 §3.5: watch one stage for `Stalled` acks and re-dispatch
/// the affected tasks from their stashed specs, injecting the spill
/// handle so the retried attempt resumes from the spill instead of
/// restarting. Exits when the stage reaches a terminal state or the
/// hard cap elapses.
async fn retry_watcher(
    state: Arc<CoordState>,
    qid: pylon_coord::QueryId,
    sid: pylon_coord::StageId,
    hard_cap: std::time::Duration,
) {
    let start = std::time::Instant::now();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        match state.state_machine.stage_state(qid, sid) {
            Some(pylon_coord::query_state::StageState::Done)
            | Some(pylon_coord::query_state::StageState::Failed) => return,
            _ => {}
        }
        if start.elapsed() >= hard_cap {
            warn!(query_id = qid.0, stage_id = sid.0, "retry watcher hard cap");
            return;
        }
        for (tid, _) in state.state_machine.stalled_handles(qid, sid) {
            // Consume the handle exactly once; the retried attempt's
            // terminal ack clears the QSM entry.
            let Some(handle) = state.state_machine.clear_stalled(qid, sid, tid) else {
                continue;
            };
            let spec = lock_ok(&state.task_specs).get(&tid.0).cloned();
            let Some(mut spec) = spec else {
                continue;
            };
            if let Some(fragment) = spec.fragment.as_mut() {
                for op in &mut fragment.ops {
                    if op.name == "Aggregate" {
                        op.config.insert(
                            "spill_handle".to_string(),
                            handle.path.to_string_lossy().to_string(),
                        );
                    }
                }
            }
            let workers: Vec<Arc<WorkerHandle>> =
                lock_ok(&state.workers).values().cloned().collect();
            let Some(worker) = workers.get(tid.0 as usize % workers.len().max(1)).cloned() else {
                // No worker available; put the handle back for the
                // next pass.
                state.state_machine.put_back_stalled(qid, sid, tid, handle);
                continue;
            };
            if worker
                .tx
                .send(TaskRequest { spec: Some(spec) })
                .await
                .is_ok()
            {
                info!(
                    task_id = tid.0,
                    query_id = qid.0,
                    stage_id = sid.0,
                    spill = %handle.path.display(),
                    "re-dispatched stalled task"
                );
            } else {
                state.state_machine.put_back_stalled(qid, sid, tid, handle);
            }
        }
    }
}

async fn plan_and_dispatch(state: Arc<CoordState>, qid: QueryId, sql: &str) -> Result<()> {
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

    let table = body
        .from
        .first()
        .map(|t| t.relation.to_string())
        .unwrap_or_else(|| "sample".into());

    let mut columns: Vec<String> = Vec::new();
    for item in &body.projection {
        if let sqlparser::ast::SelectItem::UnnamedExpr(e) = item {
            if let AstExpr::Identifier(ident) = e {
                columns.push(ident.value.clone());
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
    let workers: Vec<Arc<WorkerHandle>> = lock_ok(&state.workers).values().cloned().collect();
    if workers.is_empty() {
        anyhow::bail!("no workers registered");
    }
    // n_partitions is decided by the fragmenter config and applied
    // uniformly (see `FragmenterConfig::default_partition_count`).
    // We don't pin it to `workers.len()` so dispatch stays
    // independent of cluster size.

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
    // `physical_from_logical` now returns `Arc<dyn ExecutionPlan>`
    // (post-R2.3). The fragmenter consumes that directly — no enum
    // match, no wrap.
    let physical_plan = match physical_from_logical(logical_plan) {
        Ok(p) => p,
        Err(e) => {
            return Err(anyhow::anyhow!("physical plan: {e:?}"));
        }
    };
    // Use 2 partitions for M3 first cut cross-worker demo.
    let fragmenter = Fragmenter::new(FragmenterConfig {
        default_partition_count: 2,
    });
    let dag = match fragmenter.fragment(&physical_plan, qid_u64, &worker_flight_addrs) {
        Ok(d) => d,
        Err(e) => {
            return Err(anyhow::anyhow!("fragment: {e:?}"));
        }
    };
    info!(
        query_id = qid_u64,
        stages = dag.stages.len(),
        "fragmented plan"
    );
    let (stage0_ops, stage1_tasks) = split_dag_for_dispatch(&dag);

    // M3 tail — PR1 (B3): the dispatcher is the authoritative
    // source for stage1 partition → worker assignment. Rewrite
    // `ExchangeSinkRpc.target_flight_addrs` in the stage0 ops with
    // the actual addresses so that same-worker partitions become
    // a true loopback gRPC target (rather than relying on the
    // in-process `ExchangeSinkOp` short-circuit that PR2 removes).
    let stage1_partition_count = dag.stages[1].partition_count;
    let stage1_flight_addrs: Vec<String> = (0..stage1_partition_count)
        .map(|p| {
            worker_flight_addrs
                .get(p % worker_flight_addrs.len().max(1))
                .cloned()
                .unwrap_or_default()
        })
        .collect();
    let mut stage0_ops = stage0_ops;
    let rewrites = rewrite_exchange_targets_in_place(
        &mut stage0_ops,
        stage1_partition_count,
        &stage1_flight_addrs,
    );
    if rewrites > 0 {
        info!(
            rewrites,
            stage1_partition_count,
            stage1_workers = stage1_flight_addrs.len(),
            "PR1 dispatch-rewrote ExchangeSinkRpc.target_flight_addrs"
        );
    }

    let stage0 = pylon_proto::pylon::TaskSpec {
        id: qid_u64.wrapping_mul(1000).wrapping_add(1),
        query_id: qid_u64,
        stage_id: 1,
        partition: 0, // Single task per worker; concurrency on workers side
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
        w.tx.send(TaskRequest {
            spec: Some(stage0.clone()),
        })
        .await
        .map_err(|e| anyhow::anyhow!("worker stage0 send: {e}"))?;
        info!(stage = 0, worker = 0, "stage0 dispatched");
        // RFC 0007 §3.5: keep the spec so the retry watcher can
        // re-dispatch from it if the task acks Stalled.
        state
            .task_specs
            .lock()
            .unwrap()
            .insert(stage0.id, stage0.clone());
        // Save stage0 task ID for the polling task to drain.
        {
            let mut qmap = lock_ok(&state.queries);
            info!(stage = 0, qid = ?qid, qmap_len = qmap.len(), keys = ?qmap.keys().collect::<Vec<_>>(), "save stage0_task_id");
            if let Some(q) = qmap.get_mut(&qid) {
                q.stage0_task_id = Some(stage0.id);
                info!(stage = 0, task_id = stage0.id, "saved stage0_task_id");
            } else {
                info!(stage = 0, qid = ?qid, "qmap.get_mut returned None");
            }
        }
    }

    let _expected_stage0_acks = 1_usize; // legacy field; QSM owns the count now
    let state_for_send = state.clone();
    let workers_snapshot = workers.clone();
    let stage1_tasks_clone = stage1_tasks.clone();
    let stage0_task_id_for_register = stage0.id;
    let qid_u64_for_spawn = qid_u64;
    let stage0_qid_for_register = qid_u64;
    let stage0_stage_id = 1u64;
    let stage0_deadline = std::time::Duration::from_secs(30);
    let stage1_deadline = std::time::Duration::from_secs(30);
    tokio::spawn(async move {
        // RFC 0005 R7 (M3-tail #1): register stage 0 with the
        // QueryStateMachine and await its TASK_DONE acks instead of
        // sleeping a heuristic.
        let stage0_qid = pylon_coord::QueryId(stage0_qid_for_register);
        let stage0_sid = pylon_coord::StageId(stage0_stage_id);
        state_for_send.state_machine.register_stage(
            stage0_qid,
            stage0_sid,
            vec![pylon_coord::TaskId(stage0_task_id_for_register)],
        );
        state_for_send
            .task_locs
            .lock()
            .unwrap()
            .insert(stage0_task_id_for_register, (stage0_qid, stage0_sid));
        // RFC 0007 §3.5: consume Stalled acks while the stage runs.
        {
            let watcher_state = state_for_send.clone();
            let watcher_qid = stage0_qid;
            let watcher_sid = stage0_sid;
            tokio::spawn(retry_watcher(
                watcher_state,
                watcher_qid,
                watcher_sid,
                stage0_deadline + std::time::Duration::from_secs(5),
            ));
        }
        let stage0_wait = state_for_send
            .state_machine
            .wait_for_stage_done(stage0_qid, stage0_sid, stage0_deadline)
            .await;
        if let Err(e) = stage0_wait {
            warn!(
                query_id = stage0_qid.0, stage_id = stage0_stage_id, error = %e,
                "stage 0 ack failed; aborting stage 1 dispatch"
            );
            let mut qmap = lock_ok(&state_for_send.queries);
            if let Some(q) = qmap.get_mut(&stage0_qid) {
                q.state = pylon_coord::query::QueryState::Failed;
                q.error = Some(format!("stage 0: {e}"));
            }
            // C5.6: terminal query — drop QSM bookkeeping so a
            // long-lived coord does not accumulate stale maps.
            state_for_send.state_machine.remove_query(stage0_qid);
            return;
        }
        info!(
            query_id = stage0_qid.0,
            stage_id = stage0_stage_id,
            "stage 0 acked"
        );
        // After Stage 0: dispatch each stage1 partition task to a
        // worker (round-robin: partition p → worker p % n_workers).
        // Stage 1 tasks are pre-registered with the QSM so we can
        // wait_for_stage_done directly after the dispatch loop.
        let mut dispatched_ids: Vec<u64> = Vec::new();
        let mut stage1_register_ids: Vec<pylon_coord::TaskId> = Vec::new();
        if stage1_tasks_clone.is_empty() {
            info!(
                stage = 1,
                query_id = stage0_qid.0,
                "no stage1 tasks (non-aggregate query)"
            );
        } else {
            let n_workers = workers_snapshot.len().max(1);
            let stage1_qid = stage0_qid;
            let stage1_sid = pylon_coord::StageId(2);
            for (p, partition_ops) in stage1_tasks_clone.iter().enumerate() {
                let worker_idx = p % n_workers;
                let worker = match workers_snapshot.get(worker_idx) {
                    Some(w) => w.clone(),
                    None => {
                        warn!(partition = p, "no worker for partition");
                        continue;
                    }
                };
                let stage1_task_id = qid_u64_for_spawn
                    .wrapping_mul(1000)
                    .wrapping_add(2)
                    .wrapping_add(p as u64);
                let task_spec = pylon_proto::pylon::TaskSpec {
                    id: stage1_task_id,
                    query_id: qid_u64_for_spawn,
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
                if worker
                    .tx
                    .send(TaskRequest {
                        spec: Some(task_spec.clone()),
                    })
                    .await
                    .is_err()
                {
                    warn!(partition = p, worker = worker_idx, "stage1 send failed");
                    continue;
                }
                // RFC 0007 §3.5: stash the spec for the retry watcher.
                state_for_send
                    .task_specs
                    .lock()
                    .unwrap()
                    .insert(stage1_task_id, task_spec.clone());
                // Register with QSM + reverse index immediately so
                // the inbound open_session handler can resolve
                // TaskResponse.task_id → (qid, sid) and the
                // follow-up wait sees the full expected set.
                state_for_send
                    .task_locs
                    .lock()
                    .unwrap()
                    .insert(stage1_task_id, (stage1_qid, stage1_sid));
                stage1_register_ids.push(pylon_coord::TaskId(stage1_task_id));
                dispatched_ids.push(stage1_task_id);
                info!(
                    stage = 1,
                    partition = p,
                    worker = worker_idx,
                    task_id = stage1_task_id,
                    "stage1 dispatched"
                );
            }
            if !stage1_register_ids.is_empty() {
                state_for_send.state_machine.register_stage(
                    stage1_qid,
                    stage1_sid,
                    stage1_register_ids,
                );
                {
                    let watcher_state = state_for_send.clone();
                    let watcher_qid = stage1_qid;
                    let watcher_sid = stage1_sid;
                    tokio::spawn(retry_watcher(
                        watcher_state,
                        watcher_qid,
                        watcher_sid,
                        stage1_deadline + std::time::Duration::from_secs(5),
                    ));
                }
                let stage1_wait = state_for_send
                    .state_machine
                    .wait_for_stage_done(stage1_qid, stage1_sid, stage1_deadline)
                    .await;
                if let Err(e) = stage1_wait {
                    warn!(
                        query_id = stage1_qid.0, stage_id = stage1_sid.0, error = %e,
                        "stage 1 ack failed; result set will be partial"
                    );
                    let mut qmap = lock_ok(&state_for_send.queries);
                    if let Some(q) = qmap.get_mut(&stage1_qid) {
                        q.state = pylon_coord::query::QueryState::Failed;
                        q.error = Some(format!("stage 1: {e}"));
                    }
                    // C5.6: terminal query — drop QSM bookkeeping.
                    state_for_send.state_machine.remove_query(stage1_qid);
                } else {
                    info!(
                        query_id = stage1_qid.0,
                        stage_id = stage1_sid.0,
                        "stage 1 acked"
                    );
                }
            }
            // Save the dispatched task IDs for the result-drain step
            // below so we know which completed maps to consult.
            {
                let mut qmap = lock_ok(&state_for_send.queries);
                if let Some(q) = qmap.get_mut(&pylon_coord::QueryId(qid_u64_for_spawn)) {
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
            let qmap = lock_ok(&state_inner.queries);
            let result = qmap
                .get(&pylon_coord::QueryId(qid_inner))
                .map(|q| (q.stage0_task_id, q.stage1_task_ids.clone()))
                .unwrap_or((None, vec![]));
            info!(stage0_task_id = ?result.0, stage1_task_ids = ?result.1, "polling: read task ids");
            result
        };
        tokio::spawn(async move {
            // RFC 0005 R7: by the time we get here, the dispatcher
            // above has already awaited stage 0 + stage 1 via the
            // QueryStateMachine. Workers' `TaskResponse` payloads
            // have landed in `w.completed`; we just drain them.
            let mut all_batches: Vec<arrow_array::RecordBatch> = Vec::new();
            let mut schema: Option<arrow_schema::SchemaRef> = None;
            let qid_q = pylon_coord::QueryId(qid_inner);
            // Collect all task IDs to drain (stage0 + stage1).
            let mut task_ids: Vec<u64> = Vec::new();
            if let Some(t) = stage0_task_id {
                task_ids.push(t);
            }
            task_ids.extend(stage1_task_ids.iter().copied());
            {
                let workers_lock = lock_ok(&state_inner.workers);
                let mut seen = 0usize;
                for w in workers_lock.values() {
                    let comp = lock_ok(&w.completed);
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
            let mut qmap = lock_ok(&state_inner.queries);
            if let Some(s) = qmap.get_mut(&qid_q) {
                s.rows = all_batches;
                s.schema = schema;
                s.state = pylon_coord::query::QueryState::Done;
            }
            // C5.6: terminal query — drop QSM bookkeeping.
            state_inner.state_machine.remove_query(qid_q);
        });
    });

    Ok(())
}

/// stage1 task op lists). M3 B-3.5: stage0 is always 1 task (the
/// Fragmenter emits a single stage0 task with N
/// ExchangeSink[Rpc] targets). Stage 1 has N partitioned tasks;
/// each is `[ExchangeSource, Aggregate]` (the Fragmenter emits them
/// as a flat `stage1_ops` list with the [source, agg] pair layout).
fn split_dag_for_dispatch(
    dag: &pylon_coord::StageDag,
) -> (
    Vec<pylon_proto::pylon::OpSpec>,
    Vec<Vec<pylon_proto::pylon::OpSpec>>,
) {
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

/// M3 tail — PR1 (B3): rewrite `ExchangeSinkRpc.target_flight_addrs`
/// at dispatch time so the per-partition flight_addr list reflects
/// the actual stage1 partition → worker assignment, not the
/// fragmenter's best-effort round-robin.
///
/// The fragmenter emits a placeholder `target_flight_addrs` keyed
/// off its `worker_flight_addrs` (ordered by registration); the
/// dispatcher is the authoritative source for which worker runs
/// stage1 partition p, so we overwrite the placeholder here before
/// the stage0 task is shipped to the worker. After this rewrite,
/// same-worker shuffle is naturally expressed as
/// `target_flight_addr == local flight_addr` (true loopback gRPC);
/// cross-worker is the original DoExchange behaviour. The
/// In-process `ExchangeSinkOp` short-circuit was removed in PR2;
/// only the Flight path remains.
///
/// Mutates every `OpSpec` named `ExchangeSinkRpc` in place.
/// Returns the count rewritten (0 when none present, e.g. for
/// non-aggregate queries).
///
/// Semantics:
/// - `n_partitions` is the stage1 partition count from
///   `dag.stages[1].partition_count`.
/// - For partition p, the rewritten address is
///   `stage1_flight_addrs[p % stage1_flight_addrs.len()]` when
///   non-empty, else the empty string. The existing worker factory
///   (B-2 / B-3.5) enforces `flight_addrs.len() == descs.len()` on
///   parse, so an empty `stage1_flight_addrs` produces a length
///   mismatch at the worker (a clear diagnostic) rather than a
///   silent zero-byte error.
fn rewrite_exchange_targets_in_place(
    ops: &mut [pylon_proto::pylon::OpSpec],
    n_partitions: usize,
    stage1_flight_addrs: &[String],
) -> usize {
    let target_value: String = if stage1_flight_addrs.is_empty() {
        // n_workers = max(1, len) in the fragmenter; here we mirror
        // that contract — an empty list still produces a
        // syntactically valid semicolon-joined string for the
        // length-mismatch check downstream.
        std::iter::repeat_n(String::new(), n_partitions)
            .collect::<Vec<_>>()
            .join(";")
    } else {
        (0..n_partitions)
            .map(|p| stage1_flight_addrs[p % stage1_flight_addrs.len()].clone())
            .collect::<Vec<_>>()
            .join(";")
    };
    let mut count = 0;
    for op in ops.iter_mut() {
        if op.name == "ExchangeSinkRpc" {
            op.config
                .insert("target_flight_addrs".to_string(), target_value.clone());
            count += 1;
        }
    }
    count
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

pub struct CoordGrpc {
    state: Arc<CoordState>,
}

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
            return Err(tonic::Status::invalid_argument("flight_addr is required"));
        }
        let worker_id = self
            .state
            .worker_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let reg = self
            .state
            .discovery
            .register(worker_id, req.flight_addr, req.grpc_addr);
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
        let pre_registered = header_worker_id.and_then(|id| self.state.discovery.lookup(id));

        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<TaskRequest>(16);

        let (worker_id, flight_addr) = match pre_registered {
            Some(reg) => {
                info!(?peer, registered_worker_id = reg.worker_id, flight_addr = %reg.flight_addr, "worker connected (registered)");
                (WorkerId(reg.worker_id), Some(reg.flight_addr))
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
        lock_ok(&self.state.workers).insert(worker_id, handle);
        info!(worker_id = worker_id.0, "registered");

        // Capture handles the spawned inbound task needs without
        // borrowing `&self` (which can't cross the `'static`
        // async-move boundary).
        let state = self.state.clone();
        tokio::spawn(async move {
            while let Some(msg) = inbound.next().await {
                match msg {
                    Ok(resp) => {
                        let tid = resp.task_id;
                        // M3-tail #1 (RFC 0005 R7): drive the
                        // coord's QueryStateMachine off the worker's
                        // existing TaskResponse.state field. No
                        // proto change; the worker already publishes
                        // TASK_DONE / TASK_FAILED on every task; we
                        // just count and wake.
                        let loc = lock_ok(&state.task_locs).get(&tid).copied();
                        match (resp.state, loc) {
                            (1, Some((qid, sid))) => {
                                state.state_machine.ack_task(
                                    qid,
                                    sid,
                                    pylon_coord::TaskId(tid),
                                    pylon_coord::query_state::TaskAck::Done,
                                );
                                trace!(
                                    worker = worker_id.0,
                                    task_id = tid,
                                    qid = qid.0,
                                    stage_id = sid.0,
                                    "QSM ack: done"
                                );
                            }
                            (2, Some((qid, sid))) => {
                                let msg = resp.message.clone();
                                state.state_machine.ack_task(
                                    qid,
                                    sid,
                                    pylon_coord::TaskId(tid),
                                    pylon_coord::query_state::TaskAck::Failed,
                                );
                                warn!(
                                    worker = worker_id.0, task_id = tid,
                                    qid = qid.0, stage_id = sid.0, %msg,
                                    "QSM ack: failed"
                                );
                            }
                            (4, Some((qid, sid))) => {
                                // RFC 0007 §3.5: recoverable spill
                                // boundary. Record the handle; the
                                // dispatcher reads it back via
                                // stalled_handles() to re-dispatch.
                                let key = resp.spill_handle.clone();
                                state.state_machine.ack_task(
                                    qid,
                                    sid,
                                    pylon_coord::TaskId(tid),
                                    pylon_coord::query_state::TaskAck::Stalled {
                                        spill_handle: pylon_runtime::spill::SpillHandle {
                                            path: std::path::PathBuf::from(key),
                                            bytes: 0,
                                            seq: 0,
                                        },
                                    },
                                );
                                info!(
                                    worker = worker_id.0,
                                    task_id = tid,
                                    qid = qid.0,
                                    stage_id = sid.0,
                                    "QSM ack: stalled"
                                );
                            }
                            _ => {
                                // Either state is RUNNING/CANCELLED
                                // (no ack) or task_id was never
                                // registered by a dispatch step
                                // (rare; ignore).
                            }
                        }
                        // M3 B-3.5: decode the real Arrow IPC streaming
                        // bytes from the worker. A single response
                        // carries one full IPC stream (schema + N
                        // batches + EOS), so we may decode multiple
                        // RecordBatches per response.
                        if !resp.batch.is_empty() {
                            match decode_ipc_stream(&resp.batch) {
                                Ok(batches) if !batches.is_empty() => {
                                    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
                                    debug!(
                                        worker = worker_id.0,
                                        task_id = tid,
                                        batches = batches.len(),
                                        rows = n,
                                        "decoded IPC stream"
                                    );
                                    completed
                                        .lock()
                                        .unwrap()
                                        .entry(tid)
                                        .or_default()
                                        .extend(batches.clone());
                                    let stored = completed
                                        .lock()
                                        .unwrap()
                                        .get(&tid)
                                        .map(|v| v.len())
                                        .unwrap_or(0);
                                    debug!(
                                        worker = worker_id.0,
                                        task_id = tid,
                                        stored_batches = stored,
                                        "stored in completed"
                                    );
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

        Ok(Response::new(SessionOutStream {
            inner: ReceiverStream::new(rx),
        }))
    }
}

/// Decode an Arrow IPC streaming payload (schema + N RecordBatch
/// messages + EOS) into the contained RecordBatches. M3 B-3.5.
fn decode_ipc_stream(bytes: &[u8]) -> anyhow::Result<Vec<arrow_array::RecordBatch>> {
    use arrow_ipc::reader::StreamReader;
    let cursor = std::io::Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

pub struct SessionOutStream {
    inner: ReceiverStream<TaskRequest>,
}

impl futures::Stream for SessionOutStream {
    type Item = Result<TaskRequest, Status>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::pin::Pin;
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|opt| opt.map(Ok))
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

#[cfg(test)]
mod b3_rewrite_tests {
    use super::rewrite_exchange_targets_in_place;
    use pylon_proto::pylon::OpSpec as OpSpecMsg;
    use std::collections::HashMap;

    fn op(name: &str) -> OpSpecMsg {
        OpSpecMsg {
            name: name.to_string(),
            config: HashMap::new(),
        }
    }

    fn op_with_config(name: &str, pairs: &[(&str, &str)]) -> OpSpecMsg {
        let mut o = op(name);
        for (k, v) in pairs {
            o.config.insert((*k).to_string(), (*v).to_string());
        }
        o
    }

    #[test]
    fn rewrites_exchange_sink_rpc_with_full_addr_list() {
        let mut ops = vec![op_with_config(
            "ExchangeSinkRpc",
            &[
                ("descriptors", "d0;d1;d2;d3"),
                ("target_flight_addrs", "WRONG;WRONG;WRONG;WRONG"),
            ],
        )];
        let addrs = vec!["a".to_string(), "b".to_string()];
        let n = rewrite_exchange_targets_in_place(&mut ops, 4, &addrs);
        assert_eq!(n, 1);
        // 4 partitions, 2 workers, round-robin
        assert_eq!(ops[0].config.get("target_flight_addrs").unwrap(), "a;b;a;b");
        // other keys preserved
        assert_eq!(ops[0].config.get("descriptors").unwrap(), "d0;d1;d2;d3");
    }

    #[test]
    fn no_op_when_no_exchange_sink_rpc() {
        let mut ops = vec![op("SeqScan"), op("Filter")];
        let addrs = vec!["a".to_string()];
        let n = rewrite_exchange_targets_in_place(&mut ops, 4, &addrs);
        assert_eq!(n, 0);
    }

    #[test]
    fn empty_addr_list_emits_semicolon_padding() {
        let mut ops = vec![op("ExchangeSinkRpc")];
        let n = rewrite_exchange_targets_in_place(&mut ops, 3, &[]);
        assert_eq!(n, 1);
        assert_eq!(ops[0].config.get("target_flight_addrs").unwrap(), ";;");
    }

    #[test]
    fn rewrites_every_exchange_sink_rpc_op() {
        let mut ops = vec![
            op("SeqScan"),
            op_with_config("ExchangeSinkRpc", &[("descriptors", "d0;d1")]),
            op_with_config("ExchangeSinkRpc", &[("descriptors", "d2")]),
        ];
        let addrs = vec!["a".to_string()];
        let n = rewrite_exchange_targets_in_place(&mut ops, 2, &addrs);
        assert_eq!(n, 2);
        for op in &ops[1..] {
            assert_eq!(op.config.get("target_flight_addrs").unwrap(), "a;a");
        }
    }

    #[test]
    fn single_partition_single_worker() {
        let mut ops = vec![op("ExchangeSinkRpc")];
        let addrs = vec!["only".to_string()];
        let n = rewrite_exchange_targets_in_place(&mut ops, 1, &addrs);
        assert_eq!(n, 1);
        assert_eq!(ops[0].config.get("target_flight_addrs").unwrap(), "only");
    }

    #[test]
    fn more_partitions_than_workers_wraps_modulo() {
        let mut ops = vec![op("ExchangeSinkRpc")];
        let addrs = vec!["x".to_string(), "y".to_string(), "z".to_string()];
        let n = rewrite_exchange_targets_in_place(&mut ops, 7, &addrs);
        assert_eq!(n, 1);
        assert_eq!(
            ops[0].config.get("target_flight_addrs").unwrap(),
            "x;y;z;x;y;z;x"
        );
    }
}
