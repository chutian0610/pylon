//! Exchange operators — Arrow Flight producer/consumer pair.
//!
//! M3+ unified path: there is exactly one producer (`ExchangeSinkRpc`)
//! and one consumer (`ExchangeSourceOp`). The producer always uses
//! Arrow Flight `DoExchange` to push batches to a target worker's
//! `PylonFlightService`; same-worker fan-out is naturally expressed
//! as a loopback gRPC target (`target.flight_addr == local_addr`)
//! and goes through the same code path as cross-worker.
//!
//! Pre-M3, an in-process `ExchangeSinkOp` short-circuit coexisted
//! with `ExchangeSinkRpc`. The dispatcher's authoritative
//! `target_flight_addrs` rewrite (PR1 in
//! `docs/roadmap/m3-tail-exchange-unify.md`) and the fragmenter's
//! single-mode emission (PR2) collapse both into the Flight path.
//!
//! Routing (per row): `hash(partition_keys_col_values) %
//! n_targets` matches the same name to the same partition
//! regardless of transport, so an ExchangeSource on worker W sees
//! exactly the rows the matching ExchangeSinkRpc sent to W.

use crate::op::PipelineOp;
use arrow_array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use async_trait::async_trait;
use pylon_exchange::{FlightDescriptor, PylonFlightService};
use pylon_types::{PylonError, Result};
use std::sync::Arc;

/// Fold a single cell of an Arrow array into a u64 hash. Supports the
/// types we currently use for group-by keys (Int64, Utf8, Float64,
/// UInt32, UInt64). Other types are hashed by their debug string —
/// good enough for M3 first cut, replaced by a proper visitor in M4.
/// Pure (sync) per-row partition computation. Returns
/// `partition_index[row]` for each row. FNV-1a mix consistent
/// with `ExchangeSink` (A2).
fn compute_partitions(batch: &RecordBatch, indices: &[usize], n_partitions: usize) -> Vec<usize> {
    let n_rows = batch.num_rows();
    let mut out = Vec::with_capacity(n_rows);
    for row in 0..n_rows {
        let mut h: u64 = 0xcbf29ce484222325;
        for &col_idx in indices {
            let arr = batch.column(col_idx).as_ref();
            fold_array_into_hash(arr, row, &mut h);
        }
        out.push((h as usize) % n_partitions);
    }
    out
}

fn fold_array_into_hash(arr: &dyn Array, row: usize, h: &mut u64) {
    if arr.is_null(row) {
        // NULL → 0xdeadbeef sentinel; never collides with valid values
        // for the supported types below.
        *h ^= 0xdeadbeefu64;
        *h = h.wrapping_mul(0x100000001b3); // FNV prime
        return;
    }
    match arr.data_type() {
        DataType::Int64 => {
            let v = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row);
            *h = fold_u64(*h, v as u64);
        }
        DataType::Float64 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            *h = fold_u64(*h, v.to_bits());
        }
        DataType::UInt32 => {
            let v = arr
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row) as u64;
            *h = fold_u64(*h, v);
        }
        DataType::UInt64 => {
            let v = arr
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row);
            *h = fold_u64(*h, v);
        }
        DataType::Utf8 => {
            let v = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row);
            for &b in v.as_bytes() {
                *h ^= b as u64;
                *h = h.wrapping_mul(0x100000001b3);
            }
        }
        other => {
            // Fallback: hash the type's debug string + a row tag. Good
            // enough for M3 first cut; not stable across types so the
            // caller should restrict to a single type per sink.
            let tag = format!("{other:?}");
            for &b in tag.as_bytes() {
                *h ^= b as u64;
                *h = h.wrapping_mul(0x100000001b3);
            }
        }
    }
}

fn fold_u64(mut h: u64, v: u64) -> u64 {
    h ^= v;
    h = h.wrapping_mul(0x100000001b3);
    h
}

/// Consumer operator: pulls batches from the flight service keyed by its
/// descriptor. In a multi-stage query the descriptor points to another
/// task's `ExchangeSinkRpc` output (the staging queue lives on
/// the target worker's `PylonFlightService`, regardless of whether
/// the producer is loopback or remote).
pub struct ExchangeSourceOp {
    pub descriptor: FlightDescriptor,
    pub service: Arc<PylonFlightService>,
    pub input_buf: Vec<RecordBatch>,
    pub upstream_done: bool,
    /// M3 heuristic: counter of empty pops. After `producer_done_threshold`
    /// empty polls, the source treats the producer as done (no more data will arrive).
    /// M4+ replaces this with explicit Flight FIN signal.
    empty_polls: u32,
    producer_done_threshold: u32,
    /// FTE source (RFC 0007): when set, input comes from a persisted
    /// exchange log (survives worker loss) instead of the drain-once
    /// flight queue. Served FIFO via `VecDeque`.
    log_backed: Option<std::collections::VecDeque<RecordBatch>>,
}

impl ExchangeSourceOp {
    pub fn new(descriptor: FlightDescriptor, service: Arc<PylonFlightService>) -> Self {
        Self {
            descriptor,
            service,
            input_buf: Vec::new(),
            upstream_done: false,
            empty_polls: 0,
            producer_done_threshold: 5, // M3 heuristic
            log_backed: None,
        }
    }

    /// FTE source: constructs a source that replays a persisted
    /// exchange-input log (written by the flight server's do_exchange
    /// on the partition-owner worker). The log is read eagerly; the
    /// drain-once queue is bypassed entirely.
    pub fn from_log(descriptor: FlightDescriptor, log_path: &std::path::Path) -> Result<Self> {
        let bytes = std::fs::read(log_path).map_err(|e| {
            pylon_types::PylonError::Io(std::io::Error::new(
                e.kind(),
                format!("reading input log {}: {e}", log_path.display()),
            ))
        })?;
        let batches = pylon_exchange::codec::read_concatenated_ipc(bytes)
            .map_err(|e| pylon_types::PylonError::Internal(format!("input log decode: {e}")))?;
        Ok(Self {
            descriptor,
            service: Arc::new(PylonFlightService::new()),
            input_buf: Vec::new(),
            upstream_done: false,
            empty_polls: 0,
            producer_done_threshold: 5,
            log_backed: Some(std::collections::VecDeque::from(batches)),
        })
    }
}

#[async_trait]
impl PipelineOp for ExchangeSourceOp {
    fn name(&self) -> &'static str {
        "ExchangeSource"
    }

    async fn needs_input(&self) -> bool {
        false // source op
    }

    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(log) = &mut self.log_backed {
            return Ok(log.pop_front());
        }
        if let Some(b) = self.input_buf.pop() {
            return Ok(Some(b));
        }
        // Pop with heuristic: if service is empty for N consecutive polls,
        // conclude the upstream producer is done and signal EOF.
        loop {
            match self.service.pop(&self.descriptor).await? {
                Some(b) => {
                    self.empty_polls = 0;
                    return Ok(Some(b));
                }
                None => {
                    self.empty_polls += 1;
                    // M3 fix: don't short-circuit on upstream_done — that's true from t=0
                    // for a source op, and would cause us to miss batches arriving from a
                    // later stage. Only the empty-poll threshold counts.
                    if self.empty_polls >= self.producer_done_threshold {
                        return Ok(None);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }

    async fn no_more_input(&mut self) -> Result<()> {
        self.upstream_done = true;
        Ok(())
    }

    async fn is_finished(&self) -> bool {
        if let Some(log) = &self.log_backed {
            return self.upstream_done && log.is_empty();
        }
        let pending = self.service.pending(&self.descriptor).await;
        self.upstream_done && self.input_buf.is_empty() && pending == 0
    }
}

// ============================================================================
// ExchangeSinkRpc — the single producer op, post-M3-tail unification
// ============================================================================
//
// Per-row hash routing over per-partition Flight targets. The only
// difference between a same-worker partition and a cross-worker one
// is the `RpcTarget.flight_addr`: same-worker uses the local Flight
// server's bound address (loopback gRPC); cross-worker uses the
// remote worker's registered address. The downstream behaviour
// (DoExchange → FlightServerImpl → PylonFlightService.push) is the
// same. The M3-tail exchange-unify PR (B3 + PR2) replaced the
// earlier in-process `ExchangeSinkOp` short-circuit with this
// single path.

use arrow_ipc::writer::StreamWriter;
use tracing::warn;

/// Per-partition target for `ExchangeSinkRpc`. The op opens a
/// `DoExchange` stream to `flight_addr` and tags all messages with
/// `descriptor` (in `app_metadata` on the first message).
#[derive(Debug, Clone)]
pub struct RpcTarget {
    pub flight_addr: String,
    pub descriptor: FlightDescriptor,
}

pub struct ExchangeSinkRpc {
    /// Per-partition routing target. `targets[i]` is where rows
    /// hashing to partition `i` go.
    targets: Vec<RpcTarget>,
    partition_keys: Vec<String>,
    /// Lazily resolved column indices (None until first add_input).
    partition_key_indices: Option<Vec<usize>>,
    upstream_done: bool,
    /// M4 FTE: in-flight DoExchange jobs. `no_more_input` joins them
    /// so the stage-0 TASK_DONE ack implies every target has finished
    /// processing (and, with FTE source, finished appending its
    /// persisted input log). Replaces the old fire-and-forget spawn
    /// + 500 ms sleep heuristic.
    inflight: Vec<tokio::task::JoinHandle<()>>,
}

impl ExchangeSinkRpc {
    /// Helper: send one batch via DoExchange. M3 first cut: open
    /// a fresh channel per call, no pooling.
    fn send_rpc_job(
        url: String,
        messages: Vec<arrow_flight::FlightData>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async move {
            // The do_exchange future's Send bound is implicit via the
            // outer `Pin<Box<dyn Future + Send>>`. Build the stream
            // inline so its type is inferred (annotation trips
            // async_trait's HRTB check).
            let channel = match tonic::transport::Channel::from_shared(url.clone()) {
                Ok(c) => match c.connect().await {
                    Ok(ch) => ch,
                    Err(e) => {
                        warn!("ExchangeSinkRpc connect {url}: {e}");
                        return;
                    }
                },
                Err(e) => {
                    warn!("ExchangeSinkRpc bad url {url}: {e}");
                    return;
                }
            };
            let mut client = arrow_flight::flight_service_client::FlightServiceClient::new(channel);
            let s = futures::stream::iter(messages);
            if let Err(e) = client.do_exchange(s).await {
                warn!("ExchangeSinkRpc do_exchange {url}: {e}");
            }
        })
    }
}

impl ExchangeSinkRpc {
    /// of downstream partitions; the op hashes `partition_keys` and
    /// routes per row.
    pub fn new_partitioned(targets: Vec<RpcTarget>, partition_keys: Vec<String>) -> Self {
        assert!(!targets.is_empty(), "ExchangeSinkRpc needs ≥1 target");
        assert!(
            !partition_keys.is_empty(),
            "ExchangeSinkRpc needs ≥1 partition key"
        );
        Self {
            targets,
            inflight: Vec::new(),
            partition_keys,
            partition_key_indices: None,
            upstream_done: false,
        }
    }

    fn resolve_partition_keys(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.partition_key_indices.is_some() {
            return Ok(());
        }
        let in_schema = batch.schema();
        let mut indices = Vec::with_capacity(self.partition_keys.len());
        for name in &self.partition_keys {
            let idx = in_schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                    PylonError::InvalidPlan(format!(
                        "ExchangeSinkRpc partition_key column {name} not found in input"
                    ))
                })?;
            indices.push(idx);
        }
        self.partition_key_indices = Some(indices);
        Ok(())
    }

    fn slice_batch(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
        if indices.len() == batch.num_rows() {
            return Ok(batch.clone());
        }
        let idx_array = arrow_array::UInt32Array::from(indices.to_vec());
        let columns: Vec<Arc<dyn arrow_array::Array>> = batch
            .columns()
            .iter()
            .map(|c| arrow_select::take::take(c.as_ref(), &idx_array, None))
            .collect::<std::result::Result<_, _>>()?;
        RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
    }
}

#[async_trait]
impl PipelineOp for ExchangeSinkRpc {
    fn name(&self) -> &'static str {
        "ExchangeSinkRpc"
    }

    async fn add_input(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // Resolve column indices first (sync, no await).
        self.resolve_partition_keys(&batch)?;
        // Clone the indices + targets up front so the async
        // portion below doesn't need to borrow `&mut self`.
        let indices: Vec<usize> = self
            .partition_key_indices
            .as_ref()
            .expect("resolve_partition_keys just set this")
            .clone();
        let targets: Vec<RpcTarget> = self.targets.clone();
        let n_partitions = targets.len();

        // Compute per-row partition index (sync helper, no async
        // borrows).
        let per_row_partition: Vec<usize> = compute_partitions(&batch, &indices, n_partitions);

        // Bucket rows by partition.
        let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); n_partitions];
        for (row, &p) in per_row_partition.iter().enumerate() {
            buckets[p].push(row as u32);
        }

        // Pre-build the per-partition (url, FlightData messages) so
        // the async block in tokio::spawn only contains the RPC
        // call (avoids async_trait's higher-ranked lifetime issues
        // with complex futures).
        let mut jobs: Vec<(String, Vec<arrow_flight::FlightData>)> = Vec::new();
        for (p, idxs) in buckets.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let part_batch = Self::slice_batch(&batch, &idxs)?;
            let target = targets[p].clone();
            let url = format!("http://{}", target.flight_addr);
            let mut buf: Vec<u8> = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, part_batch.schema().as_ref())
                    .map_err(|e| PylonError::Internal(format!("ipc writer: {e}")))?;
                writer
                    .write(&part_batch)
                    .map_err(|e| PylonError::Internal(format!("ipc write: {e}")))?;
                writer
                    .finish()
                    .map_err(|e| PylonError::Internal(format!("ipc finish: {e}")))?;
            }
            let desc_msg = arrow_flight::FlightData {
                flight_descriptor: None,
                app_metadata: tonic::codegen::Bytes::from(target.descriptor.0.clone()),
                data_body: tonic::codegen::Bytes::new(),
                data_header: tonic::codegen::Bytes::new(),
            };
            let body_msg = arrow_flight::FlightData {
                flight_descriptor: None,
                app_metadata: tonic::codegen::Bytes::new(),
                data_body: tonic::codegen::Bytes::from(buf),
                data_header: tonic::codegen::Bytes::new(),
            };
            jobs.push((url, vec![desc_msg, body_msg]));
        }
        drop(batch);

        // Dispatch each job in a spawned task. We Box::pin the
        // future as a `Pin<Box<dyn Future + Send>>` to force async_trait
        // to accept the Send bound (avoids higher-ranked lifetime
        // issues with bare `impl Future`).
        for (url, messages) in jobs {
            let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Self::send_rpc_job(url, messages);
            self.inflight.push(tokio::spawn(fut));
        }
        Ok(())
    }

    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None) // sink has no output batches
    }

    async fn no_more_input(&mut self) -> Result<()> {
        self.upstream_done = true;
        // FTE ordering guarantee: every DoExchange job (and therefore
        // every target-side input-log append + queue push) completes
        // before this op reports done. Stage 1 dispatches only after
        // this ack, so persisted logs are complete at dispatch time.
        let handles = std::mem::take(&mut self.inflight);
        for h in handles {
            h.await
                .map_err(|e| PylonError::Internal(format!("exchange rpc join: {e}")))?;
        }
        Ok(())
    }

    async fn is_finished(&self) -> bool {
        self.upstream_done
    }
}
