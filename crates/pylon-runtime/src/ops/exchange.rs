//! Exchange operators — M3 Flight-style producer/consumer pair.
//!
//! `ExchangeSinkOp` accepts RecordBatches and routes them into the
//! in-process keyed accumulator (`PylonFlightService`). The "flight
//! over the wire" half lives in `pylon-exchange::PylonFlightService`
//! which the worker process populates from this op's batches.
//!
//! M3 A2-1: when constructed via `new_partitioned(...)` the sink
//! routes each row by `hash(partition_keys_col_values) % n_partitions`
//! to one of `n_partitions` Flight descriptors. This is the
//! HashPartitionExchange sender half — the fragmenter emits one
//! `ExchangeSink` op per stage0 task with N target descriptors and a
//! set of partition-key column names.
//!
//! `ExchangeSourceOp` reverses the direction: when the driver polls
//! `get_output`, it pulls the next batch from the same keyed
//! accumulator. Each `ExchangeSourceOp` has a single descriptor (per
//! task, per partition) and only reads from that queue.

use crate::op::PipelineOp;
use arrow_array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use async_trait::async_trait;
use pylon_exchange::{FlightDescriptor, PylonFlightService};
use pylon_types::{PylonError, Result};
use std::sync::Arc;
use tracing::trace;

/// Producer operator: collects batches into the flight service.
///
/// Two construction modes:
/// - `new(desc, service)` — single-descriptor sink. Every row goes to
///   `desc`. Used for A1 / 1-stage pipelines and for non-partitioned
///   exchanges.
/// - `new_partitioned(descriptors, partition_keys, service)` — multi-
///   descriptor sink. Each row is routed by
///   `hash(partition_keys_col_values) % descriptors.len()` to one
///   of the descriptors. Used for HashPartitionExchange.
pub struct ExchangeSinkOp {
    service: Arc<PylonFlightService>,
    /// Single-descriptor mode. `descriptors.is_empty()` iff partition_keys set.
    single: Option<FlightDescriptor>,
    /// Partitioned mode. `partition_keys` is non-empty in this mode.
    descriptors: Vec<FlightDescriptor>,
    partition_keys: Vec<String>,
    /// Column indices of `partition_keys` in the input batch, resolved
    /// on first add_input. `None` until resolved.
    partition_key_indices: Option<Vec<usize>>,
    /// Per-row hash state to amortize per-batch setup.
    input_buf: Vec<RecordBatch>,
    upstream_done: bool,
}

impl ExchangeSinkOp {
    /// Single-descriptor sink (A1 behavior).
    pub fn new(descriptor: FlightDescriptor, service: Arc<PylonFlightService>) -> Self {
        Self {
            service,
            single: Some(descriptor),
            descriptors: Vec::new(),
            partition_keys: Vec::new(),
            partition_key_indices: None,
            input_buf: Vec::new(),
            upstream_done: false,
        }
    }

    /// Partitioned sink. `descriptors.len()` is the number of
    /// downstream partitions; the op hashes `partition_keys` (column
    /// names from the input batch) and routes per row.
    pub fn new_partitioned(
        descriptors: Vec<FlightDescriptor>,
        partition_keys: Vec<String>,
        service: Arc<PylonFlightService>,
    ) -> Self {
        assert!(!descriptors.is_empty(), "partitioned sink needs ≥1 descriptor");
        assert!(
            !partition_keys.is_empty(),
            "partitioned sink needs ≥1 partition key"
        );
        Self {
            service,
            single: None,
            descriptors,
            partition_keys,
            partition_key_indices: None,
            input_buf: Vec::new(),
            upstream_done: false,
        }
    }

    /// Returns true if this sink is in partitioned mode.
    pub fn is_partitioned(&self) -> bool {
        self.single.is_none()
    }

    /// Resolve partition_key column indices in the input batch. Idempotent.
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
                        "ExchangeSink partition_key column {name} not found in input"
                    ))
                })?;
            indices.push(idx);
        }
        self.partition_key_indices = Some(indices);
        Ok(())
    }

    /// Compute the partition index for a single row, given the
    /// partition-key column arrays. Hash is a simple FNV-1a-style mix
    /// — stable across runs (good for tests) and fast. The result
    /// is `usize::from(hash) % n_partitions`.
    fn partition_for_row(
        row: usize,
        key_arrays: &[&dyn Array],
        n_partitions: usize,
    ) -> usize {
        // Combine column values with a tiny fold. We don't use
        // `DefaultHasher` because its output isn't stable across Rust
        // versions, which would make E2E tests flaky.
        let mut h: u64 = 0xcbf29ce484222325; // FNV offset basis
        for arr in key_arrays {
            fold_array_into_hash(*arr, row, &mut h);
        }
        (h as usize) % n_partitions
    }

    /// Slice a single batch by row indices (one bucket). Used to
    /// fan a batch out to per-partition batches. Returns the input
    /// itself when `indices` covers every row in order (avoids the
    /// overhead of arrow_select::take on the common case).
    fn slice_batch(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
        if indices.len() == batch.num_rows() {
            // Fast path: every row goes to this partition, keep the
            // original batch.
            return Ok(batch.clone());
        }
        let idx_array = UInt32Array::from(indices.to_vec());
        let columns: Vec<Arc<dyn Array>> = batch
            .columns()
            .iter()
            .map(|c| arrow_select::take::take(c.as_ref(), &idx_array, None))
            .collect::<std::result::Result<_, _>>()?;
        RecordBatch::try_new(batch.schema(), columns).map_err(Into::into)
    }
}

/// Fold a single cell of an Arrow array into a u64 hash. Supports the
/// types we currently use for group-by keys (Int64, Utf8, Float64,
/// UInt32, UInt64). Other types are hashed by their debug string —
/// good enough for M3 first cut, replaced by a proper visitor in M4.
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
            let v = arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row);
            *h = fold_u64(*h, v as u64);
        }
        DataType::Float64 => {
            let v = arr.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
            *h = fold_u64(*h, v.to_bits());
        }
        DataType::UInt32 => {
            let v = arr.as_any().downcast_ref::<UInt32Array>().unwrap().value(row) as u64;
            *h = fold_u64(*h, v);
        }
        DataType::UInt64 => {
            let v = arr.as_any().downcast_ref::<UInt64Array>().unwrap().value(row);
            *h = fold_u64(*h, v);
        }
        DataType::Utf8 => {
            let v = arr.as_any().downcast_ref::<StringArray>().unwrap().value(row);
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

#[async_trait]
impl PipelineOp for ExchangeSinkOp {
    fn name(&self) -> &'static str {
        "ExchangeSink"
    }

    async fn add_input(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if let Some(desc) = &self.single {
            // Single-descriptor mode: push the whole batch to one queue.
            self.service.push(desc, batch).await?;
            return Ok(());
        }

        // Partitioned mode: per-row hash routing.
        self.resolve_partition_keys(&batch)?;
        let indices = self
            .partition_key_indices
            .as_ref()
            .expect("resolve_partition_keys just set this");
        let key_arrays: Vec<&dyn Array> = indices
            .iter()
            .map(|&i| batch.column(i).as_ref())
            .collect();
        let n_partitions = self.descriptors.len();
        let n_rows = batch.num_rows();

        // Bucket rows by partition.
        let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); n_partitions];
        for row in 0..n_rows {
            let p = Self::partition_for_row(row, &key_arrays, n_partitions);
            buckets[p].push(row as u32);
        }

        // For each non-empty bucket, slice the batch and push to the
        // matching descriptor.
        for (p, idxs) in buckets.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let part_batch = Self::slice_batch(&batch, &idxs)?;
            trace!(
                rows = part_batch.num_rows(),
                partition = p,
                desc = %self.descriptors[p].as_str(),
                "ExchangeSink partitioned push"
            );
            self.service.push(&self.descriptors[p], part_batch).await?;
        }
        Ok(())
    }

    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None) // sink has no output batches
    }

    async fn no_more_input(&mut self) -> Result<()> {
        self.upstream_done = true;
        Ok(())
    }

    async fn is_finished(&self) -> bool {
        self.upstream_done && self.input_buf.is_empty()
    }
}

/// Consumer operator: pulls batches from the flight service keyed by its
/// descriptor. In a multi-stage query the descriptor points to another
/// task's `ExchangeSinkOp` output.
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
}

impl ExchangeSourceOp {
    pub fn new(descriptor: FlightDescriptor, service: Arc<PylonFlightService>) -> Self {
        Self {
            descriptor,
            service,
            input_buf: Vec::new(),
            upstream_done: false,
            empty_polls: 0,
            producer_done_threshold: 5,  // M3 heuristic
        }
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
        let pending = self.service.pending(&self.descriptor).await;
        self.upstream_done && self.input_buf.is_empty() && pending == 0
    }
}
