//! Exchange operators — M3 Flight-style producer/consumer pair.
//!
//! `ExchangeSinkOp` accepts RecordBatches and (in M3 first cut) writes them
//! into an in-process keyed accumulator. The "flight over the wire" half
//! lives in `pylon-exchange::PylonFlightService` which the worker process
//! populates from this op's batches.
//!
//! `ExchangeSourceOp` reverses the direction: when the driver poll's
//! `get_output`, it pulls the next batch from the same keyed accumulator.

use crate::op::PipelineOp;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use pylon_exchange::{FlightDescriptor, PylonFlightService};
use pylon_types::Result;
use std::sync::Arc;
use tracing::trace;

/// Producer operator: collects batches into the flight service under
/// a per-task descriptor. Real Flight RPC transport is in M3 task #4+.
pub struct ExchangeSinkOp {
    pub descriptor: FlightDescriptor,
    pub service: Arc<PylonFlightService>,
    pub input_buf: Vec<RecordBatch>,
    pub upstream_done: bool,
}

impl ExchangeSinkOp {
    pub fn new(descriptor: FlightDescriptor, service: Arc<PylonFlightService>) -> Self {
        Self {
            descriptor,
            service,
            input_buf: Vec::new(),
            upstream_done: false,
        }
    }
}

#[async_trait]
impl PipelineOp for ExchangeSinkOp {
    fn name(&self) -> &'static str {
        "ExchangeSink"
    }

    async fn add_input(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() > 0 {
            // For M3 first cut we route the batch directly through the
            // in-process PylonFlightService; later we serialise via Arrow
            // IPC streaming + push to a peer worker over gRPC.
            self.service.push(&self.descriptor, batch.clone()).await?;
            trace!(
                rows = batch.num_rows(),
                desc = %self.descriptor.as_str(),
                "ExchangeSink forwarded batch"
            );
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
}

impl ExchangeSourceOp {
    pub fn new(descriptor: FlightDescriptor, service: Arc<PylonFlightService>) -> Self {
        Self {
            descriptor,
            service,
            input_buf: Vec::new(),
            upstream_done: false,
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
        // Stage 0 of M3 first cut: in-process service; we drain one batch
        // per poll. Multiple batches per call wouldn't break the chain.
        if let Some(b) = self.input_buf.pop() {
            return Ok(Some(b));
        }
        // Pull from service if marked ended, drain remaining batches.
        loop {
            match self.service.pop(&self.descriptor).await? {
                Some(b) => return Ok(Some(b)),
                None => {
                    if self.upstream_done {
                        return Ok(None);
                    }
                    tokio::task::yield_now().await;
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
