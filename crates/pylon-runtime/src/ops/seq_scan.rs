//! Sequence-scan operator: reads Parquet rows into RecordBatches.
//!
//! For M1: synchronous read of the entire file into memory, then streamed
//! in batch_size chunks.

use crate::op::PipelineOp;
use async_trait::async_trait;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pylon_types::{PylonError, Result};
use std::fs::File;
use tracing::debug;

pub struct SeqScanOp {
    pub path: String,
    pub batches: Vec<RecordBatch>,
    pub next: usize,
    pub batch_size: usize,
}

impl SeqScanOp {
    pub fn new(path: String, batch_size: usize) -> Self {
        Self {
            path,
            batches: Vec::new(),
            next: 0,
            batch_size,
        }
    }
}

#[async_trait]
impl PipelineOp for SeqScanOp {
    fn name(&self) -> &'static str {
        "SeqScan"
    }

    async fn needs_input(&self) -> bool {
        false
    }

    async fn is_finished(&self) -> bool {
        self.next >= self.batches.len()
    }

    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        if self.batches.is_empty() {
            let file = File::open(&self.path)?;
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
                    PylonError::Parquet(format!("build reader: {e}"))
                })?;
            let reader = builder.with_batch_size(self.batch_size).build().map_err(|e| {
                PylonError::Parquet(format!("with batch size: {e}"))
            })?;
            for batch in reader {
                self.batches.push(batch.map_err(|e| {
                    PylonError::Parquet(format!("read batch: {e}"))
                })?);
            }
            debug!("SeqScan: loaded {} batches from {}", self.batches.len(), self.path);
        }
        if let Some(b) = self.batches.get(self.next).cloned() {
            self.next += 1;
            Ok(Some(b))
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.batches.clear();
        self.batches.shrink_to_fit();
        Ok(())
    }
}
