//! PartitionFilter — keep rows where (id % modulus == partition).
//!
//! M2 helper used by coord to partition data across workers:
//! each worker gets `partition = i % n`, and the operator emits only rows
//! whose `id` field satisfies the modular equality.

use crate::op::PipelineOp;
use arrow::compute::filter_record_batch;
use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch};
use async_trait::async_trait;
use pylon_types::Result;
use tracing::trace;

pub struct PartitionFilterOp {
    pub col_name: String,
    pub partition: i64,
    pub modulus: i64,
    pub input_buf: Vec<RecordBatch>,
    pub upstream_done: bool,
}

impl PartitionFilterOp {
    pub fn new(col_name: String, literal: &str) -> Result<Self> {
        let (p_str, n_str) = literal
            .split_once('|')
            .ok_or_else(|| pylon_types::PylonError::InvalidPlan(
                format!("PartitionFilter literal must be 'p|n', got: {literal}")
            ))?;
        let partition: i64 = p_str.parse().map_err(|e: std::num::ParseIntError|
            pylon_types::PylonError::InvalidPlan(format!("partition not int: {e}"))
        )?;
        let modulus: i64 = n_str.parse().map_err(|e: std::num::ParseIntError|
            pylon_types::PylonError::InvalidPlan(format!("modulus not int: {e}"))
        )?;
        Ok(Self {
            col_name,
            partition,
            modulus,
            input_buf: Vec::new(),
            upstream_done: false,
        })
    }
}

#[async_trait]
impl PipelineOp for PartitionFilterOp {
    fn name(&self) -> &'static str {
        "PartitionFilter"
    }

    async fn add_input(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 { return Ok(()); }
        // Find the column index
        let idx = batch.schema().fields().iter().position(|f| f.name() == &self.col_name)
            .ok_or_else(|| pylon_types::PylonError::InvalidPlan(
                format!("PartitionFilter: column {} not found", self.col_name)
            ))?;
        let col = batch.column(idx);
        let arr = col.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| pylon_types::PylonError::InvalidPlan(
                format!("PartitionFilter: column {} not int64", self.col_name)
            ))?;
        let mask: Vec<bool> = (0..batch.num_rows())
            .map(|r| arr.value(r) % self.modulus == self.partition)
            .collect();
        let bool_arr = BooleanArray::from(mask);
        let filtered = filter_record_batch(&batch, &bool_arr)?;
        if filtered.num_rows() > 0 {
            trace!("PartitionFilter emitted {} rows", filtered.num_rows());
            self.input_buf.push(filtered);
        }
        Ok(())
    }

    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        if self.input_buf.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.input_buf.remove(0)))
        }
    }

    async fn no_more_input(&mut self) -> Result<()> {
        self.upstream_done = true;
        Ok(())
    }

    async fn is_finished(&self) -> bool {
        self.upstream_done && self.input_buf.is_empty()
    }
}
