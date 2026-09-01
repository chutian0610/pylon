//! Filter operator: keep rows where predicate is true.
//!
//! M1 implements the predicate as `>`, `<`, `=`, etc. on Int64/Float64/Utf8
//! columns vs a literal value.

use crate::op::PipelineOp;
use arrow::compute::filter_record_batch;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use async_trait::async_trait;
use pylon_types::Result;
use tracing::trace;

pub struct FilterOp {
    pub col_name: String,
    pub op_str: String,
    pub literal: String,
    pub input_buf: Vec<RecordBatch>,
    pub upstream_done: bool,
}

impl FilterOp {
    pub fn new(col_name: String, op: String, literal: String) -> Self {
        Self {
            col_name,
            op_str: op,
            literal,
            input_buf: Vec::new(),
            upstream_done: false,
        }
    }

    fn matches(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == &self.col_name)
            .ok_or_else(|| {
                pylon_types::PylonError::InvalidPlan(format!(
                    "filter: column {} not found",
                    self.col_name
                ))
            })?;

        let col = batch.column(idx);
        let lit = &self.literal;
        let mask: Vec<bool> = match col.data_type() {
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                let lit_n: i64 = lit.parse().map_err(|_| {
                    pylon_types::PylonError::InvalidPlan(format!("filter literal not i64: {lit}"))
                })?;
                arr.iter()
                    .map(|v| v.map(|x| self.cmp_i64(x, lit_n)).unwrap_or(false))
                    .collect()
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                let lit_n: f64 = lit.parse().map_err(|_| {
                    pylon_types::PylonError::InvalidPlan(format!("filter literal not f64: {lit}"))
                })?;
                arr.iter()
                    .map(|v| v.map(|x| self.cmp_f64(x, lit_n)).unwrap_or(false))
                    .collect()
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                arr.iter()
                    .map(|v| v.map(|x| self.cmp_str(x, lit)).unwrap_or(false))
                    .collect()
            }
            _ => {
                return Err(pylon_types::PylonError::InvalidPlan(format!(
                    "filter on type {:?} not supported in M1",
                    col.data_type()
                )));
            }
        };

        let mask_arr = arrow_array::BooleanArray::from(mask);
        let filtered = filter_record_batch(batch, &mask_arr)?;
        Ok(filtered)
    }

    fn cmp_i64(&self, x: i64, lit: i64) -> bool {
        match self.op_str.as_str() {
            ">" => x > lit,
            "<" => x < lit,
            ">=" => x >= lit,
            "<=" => x <= lit,
            "=" => x == lit,
            "<>" => x != lit,
            _ => false,
        }
    }
    fn cmp_f64(&self, x: f64, lit: f64) -> bool {
        match self.op_str.as_str() {
            ">" => x > lit,
            "<" => x < lit,
            ">=" => x >= lit,
            "<=" => x <= lit,
            "=" => (x - lit).abs() < f64::EPSILON,
            "<>" => (x - lit).abs() >= f64::EPSILON,
            _ => false,
        }
    }
    fn cmp_str(&self, x: &str, lit: &str) -> bool {
        match self.op_str.as_str() {
            "=" => x == lit,
            "<>" => x != lit,
            _ => false, // M1: no lex compare yet
        }
    }
}

#[async_trait]
impl PipelineOp for FilterOp {
    fn name(&self) -> &'static str {
        "Filter"
    }

    async fn add_input(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() > 0 {
            let filtered = self.matches(&batch)?;
            if filtered.num_rows() > 0 {
                self.input_buf.push(filtered);
            }
        }
        Ok(())
    }

    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        if self.input_buf.is_empty() {
            Ok(None)
        } else {
            let next = self.input_buf.remove(0);
            trace!("Filter emits batch of {} rows", next.num_rows());
            Ok(Some(next))
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
