//! Project operator: select columns by name.

use crate::op::PipelineOp;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use pylon_types::Result;
use std::sync::Arc;
use tracing::trace;

pub struct ProjectOp {
    pub col_names: Vec<String>,
    pub input_buf: Vec<RecordBatch>,
    pub output_schema: Arc<Schema>,
    pub upstream_done: bool,
    pub first_batch_seen: bool,
}

impl ProjectOp {
    pub fn new(col_names: Vec<String>, output_schema: Arc<Schema>) -> Self {
        // M2: if output_schema is empty, infer from input data lazily
        // This is set later when first_batch arrives; placeholder for now.
        if output_schema.fields().is_empty() {
            // placeholder; will be derived in add_input
        }
        Self {
            col_names,
            input_buf: Vec::new(),
            output_schema,
            upstream_done: false,
            first_batch_seen: false,
        }
    }

    fn do_project(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        // If output_schema is empty, derive it on first batch:
        if !self.first_batch_seen {
            self.first_batch_seen = true;
            if self.output_schema.fields().is_empty() {
                let new_fields: Vec<_> = self.col_names.iter()
                    .map(|name| batch.schema().field_with_name(name)
                        .cloned()
                        .unwrap_or_else(|_| {
                            arrow_schema::Field::new(name, arrow_schema::DataType::Null, true)
                        }))
                    .collect();
                self.output_schema = Arc::new(Schema::new(new_fields));
            }
        }

        let mut arrays = Vec::with_capacity(self.col_names.len());
        let in_schema = batch.schema();
        for name in &self.col_names {
            let idx = in_schema.fields().iter().position(|f| f.name() == name)
                .ok_or_else(|| pylon_types::PylonError::InvalidPlan(
                    format!("projection: column {name} not found")
                ))?;
            arrays.push(batch.column(idx).clone());
        }
        let projected = RecordBatch::try_new(self.output_schema.clone(), arrays)?;
        Ok(projected)
    }
}

#[async_trait]
impl PipelineOp for ProjectOp {
    fn name(&self) -> &'static str {
        "Project"
    }

    async fn add_input(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() > 0 {
            let p = self.do_project(&batch)?;
            self.input_buf.push(p);
        }
        Ok(())
    }

    async fn get_output(&mut self) -> Result<Option<RecordBatch>> {
        if self.input_buf.is_empty() {
            Ok(None)
        } else {
            let next = self.input_buf.remove(0);
            trace!("Project emits batch of {} rows", next.num_rows());
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
