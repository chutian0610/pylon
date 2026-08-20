use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// A stable Arrow page passed across the connector boundary.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConnectorPage {
    batch: RecordBatch,
}

impl ConnectorPage {
    /// Wraps an Arrow record batch as a connector page.
    pub fn new(batch: RecordBatch) -> Self {
        Self { batch }
    }

    /// Returns the page schema.
    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    /// Returns the number of rows in this page.
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    /// Borrows the underlying Arrow record batch.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Consumes the page and returns its Arrow record batch.
    pub fn into_batch(self) -> RecordBatch {
        self.batch
    }
}
