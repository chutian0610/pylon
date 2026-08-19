use std::pin::Pin;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;

use crate::Result;

/// A poll-based stream of Arrow batches with a stable output schema.
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    /// Returns the schema shared by every batch in the stream.
    fn schema(&self) -> SchemaRef;
}

/// An owned, sendable record-batch stream used across engine boundaries.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;
