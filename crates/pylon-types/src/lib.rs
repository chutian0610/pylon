//! Basic shared types for the Pylon query engine.

pub mod error;
pub mod stream;

pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema, SchemaRef};
pub use error::{ConnectorError, ConnectorErrorCode, PylonError, Result};
pub use stream::{RecordBatchStream, SendableRecordBatchStream};
