//! Basic shared types for the Pylon query engine.

pub mod codec;
pub mod error;
pub mod memory_pool;
pub mod stream;

pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema, SchemaRef};
pub use error::{ConnectorError, ConnectorErrorCode, PylonError, Result};
pub use memory_pool::MemoryPool;
pub use stream::{RecordBatchStream, SendableRecordBatchStream};
