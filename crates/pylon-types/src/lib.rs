//! Basic shared types for the Pylon query engine.

pub mod error;

pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema, SchemaRef};
pub use error::{PylonError, Result};
