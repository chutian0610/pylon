//! Engine-internal catalog interfaces used during planning.

use arrow_schema::SchemaRef;
use pylon_types::PylonError;

/// Resolves table schemas for the SQL planner.
pub trait SchemaProvider: Send + Sync {
    /// Returns the Arrow schema for `table`.
    fn get_schema(&self, table: &str) -> Result<SchemaRef, PylonError>;
}
