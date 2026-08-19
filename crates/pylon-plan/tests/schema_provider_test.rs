use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use pylon_plan::catalog::SchemaProvider;
use pylon_plan::logical::LogicalPlan;
use pylon_plan::translate::logical_from_sql;
use pylon_types::PylonError;

struct TestSchemaProvider {
    schema: SchemaRef,
}

impl SchemaProvider for TestSchemaProvider {
    fn get_schema(&self, table: &str) -> Result<SchemaRef, PylonError> {
        if table == "orders" {
            Ok(Arc::clone(&self.schema))
        } else {
            Err(PylonError::InvalidPlan(format!("table not found: {table}")))
        }
    }
}

#[test]
fn logical_planner_uses_schema_provider_trait() {
    let provider = TestSchemaProvider {
        schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
    };

    let plan = logical_from_sql("SELECT id FROM orders", &provider).unwrap();
    assert!(matches!(plan, LogicalPlan::Project { .. }));
}
