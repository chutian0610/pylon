//! `PhysicalExpr` trait + concrete impls (RFC 0005 § 4 role 2b).
//!
//! The pre-existing `enum crate::PhysicalExpr` stays in this module's
//! parent (`physical/mod.rs`) as a deprecated facade; new code uses
//! the trait + these structs. R2.2.a migrates `fragment.rs` to the
//! trait; until then, the structs are exercised only by the unit
//! tests below.
//!
//! The trait is `Send + Sync + Any` so it can live behind
//! `Arc<dyn PhysicalExpr>` in operator properties (e.g. the keys of
//! `Distribution::Hash`). Methods are pure (no `&mut self`) so the
//! same trait object can be evaluated many times in parallel.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, FieldRef, Schema};

use pylon_types::PylonError;

/// A compiled, eval-ready expression over a `RecordBatch`. The trait
/// is dyn-compatible so we can store `Arc<dyn PhysicalExpr>` in
/// operator properties.
pub trait PhysicalExpr: Send + Sync + std::fmt::Debug {
    /// Stable type name (used in `AggregateFunction::name`-like
    /// places; not to be confused with the *output field name*).
    /// Returns `String` instead of `&'static str` so the trait stays
    /// object-safe (`self.field.name()` borrows from self).
    fn name(&self) -> String;

    /// Output `DataType` of this expression for the given input
    /// schema. E.g. `Column { index: 0 }` → `schema.field(0).data_type()`.
    fn data_type(&self, schema: &Schema) -> Result<DataType, PylonError>;

    /// Whether the expression can ever evaluate to null. Used by
    /// downstream operators to short-circuit on cheap nullability
    /// checks without invoking `evaluate`.
    fn nullable(&self, schema: &Schema) -> Result<bool, PylonError>;

    /// Evaluate against a `RecordBatch`. Returns a columnar `Array`.
    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<ArrayRef, PylonError>;

    /// Optional: `Arrow Field` for the output of this expr. Defaults
    /// to `Field::new(self.name(), self.data_type())`; operators
    /// that need different nullability / metadata override.
    fn return_field(
        &self,
        schema: &Schema,
    ) -> Result<FieldRef, PylonError> {
        let f = arrow_schema::Field::new(self.name(), self.data_type(schema)?, self.nullable(schema)?);
        Ok(Arc::new(f))
    }

    /// `Any` for downcasting on the consumer side (mostly stats /
    /// optimizer — none required for R2.1).
    fn as_any(&self) -> &dyn std::any::Any;
}

/// A column reference. `index` is the position in the input schema.
#[derive(Debug, Clone)]
pub struct ColumnExpr {
    pub index: usize,
    pub field: arrow_schema::Field,
}

impl ColumnExpr {
    pub fn new(index: usize, field: arrow_schema::Field) -> Self {
        Self { index, field }
    }
}

impl PhysicalExpr for ColumnExpr {
    fn name(&self) -> String {
        self.field.name().to_string()
    }

    fn data_type(&self, _schema: &Schema) -> Result<DataType, PylonError> {
        Ok(self.field.data_type().clone())
    }

    fn nullable(&self, _schema: &Schema) -> Result<bool, PylonError> {
        Ok(self.field.is_nullable())
    }

    fn evaluate(&self, batch: &arrow_array::RecordBatch) -> Result<ArrayRef, PylonError> {
        Ok(batch.column(self.index).clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl From<ColumnExpr> for Arc<dyn PhysicalExpr> {
    fn from(c: ColumnExpr) -> Self {
        Arc::new(c)
    }
}

/// A literal scalar (rendered into one-row arrays). M3 only carries
/// string-encoded literals (sufficient for filter comparisons);
/// numeric / temporal literals land in M4 alongside
/// `PhysicalPlanner::create_physical_expr`.
#[derive(Debug, Clone)]
pub struct LiteralExpr {
    pub value: String,
    pub data_type: DataType,
}

impl LiteralExpr {
    pub fn new(value: impl Into<String>, data_type: DataType) -> Self {
        Self {
            value: value.into(),
            data_type,
        }
    }
}

impl PhysicalExpr for LiteralExpr {
    fn name(&self) -> String {
        "literal".to_string()
    }

    fn data_type(&self, _schema: &Schema) -> Result<DataType, PylonError> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _schema: &Schema) -> Result<bool, PylonError> {
        // Literals are non-null by construction; invalid paths
        // (NULL literal) become `LiteralExpr { value: "NULL",
        // data_type: Int64 }` and the planner rejects them earlier
        // when it can.
        Ok(false)
    }

    fn evaluate(&self, batch: &arrow_array::RecordBatch) -> Result<ArrayRef, PylonError> {
        let n = batch.num_rows();
        // Render the value into a length-n array via Arrow's
        // `Scalar` API. For now only String-typed; other types land
        // in M4 alongside the planner.
        if matches!(self.data_type, DataType::Utf8) {
            use arrow_array::StringArray;
            Ok(Arc::new(StringArray::from(vec![self.value.clone(); n])))
        } else {
            // Fallback: emit zeros and warn via the error type.
            Err(PylonError::Internal(format!(
                "LiteralExpr: type {:?} not yet supported in evaluate (M4)",
                self.data_type
            )))
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl From<LiteralExpr> for Arc<dyn PhysicalExpr> {
    fn from(c: LiteralExpr) -> Self {
        Arc::new(c)
    }
}

/// A binary op over two `PhysicalExpr` sub-expressions. `op` is a
/// lowercase symbolic string (e.g. `"="`, `">"`). The engine is
/// expected to use the arrow-eval kernel for actual evaluation; this
/// struct's `evaluate` is a placeholder that the filter operator
/// (M3 first cut) does not exercise (it calls `compute_kernels`
/// directly on the optimized plan). M4 plans a real binder pass.
#[derive(Debug, Clone)]
pub struct BinaryOpExpr {
    pub left: Arc<dyn PhysicalExpr>,
    pub op: String,
    pub right: Arc<dyn PhysicalExpr>,
}

impl BinaryOpExpr {
    pub fn new(
        left: impl Into<Arc<dyn PhysicalExpr>>,
        op: impl Into<String>,
        right: impl Into<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            left: left.into(),
            op: op.into(),
            right: right.into(),
        }
    }
}

impl PhysicalExpr for BinaryOpExpr {
    fn name(&self) -> String {
        "binary_op".to_string()
    }

    fn data_type(&self, schema: &Schema) -> Result<DataType, PylonError> {
        // The engine evaluates via arrow_compute::expr::ColumnarValue
        // directly from bound operators; this default stands in
        // until M4 wires the binder pass.
        let _ = schema;
        Ok(DataType::Boolean)
    }

    fn nullable(&self, schema: &Schema) -> Result<bool, PylonError> {
        let _ = schema;
        // `=` over nullable columns is nullable; the Filter op
        // handles three-valued logic at runtime.
        Ok(true)
    }

    fn evaluate(&self, batch: &arrow_array::RecordBatch) -> Result<ArrayRef, PylonError> {
        // M3 first cut: this is the dispatchable `PhysicalExpr`
        // path; actual filter evaluation lives in
        // `pylon_runtime::filter_record_batch`. Returning an error
        // here is "shouldn't be called" — the runtime always goes
        // through the optimized kernel.
        let _ = batch;
        Err(PylonError::Internal(
            "BinaryOpExpr::evaluate: not bound to a kernel in M3; \
             use pylon_runtime::filter_record_batch"
                .into(),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl From<BinaryOpExpr> for Arc<dyn PhysicalExpr> {
    fn from(c: BinaryOpExpr) -> Self {
        Arc::new(c)
    }
}

/// Aggregate function expression (`COUNT(*)`, `SUM(col)`, …). `args`
/// is empty for `count(*)`; otherwise one `PhysicalExpr` per arg
/// (typically a `ColumnExpr`).
#[derive(Debug, Clone)]
pub struct AggregateFunctionExpr {
    pub func: String,
    pub name: String,
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    pub data_type: DataType,
    pub input_data_types: Vec<DataType>,
}

impl AggregateFunctionExpr {
    pub fn new(
        func: impl Into<String>,
        name: impl Into<String>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        data_type: DataType,
        input_data_types: Vec<DataType>,
    ) -> Self {
        Self {
            func: func.into(),
            name: name.into(),
            args,
            data_type,
            input_data_types,
        }
    }
}

impl PhysicalExpr for AggregateFunctionExpr {
    fn name(&self) -> String {
        // Caller uses `AggregateFunction::name` for the *output field
        // name*; this `name()` is the stable operator-type tag.
        "aggregate_function".to_string()
    }

    fn data_type(&self, _schema: &Schema) -> Result<DataType, PylonError> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _schema: &Schema) -> Result<bool, PylonError> {
        // Aggregates are never null (COALESCE / FILTER semantics
        // aside, which arrive in M4).
        Ok(false)
    }

    fn evaluate(&self, batch: &arrow_array::RecordBatch) -> Result<ArrayRef, PylonError> {
        // M3 first cut: HashAggregateOp handles its own state
        // machine; the PhysicalExpr side only types checks.
        let _ = batch;
        Err(PylonError::Internal(
            "AggregateFunctionExpr::evaluate: HashAggregateOp has its own loop; \
             the kernel path lives in pylon_runtime::ops::aggregate"
                .into(),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl From<AggregateFunctionExpr> for Arc<dyn PhysicalExpr> {
    fn from(c: AggregateFunctionExpr) -> Self {
        Arc::new(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    fn schema_one_col() -> Schema {
        Schema::new(vec![Field::new("c0", DataType::Int64, false)])
    }

    #[test]
    fn column_expr_types_and_nullability_follow_input_schema() {
        let s = schema_one_col();
        let c = ColumnExpr::new(0, s.field(0).clone());
        assert_eq!(c.data_type(&s).unwrap(), DataType::Int64);
        assert_eq!(c.nullable(&s).unwrap(), false);
        assert_eq!(c.name(), "c0");
    }

    #[test]
    fn column_expr_distinct_via_from() {
        let s = schema_one_col();
        let c: Arc<dyn PhysicalExpr> = ColumnExpr::new(0, s.field(0).clone()).into();
        // Downcast via Any if we ever need it — for now just confirm
        // the trait object is callable.
        assert_eq!(c.name(), "c0");
    }

    #[test]
    fn literal_expr_typed_as_string_returns_string_array() {
        let s = schema_one_col();
        let l = LiteralExpr::new("42", DataType::Utf8);
        assert_eq!(l.data_type(&s).unwrap(), DataType::Utf8);
        // `evaluate` returns a length-1 StringArray when called
        // against a 1-row batch.
        let batch =
            arrow_array::RecordBatch::try_new(Arc::new(s.clone()), vec![Arc::new(
                arrow_array::Int64Array::from(vec![1]),
            ) as ArrayRef])
            .unwrap();
        let out = l.evaluate(&batch).unwrap();
        let arr = out.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(arr.value(0), "42");
    }

    #[test]
    fn binary_op_expr_trait_object_constructed() {
        let s = schema_one_col();
        let c0: Arc<dyn PhysicalExpr> = ColumnExpr::new(0, s.field(0).clone()).into();
        let lit: Arc<dyn PhysicalExpr> = LiteralExpr::new("0", DataType::Int64).into();
        let b = BinaryOpExpr::new(c0, "=", lit);
        let _boxed: Arc<dyn PhysicalExpr> = b.into();
        // Construction succeeds; runtime evaluation path is in
        // pylon_runtime::filter_record_batch — out of scope here.
    }

    #[test]
    fn aggregate_function_expr_carries_typed_metadata() {
        let s = schema_one_col();
        let c0: Arc<dyn PhysicalExpr> = ColumnExpr::new(0, s.field(0).clone()).into();
        let agg: Arc<dyn PhysicalExpr> = AggregateFunctionExpr::new(
            "count",
            "count_c0",
            vec![c0],
            DataType::Int64,
            vec![DataType::Int64],
        )
        .into();
        assert_eq!(agg.data_type(&s).unwrap(), DataType::Int64);
        assert_eq!(agg.nullable(&s).unwrap(), false);
    }
}
