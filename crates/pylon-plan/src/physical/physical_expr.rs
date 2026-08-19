//! Legacy `PhysicalExpr` enum (pre-R2.1). Retained through R2.2 because
//! fragment.rs and tests still match on it; R2.3 deletes this module
//! once fragment.rs + tests have moved to the new trait.

use arrow_schema::DataType;

#[deprecated(
    since = "0.2.0",
    note = "Use the `PhysicalExpr` trait in `physical::expr` plus the \
            concrete structs (`ColumnExpr`, `LiteralExpr`, `BinaryOpExpr`, \
            `AggregateFunctionExpr`). R2.3 deletes this enum."
)]
#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    Column { index: usize, field: arrow_schema::Field },
    Literal { value: String, data_type: DataType },
    BinaryOp {
        left: Box<PhysicalExpr>,
        op: String,
        right: Box<PhysicalExpr>,
    },
    /// `func` is the lowercased function name: `count` | `sum` | `min` | `max`.
    /// `args` is empty for `COUNT(*)`; otherwise one `PhysicalExpr`
    /// (typically `Column`) per arg.
    /// `input_data_types` mirrors `args` and is used at runtime to
    /// pick the right accumulator.
    /// `name` is the **output field name** (alias if supplied, else
    /// `func` for COUNT(*) or `func_col` e.g. `sum_amount`).
    AggregateFunction {
        func: String,
        name: String,
        args: Vec<PhysicalExpr>,
        data_type: DataType,
        input_data_types: Vec<DataType>,
    },
}
