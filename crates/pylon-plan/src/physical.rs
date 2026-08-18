//! PhysicalPlan for M1: SeqScan, Filter, Project.
//! M3 first cut: Aggregate (used by A1-1 to plumb the new node; the
//! op itself is implemented in `pylon-runtime` in A1-3).

use arrow_schema::SchemaRef;

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan {
        table: String,
        schema: SchemaRef,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: physical_expr::PhysicalExpr,
    },
    Project {
        input: Box<PhysicalPlan>,
        projections: Vec<physical_expr::PhysicalExpr>,
        schema: SchemaRef,
    },
    /// `SELECT <group_by>, <aggs> FROM <input> GROUP BY <group_by>`.
    /// `schema` is the post-aggregation schema (group_by cols + agg
    /// result cols). For A1-1 this is a passthrough node; the real
    /// operator lives in `pylon-runtime::ops::aggregate` (A1-3).
    Aggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<physical_expr::PhysicalExpr>,
        aggs: Vec<physical_expr::PhysicalExpr>,
        schema: SchemaRef,
    },
}

pub mod physical_expr {
    use arrow_schema::{DataType, Field};

    #[derive(Debug, Clone)]
    pub enum PhysicalExpr {
        Column { index: usize, field: Field },
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
}
