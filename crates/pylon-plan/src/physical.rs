//! PhysicalPlan for M1: SeqScan, Filter, Project.

use arrow_schema::{Field, SchemaRef};

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
    }
}
