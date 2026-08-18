//! LogicalPlan for the M1 subset: Scan / Filter / Project.

use arrow_schema::{Field, SchemaRef};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        schema: SchemaRef,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        projections: Vec<Expr>,
    },
}

#[derive(Debug, Clone)]
pub enum Expr {
    Column(Field),
    Literal(String),
    BinaryOp {
        left: Box<Expr>,
        op: String,
        right: Box<Expr>,
    },
    Wildcard,
}
