//! `pylon-plan` PhysicalPlan layer (RFC 0005 §4 role 5).
//!
//! Post-R2.3: there is no `enum PhysicalPlan` anymore. Every op is a
//! concrete struct (`SeqScanExec`, `FilterExec`, `ProjectExec`,
//! `AggregateExec`) implementing the `ExecutionPlan` trait; callers
//! interact with `Arc<dyn ExecutionPlan>` exclusively. M4+ plug-in
//! operators (R6) follow the same shape.

pub mod exec;
pub mod expr;
pub mod fragmenter;
pub mod properties;

pub use exec::{
    AggregateExec, ExecutionPlan, FilterExec, ProjectExec, RequiredDistribution,
    SeqScanExec,
};
pub use expr::{
    AggregateFunctionExpr, BinaryOpExpr, ColumnExpr, LiteralExpr, PhysicalExpr as PhysicalExprTrait,
};
pub use fragmenter::{
    rule_fires, AggregateFragmenterRule, BoundaryEmit, BoundaryStrategy, FragmenterRule,
};
pub use properties::{Boundedness, Distribution, EmissionType, PlanProperties};
