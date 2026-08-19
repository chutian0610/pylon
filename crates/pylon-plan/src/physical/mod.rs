//! `pylon-plan` PhysicalPlan layer (RFC 0005 § 4 role 5).
//!
//! R2.1 status:
//! - The `enum PhysicalPlan` and inner `pub mod physical_expr { enum PhysicalExpr }`
//!   stay as the pre-R2.1 surface. M3 call sites continue to match on the
//!   enum arms unchanged.
//! - `pub mod expr` and `pub mod exec` carry the new trait surface
//!   (`PhysicalExpr` trait + concrete structs, `ExecutionPlan` trait +
//!   concrete structs). R2.2.a migrates fragment.rs to the trait; until
//!   then the new types are exercised only by the unit tests in those
//!   modules.
//! - `pub mod properties` is the metadata skeleton that any
//!   `ExecutionPlan` impl fills in (`Distribution`, `PlanProperties`,
//!   `Boundedness`, `EmissionType`).

pub mod expr;
pub mod exec;
pub mod physical_expr;
pub mod properties;

// Re-export the new surface so callers in this crate (and future
// consumers via `pylon_plan::physical::exec::*`) can reach it.
pub use exec::{
    AggregateExec, ExecutionPlan, FilterExec, ProjectExec, RequiredDistribution,
    SeqScanExec,
};
pub use expr::{
    AggregateFunctionExpr, BinaryOpExpr, ColumnExpr, LiteralExpr, PhysicalExpr as PhysicalExprTrait,
};
pub use properties::{Boundedness, Distribution, EmissionType, PlanProperties};

// =================================================================
// Pre-R2.1 enum — kept until R2.3 deletes it. `#[deprecated]` to
// signal callers (and future migrators) to switch to the trait.
// =================================================================

/// Deprecated M1/M3 enum. Migrate to `Arc<dyn ExecutionPlan>`
/// (constructed from the structs above). R2.3 deletes this.
#[deprecated(
    since = "0.2.0",
    note = "Use `Arc<dyn ExecutionPlan>` (exec module) and the structs \
            (`SeqScanExec` etc.). R2.3 deletes this enum."
)]
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan {
        table: String,
        schema: arrow_schema::SchemaRef,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: physical_expr::PhysicalExpr,
    },
    Project {
        input: Box<PhysicalPlan>,
        projections: Vec<physical_expr::PhysicalExpr>,
        schema: arrow_schema::SchemaRef,
    },
    Aggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<physical_expr::PhysicalExpr>,
        aggs: Vec<physical_expr::PhysicalExpr>,
        schema: arrow_schema::SchemaRef,
    },
}
