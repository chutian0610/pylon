//! Pylon plan: SQL → LogicalPlan → PhysicalPlan.
//!
//! Fragmenter lives in `pylon-coord` (Trino-aligned placement).

pub mod logical;
pub mod optimizer;
pub mod physical;
pub mod translate;

pub use logical::LogicalPlan;
