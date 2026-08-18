//! Pylon plan: SQL → LogicalPlan → PhysicalPlan.
//!
//! Fragmenter lives in `pylon-coord` (Trino-aligned placement).

pub mod logical;
pub mod physical;
pub mod translate;

pub use logical::LogicalPlan;
pub use physical::PhysicalPlan;
