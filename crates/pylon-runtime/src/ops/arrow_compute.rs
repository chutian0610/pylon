//! Re-exports of arrow-rs compute kernels used by operators.
//! Keeping them in one place lets operators depend on stable, named kernels
//! without pulling `arrow` directly into every op.

pub use arrow::compute::filter_record_batch;
