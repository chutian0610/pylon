//! Pylon proto — wire types shared by coord and worker.
//!
//! Generated from `proto/pylon.proto` at build time via `tonic-build`.
//! Re-export everything as `pylon_proto::worker_service_server` etc.

pub mod pylon {
    tonic::include_proto!("pylon");
}

pub use pylon::*;
