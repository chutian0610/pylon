//! StateBridge — Trino/Velox "JoinBridge" abstraction for cross-driver state.
//!
//! In Trino's execution model, multiple Drivers within a Pipeline may share
//! state. The most famous example is HashJoinBridge: a build-side Driver
//! builds the hash table, and probe-side Drivers consume it. The pipeline
//! does not finish until all Drivers signal completion through the bridge.
//!
//! Pylon M2 doesn't have any operator that uses StateBridge yet (joins come
//! in M3). But the trait is here so M3 doesn't have to refactor pipeline.rs
//! again.

use anyhow::Result;
use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateChange {
    /// Build side completed; probe side may now begin.
    BuildComplete,
    /// Probe phase finished.
    ProbeComplete,
    /// A spilled partition is being restored from disk.
    Restored,
    /// Operator is signaling memory pressure to its peers.
    MemoryBackpressure,
}

/// A bridge shared by multiple Drivers (or operators) within a Pipeline.
pub trait StateBridge: Send + Sync + Debug {
    fn name(&self) -> &str;
    fn on_state_change(&self, change: StateChange) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct DummyBridge;

impl StateBridge for DummyBridge {
    fn name(&self) -> &str {
        "DummyBridge"
    }

    fn on_state_change(&self, _change: StateChange) -> Result<()> {
        Ok(())
    }
}
