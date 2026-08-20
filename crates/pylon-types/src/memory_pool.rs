//! `MemoryPool` — per-task byte budget.
//!
//! Defined in RFC 0007 §3.1. Trait-only; concrete impls
//! (e.g. `pylon_runtime::memory_pool::PerTaskPool`) live in `pylon-runtime`.
//! The rule (carried over from RFC 0005 §3 rule #1) is:
//!
//! > A [`MemoryPool`] trait may only live in `pylon-types`. Impls live
//! > in `pylon-runtime`. Connectors cannot import a runtime pool; they
//! > receive it through `DataSourceContext.memory_pool`
//! > (or the planned equivalent). This is enforced by
//! > `tools/check-spi-boundaries.sh`.
//!
//! ## Conformance rule (RFC 0007 §3.1)
//!
//! Every operator whose state scales with the input batch shape must
//! call [`MemoryPool::try_grow`] at allocation and
//! [`MemoryPool::release`] at drop. The operator's `Drop` impl
//! **must** call `release(allocated_bytes)` to balance. The op-level
//! invariant is "`in_use` returns to baseline by the time `Drop`
//! returns".

use crate::{PylonError, Result};

/// A per-task byte budget. Implementations must be `Send + Sync` so an
/// `Arc<dyn MemoryPool>` can be shared across threads (e.g. between the
/// driver thread and the spill manager).
///
/// # Drop semantics
///
/// The trait is intentionally drop-agnostic. The conformance rule
/// (above) requires operators to release explicitly. Implementations
/// may log a warning on drop if `in_use() != 0`, but cannot fail safely
/// to block the destructor — the only safe recovery is a process
/// abort, which is rarely desired.
///
/// # Errors
///
/// `try_grow` returns [`PylonError::Internal`] with a descriptive
/// message when the pool would exceed its budget. The caller decides
/// what to do on exhaustion (e.g. trigger a spill in a future PR;
/// see RFC 0007 §4.1 — `MemoryPool` returning `Err` is the trigger).
pub trait MemoryPool: Send + Sync + std::fmt::Debug {
    /// Try to claim `bytes` more. Returns `Ok(())` if the claim was
    /// accepted; `Err(PylonError::Internal(...))` if `bytes` would
    /// exceed [`Self::budget`].
    ///
    /// On `Err`, no bytes are claimed — the call is atomic w.r.t. the
    /// pool's accounting.
    fn try_grow(&self, bytes: usize) -> Result<()>;

    /// Release `bytes` back to the pool. Saturating: releasing more
    /// than is currently claimed clamps the counter to zero rather
    /// than underflowing. Implementations may want to log a warning
    /// when this happens; see `PerTaskPool`'s implementation in
    /// `pylon-runtime::memory_pool`.
    fn release(&self, bytes: usize);

    /// Bytes currently claimed by this pool. Cheap to call; intended
    /// for back-pressure checks in hot paths.
    fn in_use(&self) -> usize;

    /// Configured upper bound. The pool will never `try_grow` past
    /// this; calls that would exceed it return `Err`.
    fn budget(&self) -> usize;

    /// Hint: "how many of `target` bytes may I claim right now
    /// without exceeding budget?". Useful when an op wants to size
    /// batch intake (e.g. planning a buffer copy). Default
    /// implementation: `min(target, budget - in_use)`.
    fn try_reserve(&self, target: usize) -> usize {
        let headroom = self.budget().saturating_sub(self.in_use());
        target.min(headroom)
    }
}

