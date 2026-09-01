//! Concrete `MemoryPool` implementations.
//!
//! Defined by RFC 0007 §3.1 (trait) and §5 S1 (delivered here).
//! Two impls live in this module:
//!
//! * [`PerTaskPool`] — the production per-task budget. Atomic counter,
//!   `Arc<PerTaskPool>` shared across the driver thread and any
//!   spill manager.
//! * [`NoopMemoryPool`] — a no-op pool used when an op is constructed
//!   without explicit budget (e.g. most unit tests). Always allows
//!   `try_grow`; never refuses. Equivalent to `budget = usize::MAX`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pylon_types::{MemoryPool, PylonError};

/// The production per-task byte budget.
///
/// Tracks `in_use` via a single `AtomicUsize`. The `try_grow` path is
/// a single atomic compare-and-swap loop, so it has no inherent
/// fairness guarantee — but the contract is "best effort, no
/// overshoot", which the `compare_exchange_weak` loop below
/// guarantees.
///
/// # Thread safety
///
/// All methods are safe to call from any thread. The `Arc<PerTaskPool>`
/// shared between driver thread and spill manager is the canonical
/// ownership shape.
#[derive(Debug)]
pub struct PerTaskPool {
    budget: AtomicUsize,
    in_use: AtomicUsize,
}

impl PerTaskPool {
    /// Construct a new pool with `budget` bytes. The pool starts
    /// empty (in_use = 0).
    pub fn new(budget: usize) -> Arc<Self> {
        Arc::new(Self {
            budget: AtomicUsize::new(budget),
            in_use: AtomicUsize::new(0),
        })
    }
}

impl MemoryPool for PerTaskPool {
    fn try_grow(&self, bytes: usize) -> Result<(), PylonError> {
        // Common path: budget = 0 (a test pool) — refuse any non-zero
        // request immediately.
        let budget = self.budget.load(Ordering::Relaxed);
        if budget == 0 {
            return Err(PylonError::Internal(format!(
                "PerTaskPool: budget = 0, cannot claim {bytes} bytes"
            )));
        }

        // Compare-and-swap loop. Acquire on load and Release on store
        // are sufficient for an unbounded budget counter (we only
        // need atomicity; we don't need synchronization with other
        // reads).
        loop {
            let current = self.in_use.load(Ordering::Acquire);
            let next = current.saturating_add(bytes);
            if next > budget {
                return Err(PylonError::Internal(format!(
                    "PerTaskPool exhausted: budget={budget}, in_use={current}, requested={bytes}"
                )));
            }
            if self
                .in_use
                .compare_exchange_weak(current, next, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            // CAS failed — retry with the latest value.
        }
    }

    fn release(&self, bytes: usize) {
        // Saturating release: an over-release clamps to zero. This
        // protects against double-release bugs in operator Drop
        // impls without producing nonsense usize underflow.
        let mut current = self.in_use.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(bytes);
            match self.in_use.compare_exchange_weak(
                current,
                next,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Acquire)
    }

    fn budget(&self) -> usize {
        self.budget.load(Ordering::Relaxed)
    }
}

/// A `MemoryPool` that accepts everything. Used as the implicit
/// pool when an op is constructed without an explicit budget. The
/// alternative (`NoopMemoryPool` always succeeds) means no op in
/// test code or default-build paths has to thread an `Arc` around.
///
/// `try_grow` is always `Ok(())`; `release` is a no-op; `in_use` is
/// always 0; `budget` is `usize::MAX`. Never crashes, never refuses.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMemoryPool;

impl MemoryPool for NoopMemoryPool {
    fn try_grow(&self, _bytes: usize) -> Result<(), PylonError> {
        Ok(())
    }
    fn release(&self, _bytes: usize) {}
    fn in_use(&self) -> usize {
        0
    }
    fn budget(&self) -> usize {
        usize::MAX
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn budget_zero_refuses_everything() {
        let p = PerTaskPool::new(0);
        assert!(p.try_grow(1).is_err());
        assert_eq!(p.in_use(), 0);
    }

    #[test]
    fn happy_path() {
        let p = PerTaskPool::new(1024);
        assert!(p.try_grow(256).is_ok());
        assert_eq!(p.in_use(), 256);
        assert!(p.try_grow(256).is_ok());
        assert_eq!(p.in_use(), 512);
        p.release(256);
        assert_eq!(p.in_use(), 256);
        p.release(256);
        assert_eq!(p.in_use(), 0);
    }

    #[test]
    fn rejects_overshoot() {
        let p = PerTaskPool::new(1024);
        assert!(p.try_grow(512).is_ok());
        assert!(p.try_grow(513).is_err());
        assert_eq!(p.in_use(), 512);
    }

    #[test]
    fn release_saturates_to_zero() {
        let p = PerTaskPool::new(1024);
        p.release(999_999);
        assert_eq!(p.in_use(), 0);
    }

    #[test]
    fn try_reserve_default_impl() {
        let p = PerTaskPool::new(1024);
        p.try_grow(900).unwrap();
        assert_eq!(p.try_reserve(200), 124);
        assert_eq!(p.try_reserve(0), 0);
        assert_eq!(p.try_reserve(usize::MAX), 124);
    }

    #[test]
    fn concurrent_try_grow_stays_within_budget() {
        // 8 threads, each tries to claim 100 bytes out of a 400-byte pool.
        // Total demand is 800; only 4 attempts should succeed. None
        // should ever observe in_use > budget.
        let p = PerTaskPool::new(400);
        let mut successes = 0;
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let p = Arc::clone(&p);
                thread::spawn(move || p.try_grow(100).is_ok())
            })
            .collect();
        for h in handles {
            if h.join().unwrap() {
                successes += 1;
            }
        }
        assert_eq!(successes, 4, "exactly 4 of 8 attempts should fit");
        assert_eq!(p.in_use(), 400);
    }

    #[test]
    fn noop_pool_accepts_anything() {
        let n = NoopMemoryPool;
        for huge in [1usize, 1 << 20, usize::MAX / 2] {
            assert!(n.try_grow(huge).is_ok());
            n.release(huge);
        }
        assert_eq!(n.in_use(), 0);
        assert_eq!(n.budget(), usize::MAX);
    }
}
