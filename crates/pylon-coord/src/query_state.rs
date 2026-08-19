//! `QueryStateMachine` — per-query / per-stage state for the
//! coord control plane (RFC 0005 R7; M3-tail #1 follow-up).
//!
//! Today the coord polls a `sleep(2/3 sec)` heuristic for stage
//! completion. This module replaces that with an explicit
//! `register_stage + ack_task + wait_for_stage_done` flow so the
//! stage barrier fires the moment the last task acks instead of at
//! the next polling tick. Workers already publish
//! `TaskResponse::state = TASK_DONE` on the existing `OpenSession`
//! bidi; the coord's open_session handler calls `ack_task` here.
//! Zero proto change for M3-tail #1.
//!
//! Scope (M3-tail minimal): only stage-done bookkeeping. The full
//! query-level state machine per RFC 0005 § 4 role 13 (queued →
//! planning → dispatching → running → draining → finished/failed/
//! cancelled, with listeners on transition) lands in M4 alongside
//! `LogicalPlanner` / `PhysicalPlanner`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::query::QueryId;
use crate::stage::StageId;
use crate::task::TaskId;
use pylon_types::PylonError;
use tokio::sync::Notify;

/// Per-stage lifecycle. `Pending` is when `register_stage` was
/// called but no task has acked yet; `Running` once any task has
/// acked; `Done` when every expected task has acked Done; `Failed`
/// when any task has acked Failed (or the wait deadline elapsed —
/// reported by `wait_for_stage_done` itself, not stored).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageState {
    Pending,
    Running,
    Done,
    Failed,
}

/// Acknowledgement from a worker for one task. Pushed into the
/// per-(query, stage) ack set from `OpenSession`'s inbound
/// `TaskResponse` stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAck {
    Done,
    Failed,
}

#[derive(Default)]
struct Inner {
    /// Per-stage: expected task ids (set at dispatch time).
    expected: HashMap<(QueryId, StageId), HashSet<TaskId>>,
    /// Per-stage: acked task ids → their ack kind.
    acked: HashMap<(QueryId, StageId), HashMap<TaskId, TaskAck>>,
    /// Per-stage: most-recent computed `StageState`.
    state: HashMap<(QueryId, StageId), StageState>,
    /// Per-stage: one `Notify` so `wait_for_stage_done` can park
    /// until an ack arrives (or the deadline fires).
    notifiers: HashMap<(QueryId, StageId), Arc<Notify>>,
}

impl Default for QueryStateMachine {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

/// The state machine. Shared via `Arc<QueryStateMachine>` from
/// `CoordState` so the open_session handler and the dispatcher
/// share the same view.
///
/// Threading: `inner: Mutex<…>` is fine because critical sections
/// are short (HashMap entry mutation). `Notified` is `Send +
/// Sync`, so callers can park across tasks.
pub struct QueryStateMachine {
    inner: Mutex<Inner>,
}

impl QueryStateMachine {
    /// Build a fresh state machine, wrapped in `Arc` so it can
    /// be cloned into both the dispatcher and the open_session
    /// handler tasks.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a freshly-dispatched stage. Wakes any later
    /// `wait_for_stage_done` as soon as the last task acks.
    pub fn register_stage(
        &self,
        query_id: QueryId,
        stage_id: StageId,
        task_ids: Vec<TaskId>,
    ) {
        let (notifier, was_empty) = {
            let mut g = self.inner.lock().unwrap();
            let entry = g.expected.entry((query_id, stage_id)).or_default();
            let was_empty = entry.is_empty() && task_ids.is_empty();
            for tid in task_ids {
                entry.insert(tid);
            }
            g.acked.entry((query_id, stage_id)).or_default();
            let state = g.state.entry((query_id, stage_id)).or_insert(StageState::Pending);
            if !matches!(state, StageState::Pending) {
                // Already-running stage (rare — re-registration); preserve.
            }
            let n = g
                .notifiers
                .entry((query_id, stage_id))
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone();
            (n, was_empty)
        };
        // Edge case: if the stage was registered with zero tasks,
        // waiters should re-check immediately. `notified()` returns
        // a future that completes on each `notify_waiters()` call,
        // so this just nudges any sleepers that already registered.
        if was_empty {
            notifier.notify_waiters();
        }
    }

    /// Mark a task acked. Called from the coord's `OpenSession`
    /// handler when an inbound `TaskResponse.state == TASK_DONE`
    /// (or `TASK_FAILED`) is seen. Wakes any
    /// `wait_for_stage_done` future if this ack completed the
    /// stage.
    pub fn ack_task(
        &self,
        query_id: QueryId,
        stage_id: StageId,
        task_id: TaskId,
        ack: TaskAck,
    ) {
        let notifier = {
            let mut g = self.inner.lock().unwrap();
            g.acked
                .entry((query_id, stage_id))
                .or_default()
                .insert(task_id, ack);
            // Recompute stage state.
            let expected = g.expected.get(&(query_id, stage_id));
            let acked = g.acked.get(&(query_id, stage_id));
            let new_state = compute_state(expected, acked);
            if let Some(s) = new_state {
                g.state.insert((query_id, stage_id), s);
            }
            g.notifiers
                .get(&(query_id, stage_id))
                .cloned()
        };
        if let Some(n) = notifier {
            n.notify_waiters();
        }
    }

    /// Read-only view of the current state of one stage.
    /// `None` = unknown (no `register_stage` was called yet).
    pub fn stage_state(
        &self,
        query_id: QueryId,
        stage_id: StageId,
    ) -> Option<StageState> {
        let g = self.inner.lock().unwrap();
        g.state.get(&(query_id, stage_id)).copied()
    }

    /// Await stage-done (or timeout). Returns `Ok(())` once every
    /// expected task has acked Done; `Err(PylonError::Internal)`
    /// on ack Failed or timeout.
    ///
    /// The future parks on a per-stage `Notify` and re-checks
    /// under the lock on each wakeup, so spurious notifications
    /// are cheap and deadlocks are impossible (we never hold the
    /// lock across `.await`).
    pub async fn wait_for_stage_done(
        self: &Arc<Self>,
        query_id: QueryId,
        stage_id: StageId,
        deadline: Duration,
    ) -> Result<(), PylonError> {
        let notifier = {
            let g = self.inner.lock().unwrap();
            g.notifiers
                .get(&(query_id, stage_id))
                .cloned()
                .unwrap_or_else(|| Arc::new(Notify::new()))
        };
        let start = Instant::now();
        loop {
            // Re-check state under lock.
            let (expected_count, acked_count, failures, _current) = {
                let g = self.inner.lock().unwrap();
                let expected = g.expected.get(&(query_id, stage_id));
                let acked = g.acked.get(&(query_id, stage_id));
                (
                    expected.map(|s| s.len()).unwrap_or(0),
                    acked.map(|m| m.len()).unwrap_or(0),
                    acked
                        .map(|m| m.values().filter(|a| matches!(a, TaskAck::Failed)).count())
                        .unwrap_or(0),
                    g.state.get(&(query_id, stage_id)).copied(),
                )
            };
            // Failed takes precedence.
            if failures > 0 {
                return Err(PylonError::Internal(format!(
                    "stage ({}, {}) had {} failed task(s)",
                    query_id.0,
                    stage_id.0,
                    failures
                )));
            }
            // Done if every expected task has acked.
            // Special-case: a stage registered with zero expected
            // tasks (degenerate — empty broadcast / Gather-to-one
            // collapse) resolves immediately, since there's no
            // work to await.
            if expected_count == 0 && acked_count == 0 {
                return Ok(());
            }
            if expected_count > 0 && acked_count >= expected_count {
                return Ok(());
            }
            // Accept Running/Pending if no failures and not all acked yet.
            let elapsed = start.elapsed();
            if elapsed >= deadline {
                return Err(PylonError::Internal(format!(
                    "stage ({}, {}) timed out after {:?} ({}/{} acked)",
                    query_id.0,
                    stage_id.0,
                    deadline,
                    acked_count,
                    expected_count
                )));
            }
            let remaining = deadline.saturating_sub(elapsed);
            let notify = notifier.clone();
            tokio::select! {
                _ = notify.notified() => continue,
                _ = tokio::time::sleep(remaining) => continue,
            }
        }
    }
}

fn compute_state(
    expected: Option<&HashSet<TaskId>>,
    acked: Option<&HashMap<TaskId, TaskAck>>,
) -> Option<StageState> {
    match (expected, acked) {
        (Some(e), Some(a)) => {
            if e.is_empty() {
                return Some(StageState::Done);
            }
            let failures = a.values().filter(|v| matches!(v, TaskAck::Failed)).count();
            if failures > 0 {
                return Some(StageState::Failed);
            }
            let all_done = e.iter().all(|tid| a.contains_key(tid));
            if all_done {
                Some(StageState::Done)
            } else if !a.is_empty() {
                Some(StageState::Running)
            } else {
                Some(StageState::Pending)
            }
        }
        (None, _) | (_, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(n: u64) -> QueryId {
        QueryId(n)
    }
    fn s(n: u64) -> StageId {
        StageId(n)
    }
    fn t(n: u64) -> TaskId {
        TaskId(n)
    }

    #[test]
    fn unknown_stage_returns_none() {
        let qsm = QueryStateMachine::new();
        assert_eq!(qsm.stage_state(q(1), s(1)), None);
    }

    #[test]
    fn register_then_single_ack_done_signals_done() {
        let qsm = QueryStateMachine::new();
        qsm.register_stage(q(1), s(1), vec![t(7), t(8)]);
        assert_eq!(qsm.stage_state(q(1), s(1)), Some(StageState::Pending));
        qsm.ack_task(q(1), s(1), t(7), TaskAck::Done);
        assert_eq!(qsm.stage_state(q(1), s(1)), Some(StageState::Running));
        qsm.ack_task(q(1), s(1), t(8), TaskAck::Done);
        assert_eq!(qsm.stage_state(q(1), s(1)), Some(StageState::Done));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_resolves_when_last_task_acked() {
        let qsm = QueryStateMachine::new();
        qsm.register_stage(q(1), s(1), vec![t(1), t(2), t(3)]);

        // Spawn the ack on a small delay; the wait should resolve.
        let qsm2 = qsm.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            qsm2.ack_task(q(1), s(1), t(1), TaskAck::Done);
            tokio::time::sleep(Duration::from_millis(5)).await;
            qsm2.ack_task(q(1), s(1), t(2), TaskAck::Done);
            tokio::time::sleep(Duration::from_millis(5)).await;
            qsm2.ack_task(q(1), s(1), t(3), TaskAck::Done);
        });

        let res = qsm
            .wait_for_stage_done(q(1), s(1), Duration::from_secs(5))
            .await;
        assert!(res.is_ok(), "expected Ok, got {:?}", res);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_fails_fast_on_failed_ack() {
        let qsm = QueryStateMachine::new();
        qsm.register_stage(q(1), s(1), vec![t(1), t(2)]);
        qsm.ack_task(q(1), s(1), t(1), TaskAck::Failed);
        let res = qsm
            .wait_for_stage_done(q(1), s(1), Duration::from_secs(5))
            .await;
        assert!(res.is_err(), "expected Err, got {:?}", res);
        let err = res.unwrap_err();
        assert!(
            err.to_string().contains("failed"),
            "want 'failed' in: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_times_out_when_no_acks_arrive() {
        let qsm = QueryStateMachine::new();
        qsm.register_stage(q(1), s(1), vec![t(1)]);
        // No ack ever.
        let res = qsm
            .wait_for_stage_done(q(1), s(1), Duration::from_millis(20))
            .await;
        let err = res.expect_err("expected timeout Err");
        assert!(
            err.to_string().contains("timed out"),
            "want 'timed out' in: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_resolves_immediately_for_zero_task_stage() {
        // Edge case: a stage registered with zero tasks (shouldn't
        // happen in practice, but the API permits it; a degenerate
        // Gather-to-one stage might collapse to this). The wait
        // should resolve Ok(()) immediately.
        let qsm = QueryStateMachine::new();
        qsm.register_stage(q(1), s(1), vec![]);
        let res = qsm
            .wait_for_stage_done(q(1), s(1), Duration::from_millis(20))
            .await;
        assert!(res.is_ok());
    }

    #[test]
    fn distinct_queries_and_stages_isolated() {
        let qsm = QueryStateMachine::new();
        qsm.register_stage(q(1), s(1), vec![t(100)]);
        qsm.register_stage(q(1), s(2), vec![t(200)]);
        qsm.register_stage(q(2), s(1), vec![t(300)]);
        qsm.ack_task(q(1), s(1), t(100), TaskAck::Done);
        assert_eq!(qsm.stage_state(q(1), s(1)), Some(StageState::Done));
        assert_eq!(qsm.stage_state(q(1), s(2)), Some(StageState::Pending));
        assert_eq!(qsm.stage_state(q(2), s(1)), Some(StageState::Pending));
        qsm.ack_task(q(2), s(1), t(300), TaskAck::Done);
        assert_eq!(qsm.stage_state(q(2), s(1)), Some(StageState::Done));
        assert_eq!(qsm.stage_state(q(1), s(2)), Some(StageState::Pending));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_does_not_starve_other_waiters() {
        // Two waiters on the same stage should both unblock when
        // a single ack completes the stage.
        let qsm = QueryStateMachine::new();
        qsm.register_stage(q(1), s(1), vec![t(1)]);
        let qsm_a = qsm.clone();
        let qsm_b = qsm.clone();
        let w1 = tokio::spawn(async move {
            qsm_a
                .wait_for_stage_done(q(1), s(1), Duration::from_secs(5))
                .await
        });
        let w2 = tokio::spawn(async move {
            qsm_b
                .wait_for_stage_done(q(1), s(1), Duration::from_secs(5))
                .await
        });
        // Give both waiters time to park on notify_waiters().
        tokio::time::sleep(Duration::from_millis(10)).await;
        qsm.ack_task(q(1), s(1), t(1), TaskAck::Done);
        let (r1, r2) = tokio::join!(w1, w2);
        assert!(r1.unwrap().is_ok());
        assert!(r2.unwrap().is_ok());
    }
}
