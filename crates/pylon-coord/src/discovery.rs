//! Worker discovery — register + lookup.
//!
//! M3 B-1: workers report their Arrow Flight address via a unary
//! `RegisterWorker` gRPC call. The coord assigns a worker_id, stores
//! the flight_addr, and returns the id. The worker then opens the
//! bidi `OpenSession` stream carrying the same worker_id as a
//! metadata header (`x-pylon-worker-id`), so the coord can pair the
//! session with the prior registration.
//!
//! Old workers that don't call `RegisterWorker` still work — they
//! fall back to the M2 auto-assigned worker_id and have no
//! `flight_addr` registered.

use std::collections::HashMap;
use std::sync::Mutex;

/// In-memory registry of worker registrations.
///
/// Insert on `register_worker` RPC, look up on `OpenSession` when the
/// metadata header is present. Cleared on session disconnect (M3
/// first cut: never explicitly cleared; coord restarts wipe the map).
#[derive(Default)]
pub struct Discovery {
    inner: Mutex<HashMap<u64, RegisteredWorker>>,
}

#[derive(Debug, Clone)]
pub struct RegisteredWorker {
    pub worker_id: u64,
    pub flight_addr: String,
    pub grpc_addr: String,
}

impl Discovery {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new worker. Returns the assigned worker_id (the
    /// caller passes this back to the worker so it can use it on
    /// OpenSession).
    pub fn register(
        &self,
        worker_id: u64,
        flight_addr: String,
        grpc_addr: String,
    ) -> RegisteredWorker {
        let entry = RegisteredWorker {
            worker_id,
            flight_addr,
            grpc_addr,
        };
        self.inner
            .lock()
            .expect("discovery poisoned")
            .insert(worker_id, entry.clone());
        entry
    }

    /// Look up a registration by worker_id. Returns `None` if the
    /// worker didn't register.
    pub fn lookup(&self, worker_id: u64) -> Option<RegisteredWorker> {
        self.inner
            .lock()
            .expect("discovery poisoned")
            .get(&worker_id)
            .cloned()
    }

    /// Remove a registration. Used when a worker disconnects (M3
    /// first cut: not called yet, but the API is here for future
    /// cleanup).
    pub fn unregister(&self, worker_id: u64) -> Option<RegisteredWorker> {
        self.inner
            .lock()
            .expect("discovery poisoned")
            .remove(&worker_id)
    }

    /// Snapshot all currently-registered workers.
    pub fn list(&self) -> Vec<RegisteredWorker> {
        self.inner
            .lock()
            .expect("discovery poisoned")
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_lookup() {
        let d = Discovery::new();
        let r = d.register(7, "127.0.0.1:50061".into(), "127.0.0.1:9091".into());
        assert_eq!(r.worker_id, 7);
        assert_eq!(d.lookup(7).unwrap().flight_addr, "127.0.0.1:50061");
        assert!(d.lookup(8).is_none());
    }

    #[test]
    fn unregister_removes_entry() {
        let d = Discovery::new();
        d.register(1, "f1".into(), "g1".into());
        d.unregister(1);
        assert!(d.lookup(1).is_none());
    }

    #[test]
    fn list_returns_all() {
        let d = Discovery::new();
        d.register(1, "f1".into(), "g1".into());
        d.register(2, "f2".into(), "g2".into());
        let mut list = d.list();
        list.sort_by_key(|w| w.worker_id);
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].worker_id, 1);
        assert_eq!(list[1].worker_id, 2);
    }
}
