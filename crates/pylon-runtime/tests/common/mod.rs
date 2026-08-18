//! Shared helpers for tests in `pylon-runtime/tests/`.
//!
//! The M3-tail exchange unification (PR1/PR2 in
//! `docs/roadmap/m3-tail-exchange-unify.md`) removed the in-process
//! `ExchangeSinkOp` short-circuit. Every test that previously pushed
//! batches directly into a `PylonFlightService::push` queue via
//! `ExchangeSinkOp` now spins up a loopback Arrow Flight server
//! (`FlightServerImpl`) on a kernel-assigned port and exercises the
//! real `ExchangeSinkRpc` → `DoExchange` → `PylonFlightService::push`
//! path. Same-worker shards naturally express as `target_flight_addr
//! == local_addr`.
//!
//! This module is the convention `tests/common/mod.rs` so each
//! integration test file may `mod common;` to pull in `start_flight_server`
//! + `wait_for_spawned_send_jobs`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use pylon_exchange::{FlightServerImpl, PylonFlightService};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// One in-process Arrow Flight server. Bind on kernel-assigned port;
/// return `(bound_addr, service_arc, join_handle)`. Aborting the
/// handle closes the listener; tests do that at the end of their
/// scope.
pub async fn start_flight_server() -> (SocketAddr, Arc<PylonFlightService>, tokio::task::JoinHandle<()>) {
    let service = Arc::new(PylonFlightService::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind flight");
    let addr = listener.local_addr().expect("local_addr");
    let svc = FlightServerImpl::new(service.clone());
    let handle = tokio::spawn(async move {
        let incoming = TcpListenerStream::new(listener);
        let _ = Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await;
    });
    // Brief spin-up: tonic doesn't immediately accept after spawn, so
    // tests that immediately fire a DoExchange can race the listener
    // entering the accept loop. A short yield lets the task reach
    // poll-ready before the first batch is sent.
    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, service, handle)
}

/// Wait until the spawn'd DoExchange tasks produced by
/// `ExchangeSinkRpc::add_input` have flushed their batches into
/// the local `PylonFlightService` queue. We poll `service.pending()`
/// until either zero or the timeout elapses.
///
/// `#[allow(dead_code)]` because this helper isn't exercised by
/// every integration test binary that includes `mod common;`;
/// rustc's per-crate dead-code lint still triggers otherwise.
///
/// The bound `expected_batches` is the total number of non-empty
/// `add_input` batches the test expects to land; polling stops early
/// when expected_batches is reached, saving time on large fan-outs.
#[allow(dead_code)]
pub async fn wait_for_spawned_send_jobs(
    service: &PylonFlightService,
    descriptors: &[pylon_exchange::FlightDescriptor],
    expected_batches: usize,
    timeout: Duration,
) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let mut total = 0usize;
        for d in descriptors {
            total += service.pending(d).await;
        }
        if total >= expected_batches {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "wait_for_spawned_send_jobs: only {total} batches of {expected_batches} \
                 landed after {timeout:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
