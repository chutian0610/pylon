//! M3 B-2 integration test: two in-process Arrow Flight servers exchange
//! real `DoExchange` RPCs (no shared memory, no in-process queue
//! shortcuts). This is the unit-level counterpart of
//! `tools/e2e/two_worker_smoke.sh`: same code paths (tonic `DoExchange`
//! over real Arrow IPC streaming bytes) but no OS processes, so it
//! runs as part of `cargo test -p pylon-runtime`.
//!
//! What this exercises:
//! 1. Start two `PylonFlightService` instances, each with its own
//!    Arrow Flight server bound on a kernel-assigned port.
//! 2. `ExchangeSinkRpc::new_partitioned` with 4 partitions, target
//!    flight_addrs alternating between the two servers.
//! 3. Send 1024 rows through the sink.
//! 4. Each server's `ExchangeSourceOp` pulls the batches that landed
//!    in its `PylonFlightService` queue.
//! 5. `HashAggregateOp` runs per server (each sees ~half the rows for
//!    each `name`), then we union the partial aggregates and verify
//!    the final row count matches the input.
//!
//! Run: `cargo test -p pylon-runtime --test flight_rpc_e2e_test -- --nocapture`

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use pylon_exchange::{FlightDescriptor, FlightServerImpl, PylonFlightService};
use pylon_runtime::ops::{
    AggSpec, ExchangeSinkRpc, ExchangeSourceOp, HashAggregateOp, RpcTarget,
};
use pylon_runtime::{Driver, Pipeline, PipelineOp};
use std::io::Cursor;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn sample_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, true),
    ]))
}

/// Build a RecordBatch with `n` rows of deterministic test data:
/// `id = 0..n`, `name = "name_{id:05}"` (so we know which partition
/// each row should hash to), `amount = id as f64`.
fn mk_batch(n: usize) -> RecordBatch {
    let schema = sample_schema();
    let ids = Int64Array::from((0..n as i64).collect::<Vec<_>>());
    let names = StringArray::from(
        (0..n).map(|i| format!("name_{i:05}")).collect::<Vec<_>>(),
    );
    let amounts = Float64Array::from((0..n).map(|i| i as f64).collect::<Vec<_>>());
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names), Arc::new(amounts)]).unwrap()
}

/// Start one in-process Arrow Flight server. Returns
/// `(bound_addr, service_arc)`. The server is dropped (and the
/// listener closed) when the returned `JoinHandle` is aborted.
async fn start_flight_server() -> (SocketAddr, Arc<PylonFlightService>, tokio::task::JoinHandle<()>) {
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
    (addr, service, handle)
}

/// Build the standard HashAggregateOp for `SELECT name, COUNT(*)
/// GROUP BY name`.
fn mk_agg_op() -> HashAggregateOp {
    let mut op = HashAggregateOp::new(
        vec!["name".into()],
        vec![AggSpec {
            func: "count".into(),
            arg_col: None,
            out_name: "count".into(),
        }],
        Arc::new(Schema::empty()),
    );
    // Pre-resolve the post-aggregation schema (name Utf8, count
    // Int64) so partitions with zero rows still emit a well-formed
    // zero-row batch instead of failing in build_output.
    op.resolve_output_schema(Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("count", DataType::Int64, true),
    ])));
    op
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_in_process_flight_servers_exchange_real_dopexchange() {
    // ---- Arrange: two Arrow Flight servers on kernel-assigned ports.
    let (addr_a, service_a, h_a) = start_flight_server().await;
    let (addr_b, service_b, h_b) = start_flight_server().await;
    eprintln!("flight_a = {addr_a}, flight_b = {addr_b}");

    // ---- Act: a partitioned ExchangeSinkRpc whose 4 target
    // descriptors alternate between the two servers (the
    // Fragmenter's p % n_workers round-robin).
    let descriptor_for = |p: usize| -> String {
        // M3 uses descriptor form: pylon://query/{qid}/stage/{stage}/task/{p}
        format!("pylon://q/1/s/2/t/{p}")
    };
    let targets: Vec<RpcTarget> = (0..4)
        .map(|p| RpcTarget {
            flight_addr: if p % 2 == 0 {
                addr_a.to_string()
            } else {
                addr_b.to_string()
            },
            descriptor: FlightDescriptor(descriptor_for(p)),
        })
        .collect();
    let mut sink = ExchangeSinkRpc::new_partitioned(targets, vec!["name".into()]);

    // 1024 rows of test data. Same names repeat so per-name count
    // would be > 1 if we hashed by `id`, but since `name` is the
    // partition key, every name hashes to the same partition and is
    // counted together on its worker.
    let n_rows = 1024;
    let batch = mk_batch(n_rows);
    sink.add_input(batch).await.expect("add_input");
    sink.no_more_input().await.expect("no_more_input");
    drop(sink); // drop the sink so its background DoExchange tasks finish

    // Give the spawn'd `send_rpc_job` futures a moment to finish
    // (they use tokio::spawn and complete after DoExchange resolves).
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // ---- Act: drive each server's pipeline [ExchangeSource ->
    // HashAggregate] via the real Driver. We collect the final
    // batches and union them.
    let descs_a: Vec<String> = (0..4).filter(|p| p % 2 == 0).map(|p| descriptor_for(p)).collect();
    let descs_b: Vec<String> = (0..4).filter(|p| p % 2 == 1).map(|p| descriptor_for(p)).collect();
    async fn run_agg(
        service: Arc<PylonFlightService>,
        descs: Vec<String>,
    ) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        for d in descs {
            let source = Box::new(ExchangeSourceOp::new(
                FlightDescriptor(d.clone()),
                service.clone(),
            ));
            let agg = Box::new(mk_agg_op());
            let pipeline = Pipeline::new(vec![source, agg]);
            let driver = Driver::new(pipeline);
            let mut rx = driver.run(None).await.expect("driver.run");
            while let Some(b) = rx.recv().await {
                out.push(b);
            }
        }
        out
    }
    let a_batches = run_agg(service_a.clone(), descs_a).await;
    let b_batches = run_agg(service_b.clone(), descs_b).await;

    // ---- Assert 2: the union of (name, count) across both servers
    // contains all 1024 distinct names. The HashAggregate on each
    // server sees only its half of the rows; the per-name count
    // matches the rows-for-that-name in that server's partition.
    use std::collections::BTreeMap;
    let mut name_count: BTreeMap<String, i64> = BTreeMap::new();
    for batch in a_batches.iter().chain(b_batches.iter()) {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for r in 0..batch.num_rows() {
            *name_count.entry(names.value(r).to_string()).or_insert(0) += counts.value(r);
        }
    }
    let total_rows_in_batches: i64 = a_batches
        .iter()
        .chain(b_batches.iter())
        .map(|b| b.num_rows() as i64)
        .sum();
    assert_eq!(
        total_rows_in_batches, n_rows as i64,
        "all 1024 distinct names should be aggregated exactly once across both servers"
    );
    assert_eq!(
        name_count.len() as i64, n_rows as i64,
        "all 1024 distinct names should appear in the aggregate result"
    );
    // Every name should have count = 1 (each name appears once in
    // mk_batch's "name_{i:05}" sequence).
    for (n, c) in &name_count {
        assert_eq!(*c, 1, "name {n} should have count 1, got {c}");
    }

    h_a.abort();
    h_b.abort();
}

/// Same as the test above but verifies the **Arrow IPC streaming**
/// bytes on the wire (not just the in-process `PylonFlightService`
/// storage). We deserialize a single batch directly from one server's
/// `PylonFlightClient::take_bytes` (the encode path the worker uses
/// for in-process) to confirm the schema + row count round-trip
/// works for our typical data shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipc_streaming_roundtrip_preserves_schema_and_rows() {
    use pylon_exchange::PylonFlightClient;
    let original = mk_batch(8);
    let original_schema = original.schema();
    let original_rows = original.num_rows();

    let client = PylonFlightClient::connect("in-process://test".into(), "rt".into())
        .await
        .expect("connect");
    client.send(original.clone()).await.expect("send");
    client.close().await.expect("close");
    let bytes = client.take_bytes().await;

    // Decode and check.
    let reader = StreamReader::try_new(Cursor::new(bytes), None).expect("reader");
    let schema = reader.schema();
    assert_eq!(schema.as_ref(), original_schema.as_ref());
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("collect");
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, original_rows);
}
