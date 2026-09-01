//! M3+ unified exchange tests: drive `ExchangeSinkRpc` against a
//! loopback Arrow Flight server (the post-PR2 path). Loopback gRPC
//! is the same code path as cross-worker, so these tests also
//! exercise the same-worker partition semantics.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use pylon_exchange::FlightDescriptor;
use pylon_runtime::PipelineOp;
use pylon_runtime::ops::{ExchangeSinkRpc, ExchangeSourceOp, RpcTarget};

fn sample_batch(rows: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
    ]));
    let ids = Int64Array::from((0..rows).collect::<Vec<_>>());
    let amounts = Float64Array::from((0..rows).map(|i| i as f64 * 2.5).collect::<Vec<_>>());
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(amounts)]).unwrap()
}

#[tokio::test]
async fn exchange_sink_then_source_roundtrip() {
    // Loopback Flight server (PR2 unification: same code path
    // for loopback and remote).
    let (addr, service, h) = common::start_flight_server().await;
    let desc = FlightDescriptor::for_task(42, 1, 0);
    let target = RpcTarget {
        flight_addr: addr.to_string(),
        descriptor: desc.clone(),
    };
    // Single-target partitioned sink — every row hashes to
    // partition 0 since n_partitions == 1.
    let mut sink = ExchangeSinkRpc::new_partitioned(vec![target], vec!["id".into()]);
    sink.add_input(sample_batch(100)).await.unwrap();
    sink.add_input(sample_batch(50)).await.unwrap();
    sink.no_more_input().await.unwrap();
    common::wait_for_spawned_send_jobs(
        &service,
        std::slice::from_ref(&desc),
        2,
        Duration::from_secs(5),
    )
    .await;

    let mut src = ExchangeSourceOp::new(desc, service.clone());
    let b1 = src.get_output().await.unwrap().expect("first batch");
    let b2 = src.get_output().await.unwrap().expect("second batch");
    assert_eq!(b1.num_rows(), 100);
    assert_eq!(b2.num_rows(), 50);
    src.no_more_input().await.unwrap();
    assert!(src.is_finished().await);
    h.abort();
}

#[tokio::test]
async fn exchange_isolates_descriptors() {
    // Two single-target partitioned sinks sharing one Flight server
    // (loopback). Distinct descriptors give distinct queues.
    let (addr, service, h) = common::start_flight_server().await;
    let desc_a = FlightDescriptor::for_task(1, 1, 0);
    let desc_b = FlightDescriptor::for_task(1, 1, 1);
    let target_a = RpcTarget {
        flight_addr: addr.to_string(),
        descriptor: desc_a.clone(),
    };
    let target_b = RpcTarget {
        flight_addr: addr.to_string(),
        descriptor: desc_b.clone(),
    };
    // n_partitions = 1 each: every row hashes to partition 0, so
    // each sink routes the whole batch to its single target.
    let mut sink_a = ExchangeSinkRpc::new_partitioned(vec![target_a], vec!["id".into()]);
    let mut sink_b = ExchangeSinkRpc::new_partitioned(vec![target_b], vec!["id".into()]);
    sink_a.add_input(sample_batch(10)).await.unwrap();
    sink_b.add_input(sample_batch(20)).await.unwrap();
    sink_a.no_more_input().await.unwrap();
    sink_b.no_more_input().await.unwrap();
    common::wait_for_spawned_send_jobs(
        &service,
        &[desc_a.clone(), desc_b.clone()],
        2,
        Duration::from_secs(5),
    )
    .await;

    let mut src_a = ExchangeSourceOp::new(desc_a, service.clone());
    let mut src_b = ExchangeSourceOp::new(desc_b, service);

    let ba = src_a.get_output().await.unwrap().expect("a");
    let bb = src_b.get_output().await.unwrap().expect("b");
    assert_eq!(ba.num_rows(), 10);
    assert_eq!(bb.num_rows(), 20);
    h.abort();
}

#[tokio::test]
async fn exchange_descriptor_format() {
    let d = FlightDescriptor::for_task(99, 3, 5);
    assert_eq!(d.as_str(), "pylon://query/99/stage/3/task/5");
}
