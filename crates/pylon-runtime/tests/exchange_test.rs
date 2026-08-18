use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use pylon_exchange::{FlightDescriptor, PylonFlightService};
use pylon_runtime::ops::{ExchangeSinkOp, ExchangeSourceOp};
use pylon_runtime::PipelineOp;
use std::sync::Arc;

fn sample_batch(rows: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
    ]));
    let ids = Int64Array::from((0..rows).collect::<Vec<_>>());
    let amounts = arrow_array::Float64Array::from(
        (0..rows).map(|i| i as f64 * 2.5).collect::<Vec<_>>()
    );
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(amounts)]).unwrap()
}

#[tokio::test]
async fn exchange_sink_then_source_roundtrip() {
    let service = Arc::new(PylonFlightService::new());
    let desc = FlightDescriptor::for_task(42, 1, 0);

    let mut sink = ExchangeSinkOp::new(desc.clone(), service.clone());
    sink.add_input(sample_batch(100)).await.unwrap();
    sink.add_input(sample_batch(50)).await.unwrap();
    sink.no_more_input().await.unwrap();

    let mut src = ExchangeSourceOp::new(desc, service.clone());
    let b1 = src.get_output().await.unwrap().expect("first batch");
    let b2 = src.get_output().await.unwrap().expect("second batch");
    assert_eq!(b1.num_rows(), 100);
    assert_eq!(b2.num_rows(), 50);
    src.no_more_input().await.unwrap();
    assert!(src.is_finished().await);
}

#[tokio::test]
async fn exchange_isolates_descriptors() {
    let service = Arc::new(PylonFlightService::new());
    let desc_a = FlightDescriptor::for_task(1, 1, 0);
    let desc_b = FlightDescriptor::for_task(1, 1, 1);

    let mut sink_a = ExchangeSinkOp::new(desc_a.clone(), service.clone());
    let mut sink_b = ExchangeSinkOp::new(desc_b.clone(), service.clone());

    sink_a.add_input(sample_batch(10)).await.unwrap();
    sink_b.add_input(sample_batch(20)).await.unwrap();
    sink_a.no_more_input().await.unwrap();

    let mut src_a = ExchangeSourceOp::new(desc_a, service.clone());
    let mut src_b = ExchangeSourceOp::new(desc_b, service);

    let ba = src_a.get_output().await.unwrap().expect("a");
    let bb = src_b.get_output().await.unwrap().expect("b");
    assert_eq!(ba.num_rows(), 10);
    assert_eq!(bb.num_rows(), 20);
}

#[tokio::test]
async fn exchange_descriptor_format() {
    let d = FlightDescriptor::for_task(99, 3, 5);
    assert_eq!(d.as_str(), "pylon://query/99/stage/3/task/5");
}
