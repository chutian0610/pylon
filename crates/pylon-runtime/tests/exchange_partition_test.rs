//! A2-1: ExchangeSink partitioned mode tests.

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pylon_exchange::{FlightDescriptor, PylonFlightService};
use pylon_runtime::ops::ExchangeSinkOp;
use pylon_runtime::PipelineOp;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, true),
    ]))
}

fn mk_batch() -> RecordBatch {
    let s = schema();
    let ids = Int64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
    let names = StringArray::from(vec![
        "a", "b", "a", "c", "b", "a", "c", "b",
    ]);
    let amounts = Float64Array::from(vec![
        Some(1.0), Some(2.0), Some(3.0), Some(4.0),
        Some(5.0), Some(6.0), Some(7.0), Some(8.0),
    ]);
    RecordBatch::try_new(s, vec![Arc::new(ids), Arc::new(names), Arc::new(amounts)]).unwrap()
}

async fn drain(service: &PylonFlightService, desc: &FlightDescriptor) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    for _ in 0..20 {
        match service.pop(desc).await.unwrap() {
            Some(b) => out.push(b),
            None => break,
        }
    }
    out
}

#[tokio::test]
async fn partitioned_sink_routes_rows_by_group_by_col() {
    // 4 partitions, group by "name". 8 rows with values a, b, a, c, b, a, c, b.
    // Rows with same "name" must end up at the same partition index.
    let service = Arc::new(PylonFlightService::new());
    let n_partitions = 4;
    let descs: Vec<FlightDescriptor> = (0..n_partitions)
        .map(|i| FlightDescriptor(format!("pylon://q/1/s/2/t/{i}")))
        .collect();

    let mut sink = ExchangeSinkOp::new_partitioned(
        descs.clone(),
        vec!["name".into()],
        service.clone(),
    );
    sink.add_input(mk_batch()).await.unwrap();
    sink.no_more_input().await.unwrap();

    let mut total_rows = 0;
    let mut non_empty_partitions = 0;
    let mut name_per_partition: Vec<Vec<String>> = vec![Vec::new(); n_partitions];
    for (p, desc) in descs.iter().enumerate() {
        let batches = drain(&service, desc).await;
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        if n > 0 {
            non_empty_partitions += 1;
            for b in &batches {
                let names = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for r in 0..b.num_rows() {
                    name_per_partition[p].push(names.value(r).to_string());
                }
            }
        }
        total_rows += n;
    }
    assert_eq!(total_rows, 8, "all rows routed");
    assert!(non_empty_partitions >= 1);
    assert!(non_empty_partitions <= 3, "at most 3 partitions for {n_partitions}");

    for (p, names) in name_per_partition.iter().enumerate() {
        if names.is_empty() {
            continue;
        }
        let first = &names[0];
        for n in names {
            assert_eq!(n, first, "partition {p} should have uniform name");
        }
    }
}

#[tokio::test]
async fn partitioned_sink_split_batch_by_row() {
    let service = Arc::new(PylonFlightService::new());
    let descs: Vec<FlightDescriptor> = (0..2)
        .map(|i| FlightDescriptor(format!("pylon://q/1/s/2/t/{i}")))
        .collect();

    let mut sink = ExchangeSinkOp::new_partitioned(
        descs.clone(),
        vec!["id".into()],
        service.clone(),
    );
    sink.add_input(mk_batch()).await.unwrap();
    sink.no_more_input().await.unwrap();

    let mut total_rows = 0;
    let mut all_ids: Vec<i64> = Vec::new();
    for desc in &descs {
        for b in drain(&service, desc).await {
            let ids = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for r in 0..b.num_rows() {
                all_ids.push(ids.value(r));
                total_rows += 1;
            }
        }
    }
    assert_eq!(total_rows, 8);
    all_ids.sort();
    assert_eq!(all_ids, vec![0, 1, 2, 3, 4, 5, 6, 7]);
}

#[tokio::test]
async fn partitioned_sink_unknown_key_col_errors() {
    let service = Arc::new(PylonFlightService::new());
    let descs = vec![FlightDescriptor("pylon://q/1/s/2/t/0".into())];
    let mut sink = ExchangeSinkOp::new_partitioned(
        descs,
        vec!["nope".into()],
        service,
    );
    let err = sink.add_input(mk_batch()).await.unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
}

#[tokio::test]
async fn partitioned_sink_empty_batch_is_noop() {
    let service = Arc::new(PylonFlightService::new());
    let descs = vec![FlightDescriptor("pylon://q/1/s/2/t/0".into())];
    let mut sink = ExchangeSinkOp::new_partitioned(
        descs.clone(),
        vec!["name".into()],
        service.clone(),
    );
    let empty = RecordBatch::new_empty(schema());
    sink.add_input(empty).await.unwrap();
    sink.no_more_input().await.unwrap();
    for desc in &descs {
        let batches = drain(&service, desc).await;
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 0);
    }
}

#[tokio::test]
async fn single_descriptor_sink_still_works() {
    // Regression guard: A1 single-descriptor mode unchanged.
    let service = Arc::new(PylonFlightService::new());
    let desc = FlightDescriptor("pylon://q/1/s/1/t/0".into());
    let mut sink = ExchangeSinkOp::new(desc.clone(), service.clone());
    sink.add_input(mk_batch()).await.unwrap();
    sink.no_more_input().await.unwrap();
    let batches = drain(&service, &desc).await;
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 8, "all 8 rows in single descriptor");
}
