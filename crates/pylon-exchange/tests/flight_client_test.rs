//! Tests for `PylonFlightClient` — verifies M3 task #4c: real Arrow IPC
//! streaming batch encode (one schema, N batches, one EOS).

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use pylon_exchange::PylonFlightClient;

fn schema_a() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
    ]))
}

fn schema_b() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("other", DataType::Int64, false)]))
}

fn batch_a(rows: i64) -> RecordBatch {
    let ids = Int64Array::from((0..rows).collect::<Vec<_>>());
    let amounts = Float64Array::from((0..rows).map(|i| i as f64 * 2.5).collect::<Vec<_>>());
    RecordBatch::try_new(schema_a(), vec![Arc::new(ids), Arc::new(amounts)]).unwrap()
}

fn batch_b(rows: i64) -> RecordBatch {
    let ids = Int64Array::from((0..rows).collect::<Vec<_>>());
    RecordBatch::try_new(schema_b(), vec![Arc::new(ids)]).unwrap()
}

async fn build_client() -> PylonFlightClient {
    PylonFlightClient::connect("localhost:50061".into(), "pylon://q/1/s/1/t/0".into())
        .await
        .expect("connect")
}

/// Arrow IPC streaming EOS marker: 4-byte continuation + 4-byte zero
/// metadata length. The last 8 bytes of a well-formed stream must equal
/// this exactly.
fn has_eos_marker(buf: &[u8]) -> bool {
    buf.len() >= 8 && buf[buf.len() - 8..] == [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]
}

fn decode_stream(bytes: &[u8]) -> (arrow_schema::SchemaRef, Vec<RecordBatch>) {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).expect("stream reader");
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("collect batches");
    (schema, batches)
}

#[tokio::test]
async fn stream_well_formed_with_schema_eos_and_n_batches() {
    // Stronger than counting `0xFF` substrings: under the placeholder
    // impl each `send` produced a complete mini-stream (schema + batch +
    // EOS), so a downstream `StreamReader` would see the first batch, then
    // hit the EOS and stop — yielding only 1 batch for N sends. Here we
    // assert the reader sees all N batches back, which is only possible
    // when the schema is written exactly once and EOS exactly once.
    let client = build_client().await;
    client.send(batch_a(100)).await.unwrap();
    client.send(batch_a(50)).await.unwrap();
    client.send(batch_a(25)).await.unwrap();
    client.close().await.unwrap();
    let bytes = client.take_bytes().await;

    assert!(
        has_eos_marker(&bytes),
        "final 8 bytes must be the EOS continuation"
    );
    let (schema, batches) = decode_stream(&bytes);
    assert_eq!(schema.as_ref(), schema_a().as_ref());
    assert_eq!(batches.len(), 3, "all N batches must be decodable");
    let rows: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
    assert_eq!(rows, vec![100, 50, 25]);
}

#[tokio::test]
async fn decoded_stream_roundtrips_batches_in_order() {
    let client = build_client().await;
    client.send(batch_a(7)).await.unwrap();
    client.send(batch_a(11)).await.unwrap();
    client.send(batch_a(0)).await.unwrap(); // 0-row batch is fine in Open state
    client.send(batch_a(3)).await.unwrap();
    client.close().await.unwrap();
    let bytes = client.take_bytes().await;

    let (schema, batches) = decode_stream(&bytes);
    assert_eq!(schema.as_ref(), schema_a().as_ref());
    assert_eq!(batches.len(), 4);
    let row_counts: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
    assert_eq!(row_counts, vec![7, 11, 0, 3]);
}

#[tokio::test]
async fn schema_mismatch_is_rejected() {
    let client = build_client().await;
    client.send(batch_a(5)).await.unwrap();
    let err = client.send(batch_b(5)).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("schema mismatch"), "got error: {msg}");
}

#[tokio::test]
async fn send_after_close_is_rejected() {
    let client = build_client().await;
    client.send(batch_a(5)).await.unwrap();
    client.close().await.unwrap();
    let err = client.send(batch_a(5)).await.unwrap_err();
    assert!(err.to_string().contains("already closed"));
}

#[tokio::test]
async fn close_is_idempotent() {
    let client = build_client().await;
    client.send(batch_a(5)).await.unwrap();
    client.close().await.unwrap();
    client.close().await.unwrap(); // must not panic
    let _ = client.take_bytes().await;
}

#[tokio::test]
async fn close_with_no_sends_yields_well_formed_empty_stream() {
    let client = build_client().await;
    client.close().await.unwrap();
    let bytes = client.take_bytes().await;
    assert!(has_eos_marker(&bytes), "empty stream missing EOS");
    let (_schema, batches) = decode_stream(&bytes);
    assert!(batches.is_empty());
}

#[tokio::test]
async fn empty_batch_before_first_data_is_skipped() {
    // Empty batches in the Empty state should not open the writer or
    // establish a schema. Then a real batch must still be accepted.
    let client = build_client().await;
    client.send(batch_a(0)).await.unwrap(); // skip
    client.send(batch_a(0)).await.unwrap(); // skip
    client.send(batch_a(4)).await.unwrap();
    client.close().await.unwrap();
    let bytes = client.take_bytes().await;
    let (_schema, batches) = decode_stream(&bytes);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 4);
}

#[tokio::test]
async fn many_batches_share_a_single_schema_message() {
    // 10 batches: if a schema message were re-emitted per batch, the
    // StreamReader would error or stop at the first EOS. So getting all
    // 10 batches back through `StreamReader` is a strong proof that the
    // schema was written exactly once.
    let client = build_client().await;
    for _ in 0..10 {
        client.send(batch_a(8)).await.unwrap();
    }
    client.close().await.unwrap();
    let bytes = client.take_bytes().await;
    let (schema, batches) = decode_stream(&bytes);
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(batches.len(), 10);
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 80);
}

#[tokio::test]
async fn take_bytes_implicitly_finishes_an_open_stream() {
    // If the consumer calls take_bytes without close(), the stream must
    // still come back well-formed (with EOS).
    let client = build_client().await;
    client.send(batch_a(3)).await.unwrap();
    let bytes = client.take_bytes().await;
    assert!(has_eos_marker(&bytes));
    let (_schema, batches) = decode_stream(&bytes);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}
