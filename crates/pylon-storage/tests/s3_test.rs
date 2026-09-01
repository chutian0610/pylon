//! S3 integration tests (MinIO / any S3-compatible endpoint).
//!
//! These tests are `#[ignore]`d by default so CI passes without a
//! live object store. Run locally against the MinIO instance:
//!
//! ```sh
//! cargo test -p pylon-storage --test s3_test -- --ignored
//! ```

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use pylon_connector_spi::{ConnectorPage, DataSink, DataSource};
use pylon_storage::s3::{S3Config, S3DataSink, S3DataSource, S3SpillStore};
use std::sync::Arc;

fn minio_config() -> S3Config {
    S3Config::http("http://10.96.77.251:9000", "test")
        .with_credentials("8j7CU0ksYuFwpz71", "08jOANOYvJv3ePaFm07qyuSz0JUy9qHx")
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

fn test_key() -> String {
    format!("pylon-test/s3-sink-source-{}.arrow", std::process::id())
}

#[test]
#[ignore]
fn s3_put_get_roundtrip() {
    let store = S3SpillStore::connect(&minio_config()).unwrap();
    let key = "pylon-test/put-get-roundtrip.bin";
    let payload = b"hello pylon s3";

    store.put(key, payload.to_vec()).unwrap();
    assert!(store.exists(key).unwrap());

    let back = store.get(key).unwrap();
    assert_eq!(back, payload);

    store.delete(key).unwrap();
    assert!(!store.exists(key).unwrap());
}

#[test]
#[ignore]
fn s3_data_sink_source_roundtrip() {
    let store = S3SpillStore::connect(&minio_config()).unwrap();
    let key = test_key();
    let b1 = batch(&[1, 2, 3], &["a", "b", "c"]);
    let b2 = batch(&[4, 5], &["d", "e"]);

    let mut sink = S3DataSink::new(store.clone(), &key, schema()).unwrap();
    sink.append(ConnectorPage::new(b1)).unwrap();
    sink.append(ConnectorPage::new(b2)).unwrap();
    let stats = sink.finish().unwrap();
    assert!(stats.bytes() > 0);
    assert_eq!(stats.rows(), 5);

    assert!(store.exists(&key).unwrap());

    let mut source = S3DataSource::new(store.clone(), &key).unwrap();
    let out1 = source.next().unwrap().unwrap();
    assert_eq!(out1.num_rows(), 3);
    let out2 = source.next().unwrap().unwrap();
    assert_eq!(out2.num_rows(), 2);
    assert!(source.next().unwrap().is_none());
    assert_eq!(source.completed_rows(), 5);

    store.delete(&key).unwrap();
    assert!(!store.exists(&key).unwrap());
}

/// C5.6: multipart streaming with a tiny part size forces multiple
/// parts, and the object reads back through the concatenated-IPC
/// path. Also exercises `delete_prefix` (the store-level orphan GC).
#[test]
#[ignore]
fn s3_multipart_multi_chunk_roundtrip_and_gc() {
    let store = S3SpillStore::connect(&minio_config()).unwrap();
    let key = format!("pylon-test/s3-multipart-{}.arrow", std::process::id());
    let schema = schema();

    // Real protocol floor: parts must be >= 5 MiB (except the last).
    // Two ~6 MiB batches => a full 5 MiB part + a final part.
    let big_batch = |seed: i64| {
        let n = 300_000usize;
        let ids: Vec<i64> = (0..n as i64).map(|i| seed + i).collect();
        let names: Vec<String> = (0..n).map(|i| format!("row-{seed}-{i:07}")).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        batch(&ids, &name_refs)
    };
    let mut sink =
        S3DataSink::with_part_size(store.clone(), &key, schema, 5 * 1024 * 1024).unwrap();
    sink.append(ConnectorPage::new(big_batch(0))).unwrap();
    sink.append(ConnectorPage::new(big_batch(50_000_000)))
        .unwrap();
    let stats = sink.finish().unwrap();
    assert_eq!(stats.rows(), 600_000);
    assert!(
        stats.bytes() > 5 * 1024 * 1024,
        "expected >=2 parts: {} bytes flushed",
        stats.bytes()
    );

    assert!(store.exists(&key).unwrap());
    let mut source = S3DataSource::new(store.clone(), &key).unwrap();
    let mut rows = 0usize;
    while let Some(page) = source.next().unwrap() {
        rows += page.num_rows();
    }
    assert_eq!(rows, 600_000, "all batches survive chunked roundtrip");

    // GC: list + delete_prefix on a directory-style prefix (the
    // production spill layout is `pylon-spill/<query_id>/...`).
    let dir = format!("pylon-test/gc-{}/", std::process::id());
    let key = format!("{dir}spill-0.arrow");
    store.put(&key, b"gc-target".to_vec()).unwrap();
    let listed = store.list(&dir).unwrap();
    assert_eq!(listed.len(), 1, "listed before GC");
    let removed = store.delete_prefix(&dir).unwrap();
    assert_eq!(removed, 1);
    assert!(!store.exists(&key).unwrap());
}

#[test]
#[ignore]
fn s3_delete_missing_is_ok() {
    let store = S3SpillStore::connect(&minio_config()).unwrap();
    // S3 DELETE on a missing key succeeds (no-op per protocol).
    store
        .delete("pylon-test/does-not-exist-anything.arrow")
        .unwrap();
}
