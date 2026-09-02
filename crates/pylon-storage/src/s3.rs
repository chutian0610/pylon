//! S3-compatible object storage backend (RFC 0007 §5 M4.S4).
//!
//! Wraps `object_store::aws::AmazonS3` behind a synchronous
//! `put` / `get` / `delete` surface. The connector SPI traits
//! (`DataSink` / `DataSource`) are sync; the backing store is
//! async. A dedicated single-thread tokio runtime bridges the two
//! without interfering with the engine's own runtime.
//!
//! MinIO, Cloudflare R2, and any S3-compatible endpoint work by
//! passing `allow_http: true` with the endpoint URL.

use std::sync::Arc;
use std::time::Duration;

use arrow_schema::SchemaRef;
use pylon_connector_spi::{ConnectorPage, ConnectorResult, DataSink, DataSource, WriteStats};
use pylon_types::codec::{encode_batch_stream, read_concatenated_ipc};

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};
use once_cell::sync::Lazy;
use pylon_types::{ConnectorError, ConnectorErrorCode, PylonError, Result};

/// A dedicated runtime for blocking S3 calls from sync SPI methods.
/// Single-threaded + current-thread reactor; never shared with the
/// engine's async runtime, so `block_on` is safe from any context.
static S3_RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("pylon-storage: failed to build S3 blocking runtime")
});

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    S3_RUNTIME.block_on(fut)
}

/// Per-operation S3 deadlines. A hung endpoint (network partition,
/// MinIO overload) must fail the op within these bounds instead of
/// blocking the calling thread forever.
const PUT_TIMEOUT: Duration = Duration::from_secs(30);
const GET_TIMEOUT: Duration = Duration::from_secs(60);
const DELETE_TIMEOUT: Duration = Duration::from_secs(15);
const HEAD_TIMEOUT: Duration = Duration::from_secs(10);

fn timeout_err(op: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorCode::Io, format!("s3 {op}: timed out"))
}

fn block_on_timeout<T>(
    op: &str,
    deadline: Duration,
    fut: impl std::future::Future<Output = object_store::Result<T>>,
) -> ConnectorResult<T> {
    // The timeout future must be *constructed* inside the runtime:
    // `tokio::time::timeout` registers a timer eagerly on creation,
    // so building it outside `block_on` panics with "no reactor".
    block_on(async move {
        match tokio::time::timeout(deadline, fut).await {
            Ok(res) => res.map_err(|e| object_store_err(op, e)),
            Err(_) => Err(timeout_err(op)),
        }
    })
}

/// Connection settings for an S3-compatible object store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    /// Endpoint URL, e.g. `http://minio.internal:9000` or the AWS URL.
    pub endpoint: String,
    /// Bucket name (all spill keys live under this bucket).
    pub bucket: String,
    /// S3 access key ID.
    pub access_key_id: String,
    /// S3 secret access key.
    pub secret_access_key: String,
    /// Allow plain-HTTP endpoints (required for MinIO / on-prem).
    pub allow_http: bool,
}

impl S3Config {
    /// Config for plain-HTTP endpoints (MinIO, on-prem S3).
    pub fn http(endpoint: impl Into<String>, bucket: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            allow_http: true,
        }
    }

    /// Sets credentials.
    pub fn with_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.access_key_id = access_key_id.into();
        self.secret_access_key = secret_access_key.into();
        self
    }
}

fn store_err(context: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorCode::Io, context.into())
}

fn codec_err(e: PylonError) -> ConnectorError {
    ConnectorError::new(ConnectorErrorCode::Other, e.to_string())
}

fn object_store_err(context: &str, e: object_store::Error) -> ConnectorError {
    ConnectorError::new(ConnectorErrorCode::Io, format!("s3 {context}: {e}"))
}

/// Default multipart part size: 8 MiB — above the 5 MiB S3 minimum
/// and a fixed size, as R2 and similar stores require equal-sized
/// parts (except the last).
pub const DEFAULT_S3_PART_SIZE: usize = 8 * 1024 * 1024;

/// S3 protocol floor: every part except the last must be >= 5 MiB.
/// MinIO and AWS both reject smaller completes (`EntityTooSmall`).
pub const MIN_S3_PART_SIZE: usize = 5 * 1024 * 1024;

// ---------------------------------------------------------------------------
// S3-backed DataSink / DataSource
// ---------------------------------------------------------------------------

/// Arrow IPC `DataSink` backed by S3 multipart upload. Batches are
/// encoded as complete per-batch IPC streams; buffered bytes flush as
/// fixed-size parts (`DEFAULT_S3_PART_SIZE`), so steady-state memory
/// is bounded by one part regardless of spill size, and the 5 GB
/// single-PUT cap no longer applies (C5.6).
pub struct S3DataSink {
    store: S3SpillStore,
    key: String,
    schema: SchemaRef,
    upload: Option<Box<dyn MultipartUpload>>,
    /// IPC bytes buffered toward the next fixed-size part.
    pending: Vec<u8>,
    chunk_size: usize,
    parts_uploaded: usize,
    bytes_flushed: u64,
    rows_written: u64,
    finished: bool,
}

impl std::fmt::Debug for S3DataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3DataSink")
            .field("key", &self.key)
            .field("pending_bytes", &self.pending.len())
            .field("parts_uploaded", &self.parts_uploaded)
            .field("rows_written", &self.rows_written)
            .finish_non_exhaustive()
    }
}

impl S3DataSink {
    /// Creates a sink that streams Arrow IPC bytes to `key` via
    /// multipart upload. S3 resources open immediately; `finish`
    /// finalizes the object.
    pub fn new(
        store: S3SpillStore,
        key: impl Into<String>,
        schema: SchemaRef,
    ) -> ConnectorResult<Self> {
        Self::with_part_size(store, key, schema, DEFAULT_S3_PART_SIZE)
    }

    /// Like [`S3DataSink::new`] with an explicit part size. Values
    /// below the S3 5 MiB protocol floor are clamped up — smaller
    /// parts make the final `CompleteMultipartUpload` fail with
    /// `EntityTooSmall` on AWS and MinIO alike.
    pub fn with_part_size(
        store: S3SpillStore,
        key: impl Into<String>,
        schema: SchemaRef,
        part_size: usize,
    ) -> ConnectorResult<Self> {
        let key = key.into();
        let upload = store.open_multipart(&key)?;
        Ok(Self {
            store,
            key,
            schema,
            upload: Some(upload),
            pending: Vec::new(),
            chunk_size: part_size.max(MIN_S3_PART_SIZE),
            parts_uploaded: 0,
            bytes_flushed: 0,
            rows_written: 0,
            finished: false,
        })
    }

    /// Overrides the part size after construction (clamped to the
    /// 5 MiB protocol floor, see [`MIN_S3_PART_SIZE`]).
    pub fn with_chunk_size(mut self, part_size: usize) -> Self {
        self.chunk_size = part_size.max(MIN_S3_PART_SIZE);
        self
    }

    /// Returns the S3 object key this sink will write to.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the sink's Arrow schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn flush_one_part(&mut self, part: Vec<u8>) -> ConnectorResult<()> {
        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| store_err(format!("s3 sink {}: already finished", self.key)))?;
        let bytes = part.len() as u64;
        block_on_timeout(
            "put_part",
            PUT_TIMEOUT,
            upload.put_part(PutPayload::from(part)),
        )?;
        self.parts_uploaded += 1;
        self.bytes_flushed += bytes;
        Ok(())
    }
}

impl DataSink for S3DataSink {
    fn append(&mut self, page: ConnectorPage) -> ConnectorResult<()> {
        let batch = page.into_batch();
        self.rows_written += batch.num_rows() as u64;
        let stream = encode_batch_stream(&self.schema, &batch).map_err(codec_err)?;
        self.pending.extend_from_slice(&stream);
        while self.pending.len() >= self.chunk_size {
            let part: Vec<u8> = self.pending.drain(..self.chunk_size).collect();
            self.flush_one_part(part)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> ConnectorResult<WriteStats> {
        if self.finished {
            return Err(store_err(format!("s3 sink {}: already finished", self.key)));
        }
        // Take ownership so complete() can consume the upload.
        let mut upload = self
            .upload
            .take()
            .ok_or_else(|| store_err(format!("s3 sink {}: already finished", self.key)))?;
        // Final part may be any size (including zero-byte edge case,
        // which S3 rejects for multipart — guard with is_empty).
        if !self.pending.is_empty() {
            let last = std::mem::take(&mut self.pending);
            let bytes = last.len() as u64;
            block_on_timeout(
                "put_part",
                PUT_TIMEOUT,
                upload.put_part(PutPayload::from(last)),
            )?;
            self.parts_uploaded += 1;
            self.bytes_flushed += bytes;
        } else if self.parts_uploaded == 0 {
            // Degenerate: no batches at all. Complete an empty
            // multipart is invalid; fall back to a single PUT of an
            // empty schema-only stream is unnecessary — encode the
            // schema header so the object is at least readable.
            let stream = encode_batch_stream(&self.schema, &{
                use arrow_array::RecordBatch;
                RecordBatch::new_empty(self.schema.clone())
            })
            .map_err(codec_err)?;
            self.store.put(&self.key, stream)?;
            self.finished = true;
            return Ok(WriteStats::new(self.rows_written, 0));
        }
        block_on_timeout("complete", PUT_TIMEOUT, upload.complete())?;
        self.finished = true;
        Ok(WriteStats::new(self.rows_written, self.bytes_flushed))
    }

    fn abort(&mut self) -> ConnectorResult<()> {
        if let Some(mut upload) = self.upload.take() {
            // Best-effort: a failed abort leaves server-side parts,
            // which the bucket lifecycle rule (see C5.6 notes) or an
            // incomplete-partition sweep reaps.
            let _ = block_on_timeout("abort", DELETE_TIMEOUT, upload.abort());
        }
        self.pending.clear();
        self.finished = true;
        Ok(())
    }
}

/// Arrow IPC `DataSource` that downloads the full object on creation
/// and yields batches one at a time (RFC 0007 §5 M4.S4: "put/get").
pub struct S3DataSource {
    key: String,
    /// Remaining decoded batches. The whole object is downloaded on
    /// creation ("put/get" per RFC 0007 §5 M4.S4); batches arrive
    /// decoded in order across all concatenated IPC streams.
    pending: std::collections::VecDeque<arrow_array::RecordBatch>,
    completed_bytes: u64,
    completed_rows: u64,
    exhausted: bool,
    cancelled: bool,
}

impl std::fmt::Debug for S3DataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3DataSource")
            .field("key", &self.key)
            .field("pending_batches", &self.pending.len())
            .field("completed_bytes", &self.completed_bytes)
            .field("completed_rows", &self.completed_rows)
            .finish_non_exhaustive()
    }
}

impl S3DataSource {
    /// Creates a source that downloads Arrow IPC bytes from `key`
    /// in the given `store`. Accepts single-stream objects and the
    /// concatenated per-batch streams the multipart sink writes.
    pub fn new(store: S3SpillStore, key: impl Into<String>) -> ConnectorResult<Self> {
        let key = key.into();
        let bytes = store.get(&key)?;
        let completed_bytes = bytes.len() as u64;
        let pending = read_concatenated_ipc(bytes).map_err(codec_err)?.into();
        Ok(Self {
            key,
            pending,
            completed_bytes,
            completed_rows: 0,
            exhausted: false,
            cancelled: false,
        })
    }

    /// Returns the S3 object key this source reads from.
    pub fn key(&self) -> &str {
        &self.key
    }
}

impl DataSource for S3DataSource {
    fn next(&mut self) -> ConnectorResult<Option<ConnectorPage>> {
        if self.cancelled {
            return Ok(None);
        }
        match self.pending.pop_front() {
            Some(batch) => {
                self.completed_rows += batch.num_rows() as u64;
                Ok(Some(ConnectorPage::new(batch)))
            }
            None => {
                self.exhausted = true;
                Ok(None)
            }
        }
    }

    fn completed_bytes(&self) -> u64 {
        self.completed_bytes
    }

    fn completed_rows(&self) -> u64 {
        self.completed_rows
    }

    fn cancel(&mut self) {
        self.cancelled = true;
    }
}

/// A handle to an S3-compatible bucket. All methods are synchronous;
/// async I/O runs on the internal blocking runtime.
#[derive(Debug, Clone)]
pub struct S3SpillStore {
    store: Arc<dyn ObjectStore>,
}

impl S3SpillStore {
    /// Opens a multipart upload session for `key`. Used by the
    /// multipart sink; each part upload is individually timeout-
    /// bounded.
    pub(crate) fn open_multipart(&self, key: &str) -> ConnectorResult<Box<dyn MultipartUpload>> {
        let path = ObjectPath::from(key);
        block_on_timeout(
            "put_multipart",
            PUT_TIMEOUT,
            self.store.put_multipart(&path),
        )
    }

    /// Lists object keys under `prefix` (GC / diagnostics).
    pub fn list(&self, prefix: &str) -> ConnectorResult<Vec<String>> {
        let path = ObjectPath::from(prefix);
        let store = self.store.clone();
        block_on(async move {
            use futures::StreamExt;
            let mut keys = Vec::new();
            let mut stream = Box::pin(store.list(Some(&path)));
            while let Some(meta) = stream.next().await {
                let meta = meta.map_err(|e| object_store_err("list", e))?;
                keys.push(meta.location.to_string());
            }
            Ok(keys)
        })
    }

    /// Deletes every object under `prefix`. Returns how many keys
    /// were removed. This is the store-level half of orphan spill GC
    /// (C5.6): the deployment configures the bucket lifecycle rule
    /// or calls this sweep on query completion.
    pub fn delete_prefix(&self, prefix: &str) -> ConnectorResult<usize> {
        let keys = self.list(prefix)?;
        let mut removed = 0;
        for key in &keys {
            self.delete(key)?;
            removed += 1;
        }
        Ok(removed)
    }

    /// Connects to the S3-compatible endpoint described by `config`.
    pub fn connect(config: &S3Config) -> Result<Self> {
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&config.endpoint)
            .with_bucket_name(&config.bucket)
            .with_allow_http(config.allow_http);
        if !config.access_key_id.is_empty() {
            builder = builder
                .with_access_key_id(&config.access_key_id)
                .with_secret_access_key(&config.secret_access_key);
        }
        let store = builder
            .build()
            .map_err(|e| PylonError::Internal(format!("s3 connect: {e}")))?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Writes `bytes` to `key`, replacing any existing object.
    pub fn put(&self, key: &str, bytes: Vec<u8>) -> ConnectorResult<()> {
        let path = ObjectPath::from(key);
        let payload = PutPayload::from(bytes);
        block_on_timeout("put", PUT_TIMEOUT, self.store.put(&path, payload))?;
        Ok(())
    }

    /// Reads the full object at `key`.
    pub fn get(&self, key: &str) -> ConnectorResult<Vec<u8>> {
        let path = ObjectPath::from(key);
        let result = block_on_timeout("get", GET_TIMEOUT, self.store.get(&path))?;
        let bytes = block_on_timeout("get body", GET_TIMEOUT, result.bytes())?;
        Ok(bytes.to_vec())
    }

    /// Deletes the object at `key`. Succeeds if the object is already
    /// gone (S3 semantics: DELETE on missing key is a no-op).
    pub fn delete(&self, key: &str) -> ConnectorResult<()> {
        let path = ObjectPath::from(key);
        block_on_timeout("delete", DELETE_TIMEOUT, self.store.delete(&path))?;
        Ok(())
    }

    /// Returns `true` if the object exists (best-effort via HEAD).
    pub fn exists(&self, key: &str) -> ConnectorResult<bool> {
        let path = ObjectPath::from(key);
        let fut = self.store.head(&path);
        // Timeout must be constructed inside the runtime (see
        // `block_on_timeout`).
        match block_on(async move { tokio::time::timeout(HEAD_TIMEOUT, fut).await }) {
            Ok(Ok(_)) => Ok(true),
            Ok(Err(object_store::Error::NotFound { .. })) => Ok(false),
            Ok(Err(e)) => Err(object_store_err("head", e)),
            Err(_) => Err(timeout_err("head")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn s3_config_defaults() {
        let cfg = S3Config::http("http://localhost:9000", "test");
        assert_eq!(cfg.endpoint, "http://localhost:9000");
        assert_eq!(cfg.bucket, "test");
        assert!(cfg.allow_http);
        assert!(cfg.access_key_id.is_empty());
    }

    #[test]
    fn s3_config_with_credentials() {
        let cfg = S3Config::http("http://localhost:9000", "test").with_credentials("ak", "sk");
        assert_eq!(cfg.access_key_id, "ak");
        assert_eq!(cfg.secret_access_key, "sk");
    }

    fn batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    /// C5.6: the multipart sink concatenates complete per-batch IPC
    /// streams; the reader must loop over them. Also covers plain
    /// single-stream objects (backward compatibility).
    #[test]
    fn read_concatenated_ipc_roundtrip() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

        // Two separate complete streams.
        let s1 = encode_batch_stream(&schema, &batch(&schema, &[1, 2])).unwrap();
        let s2 = encode_batch_stream(&schema, &batch(&schema, &[3])).unwrap();

        // Plain single-stream file (local-FS sink format).
        let mut single = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut single, &schema).unwrap();
            w.write(&batch(&schema, &[7, 8, 9])).unwrap();
            w.finish().unwrap();
        }

        let single_out = read_concatenated_ipc(single).unwrap();
        assert_eq!(single_out.len(), 1);
        assert_eq!(single_out[0].num_rows(), 3);

        let concat_out = read_concatenated_ipc([s1, s2].concat()).unwrap();
        assert_eq!(concat_out.len(), 2, "two batches across two streams");
        assert_eq!(concat_out[0].num_rows(), 2);
        assert_eq!(concat_out[1].num_rows(), 1);
    }
}
