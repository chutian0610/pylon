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

use std::io::Cursor;
use std::sync::Arc;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use pylon_connector_spi::{ConnectorPage, ConnectorResult, DataSink, DataSource, WriteStats};

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
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

fn object_store_err(context: &str, e: object_store::Error) -> ConnectorError {
    ConnectorError::new(ConnectorErrorCode::Io, format!("s3 {context}: {e}"))
}

// ---------------------------------------------------------------------------
// S3-backed DataSink / DataSource
// ---------------------------------------------------------------------------

/// Arrow IPC `DataSink` that buffers IPC bytes in memory and issues a
/// single `PutObject` on `finish` (RFC 0007 §5 M4.S4: "local-FS-like
/// put/get"). Chunked / multipart writes are a later optimization.
pub struct S3DataSink {
    store: S3SpillStore,
    key: String,
    schema: SchemaRef,
    buffer: Vec<u8>,
    writer: Option<StreamWriter<Vec<u8>>>,
    rows_written: u64,
    finished: bool,
}

impl std::fmt::Debug for S3DataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3DataSink")
            .field("key", &self.key)
            .field("buffered_bytes", &self.buffer.len())
            .field("rows_written", &self.rows_written)
            .finish_non_exhaustive()
    }
}

impl S3DataSink {
    /// Creates a sink that will upload Arrow IPC streaming bytes to
    /// `key` in the given `store` on `finish()`.
    pub fn new(store: S3SpillStore, key: impl Into<String>, schema: SchemaRef) -> Self {
        let key = key.into();
        // Write to an in-memory buffer; `finish` uploads the result.
        let writer = StreamWriter::try_new(Vec::new(), &schema)
            .expect("in-memory Arrow IPC writer cannot fail");
        Self {
            store,
            key,
            schema,
            buffer: Vec::new(),
            writer: Some(writer),
            rows_written: 0,
            finished: false,
        }
    }

    /// Returns the S3 object key this sink will write to.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the sink's Arrow schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl DataSink for S3DataSink {
    fn append(&mut self, page: ConnectorPage) -> ConnectorResult<()> {
        let batch = page.into_batch();
        self.rows_written += batch.num_rows() as u64;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| store_err(format!("s3 sink {}: already finished", self.key)))?;
        writer.write(&batch).map_err(|e| {
            ConnectorError::new(ConnectorErrorCode::Other, format!("arrow ipc write: {e}"))
        })?;
        Ok(())
    }

    fn finish(&mut self) -> ConnectorResult<WriteStats> {
        if self.finished {
            return Err(store_err(format!("s3 sink {}: already finished", self.key)));
        }
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| store_err(format!("s3 sink {}: already finished", self.key)))?;
        writer.finish().map_err(|e| {
            ConnectorError::new(ConnectorErrorCode::Other, format!("arrow ipc finish: {e}"))
        })?;
        self.buffer = writer.into_inner().map_err(|e| {
            ConnectorError::new(ConnectorErrorCode::Other, format!("arrow ipc flush: {e}"))
        })?;
        let bytes = self.buffer.len() as u64;
        self.store
            .put(&self.key, std::mem::take(&mut self.buffer))?;
        self.finished = true;
        Ok(WriteStats::new(self.rows_written, bytes))
    }

    fn abort(&mut self) -> ConnectorResult<()> {
        self.writer.take();
        self.finished = true;
        // Nothing was uploaded, so there's nothing to clean up in S3.
        Ok(())
    }
}

/// Arrow IPC `DataSource` that downloads the full object on creation
/// and yields batches one at a time (RFC 0007 §5 M4.S4: "put/get").
pub struct S3DataSource {
    key: String,
    reader: Option<StreamReader<Cursor<Vec<u8>>>>,
    completed_bytes: u64,
    completed_rows: u64,
    cancelled: bool,
}

impl std::fmt::Debug for S3DataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3DataSource")
            .field("key", &self.key)
            .field("completed_bytes", &self.completed_bytes)
            .field("completed_rows", &self.completed_rows)
            .finish_non_exhaustive()
    }
}

impl S3DataSource {
    /// Creates a source that downloads Arrow IPC bytes from `key`
    /// in the given `store`.
    pub fn new(store: S3SpillStore, key: impl Into<String>) -> ConnectorResult<Self> {
        let key = key.into();
        let bytes = store.get(&key)?;
        let completed_bytes = bytes.len() as u64;
        let cursor = Cursor::new(bytes);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            ConnectorError::new(ConnectorErrorCode::Other, format!("arrow ipc read: {e}"))
        })?;
        Ok(Self {
            key,
            reader: Some(reader),
            completed_bytes,
            completed_rows: 0,
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
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| store_err(format!("s3 source {}: already exhausted", self.key)))?;
        match reader.next() {
            Some(Ok(batch)) => {
                self.completed_rows += batch.num_rows() as u64;
                Ok(Some(ConnectorPage::new(batch)))
            }
            Some(Err(e)) => Err(ConnectorError::new(
                ConnectorErrorCode::Other,
                format!("arrow ipc decode: {e}"),
            )),
            None => {
                self.reader.take();
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

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// A handle to an S3-compatible bucket. All methods are synchronous;
/// async I/O runs on the internal blocking runtime.
#[derive(Debug, Clone)]
pub struct S3SpillStore {
    store: Arc<dyn ObjectStore>,
}

impl S3SpillStore {
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
        block_on(self.store.put(&path, payload)).map_err(|e| object_store_err("put", e))?;
        Ok(())
    }

    /// Reads the full object at `key`.
    pub fn get(&self, key: &str) -> ConnectorResult<Vec<u8>> {
        let path = ObjectPath::from(key);
        let result = block_on(self.store.get(&path)).map_err(|e| object_store_err("get", e))?;
        let bytes = block_on(result.bytes()).map_err(|e| object_store_err("get body", e))?;
        Ok(bytes.to_vec())
    }

    /// Deletes the object at `key`. Succeeds if the object is already
    /// gone (S3 semantics: DELETE on missing key is a no-op).
    pub fn delete(&self, key: &str) -> ConnectorResult<()> {
        let path = ObjectPath::from(key);
        block_on(self.store.delete(&path)).map_err(|e| object_store_err("delete", e))?;
        Ok(())
    }

    /// Returns `true` if the object exists (best-effort via HEAD).
    pub fn exists(&self, key: &str) -> ConnectorResult<bool> {
        let path = ObjectPath::from(key);
        match block_on(self.store.head(&path)) {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(object_store_err("head", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
