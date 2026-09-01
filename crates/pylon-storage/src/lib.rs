//! `pylon-storage` — object-storage connector for Pylon.
//!
//! C3 (RFC 0007 §3.4 / §5 M4.S3): `StorageConnector` reports
//! `fault_tolerant: true` and the crate provides local-FS
//! `DataSink` / `DataSource` impls that write / read Arrow IPC
//! streaming files. The SpillManager in `pylon-runtime` routes all
//! spill I/O through these trait objects (RFC 0007 §2 rule [b]).
//! C4 (M4.S4) swaps the backing store for `object_store`-based
//! `s3://` without touching the `DataSink` / `DataSource` surface.

pub mod s3;

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use pylon_connector_spi::{
    Connector, ConnectorCapabilities, ConnectorConfig, ConnectorFactory, ConnectorPage,
    ConnectorResult, DataSink, DataSource, WriteStats,
};
use pylon_types::{ConnectorError, ConnectorErrorCode, PylonError, Result};

fn io_err(context: &std::fmt::Arguments<'_>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorCode::Io, context.to_string())
}

fn arrow_err(e: arrow_schema::ArrowError) -> ConnectorError {
    ConnectorError::new(ConnectorErrorCode::Other, format!("arrow ipc: {e}"))
}

/// The storage connector name recognized by the engine.
pub const STORAGE_CONNECTOR_NAME: &str = "storage";

/// An object-storage connector instance.
#[derive(Debug, Default)]
pub struct StorageConnector;

impl Connector for StorageConnector {
    fn name(&self) -> &str {
        STORAGE_CONNECTOR_NAME
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        // RFC 0007 §3.4: local-FS spill is a valid fault-tolerant
        // path; S3 arrives in M4.S4 behind the same flag.
        ConnectorCapabilities::FAULT_TOLERANT
    }
}

/// Creates object-storage connector instances.
#[derive(Debug, Default)]
pub struct StorageConnectorFactory;

#[async_trait]
impl ConnectorFactory for StorageConnectorFactory {
    fn name(&self) -> &str {
        STORAGE_CONNECTOR_NAME
    }

    async fn create(&self, _config: ConnectorConfig) -> ConnectorResult<Box<dyn Connector>> {
        Ok(Box::new(StorageConnector))
    }
}

/// Arrow IPC streaming `DataSink` backed by a local file. Each
/// `append` writes one `RecordBatch` IPC message; `finish` writes
/// the EOS marker and flushes.
pub struct StorageDataSink {
    path: PathBuf,
    writer: Option<StreamWriter<File>>,
    schema: SchemaRef,
    bytes_written: AtomicU64,
    rows_written: u64,
}

impl std::fmt::Debug for StorageDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageDataSink")
            .field("path", &self.path)
            .field("bytes_written", &self.bytes_written.load(Ordering::Relaxed))
            .field("rows_written", &self.rows_written)
            .finish_non_exhaustive()
    }
}

impl StorageDataSink {
    /// Creates a sink that writes Arrow IPC streaming to `path`.
    pub fn new(path: impl Into<PathBuf>, schema: SchemaRef) -> ConnectorResult<Self> {
        let path = path.into();
        let file = File::create(&path).map_err(|e| {
            io_err(&format_args!(
                "creating storage sink {}: {e}",
                path.display()
            ))
        })?;
        let writer = StreamWriter::try_new(file, &schema).map_err(arrow_err)?;
        Ok(Self {
            path,
            writer: Some(writer),
            schema,
            bytes_written: AtomicU64::new(0),
            rows_written: 0,
        })
    }

    /// Returns the backing file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the sink's Arrow schema.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl DataSink for StorageDataSink {
    fn append(&mut self, page: ConnectorPage) -> ConnectorResult<()> {
        let batch = page.into_batch();
        self.rows_written += batch.num_rows() as u64;
        let writer = self.writer.as_mut().ok_or_else(|| {
            io_err(&format_args!(
                "storage sink {}: already finished",
                self.path.display()
            ))
        })?;
        writer.write(&batch).map_err(arrow_err)?;
        Ok(())
    }

    fn finish(&mut self) -> ConnectorResult<WriteStats> {
        let mut writer = self.writer.take().ok_or_else(|| {
            io_err(&format_args!(
                "storage sink {}: already finished",
                self.path.display()
            ))
        })?;
        writer.finish().map_err(arrow_err)?;
        drop(writer);
        let bytes = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        self.bytes_written.store(bytes, Ordering::Relaxed);
        Ok(WriteStats::new(self.rows_written, bytes))
    }

    fn abort(&mut self) -> ConnectorResult<()> {
        self.writer.take();
        let _ = std::fs::remove_file(&self.path);
        Ok(())
    }
}

/// Arrow IPC streaming `DataSource` backed by a local file. Each
/// `next` returns one `RecordBatch` page; `None` at EOS.
pub struct StorageDataSource {
    path: PathBuf,
    reader: Option<StreamReader<BufReader<File>>>,
    completed_bytes: u64,
    completed_rows: u64,
    cancelled: bool,
}

impl std::fmt::Debug for StorageDataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageDataSource")
            .field("path", &self.path)
            .field("completed_bytes", &self.completed_bytes)
            .field("completed_rows", &self.completed_rows)
            .finish_non_exhaustive()
    }
}

impl StorageDataSource {
    /// Creates a source that reads Arrow IPC streaming from `path`.
    pub fn new(path: impl Into<PathBuf>) -> ConnectorResult<Self> {
        let path = path.into();
        let file = File::open(&path).map_err(|e| {
            io_err(&format_args!(
                "opening storage source {}: {e}",
                path.display()
            ))
        })?;
        let reader = StreamReader::try_new(BufReader::new(file), None).map_err(arrow_err)?;
        Ok(Self {
            path,
            reader: Some(reader),
            completed_bytes: 0,
            completed_rows: 0,
            cancelled: false,
        })
    }

    /// Returns the backing file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DataSource for StorageDataSource {
    fn next(&mut self) -> ConnectorResult<Option<ConnectorPage>> {
        if self.cancelled {
            return Ok(None);
        }
        let reader = self.reader.as_mut().ok_or_else(|| {
            io_err(&format_args!(
                "storage source {}: already exhausted",
                self.path.display()
            ))
        })?;
        match reader.next() {
            Some(Ok(batch)) => {
                self.completed_rows += batch.num_rows() as u64;
                Ok(Some(ConnectorPage::new(batch)))
            }
            Some(Err(e)) => Err(arrow_err(e)),
            None => {
                self.completed_bytes = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
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

/// Creates a spill-capable `DataSink` at `path` for `schema`.
pub fn create_spill_sink(
    path: impl Into<PathBuf>,
    schema: SchemaRef,
) -> ConnectorResult<Box<dyn DataSink>> {
    Ok(Box::new(StorageDataSink::new(path, schema)?))
}

/// Creates a spill-capable `DataSource` reading from `path`.
pub fn create_spill_source(path: impl Into<PathBuf>) -> ConnectorResult<Box<dyn DataSource>> {
    Ok(Box::new(StorageDataSource::new(path)?))
}

/// Removes a spill file. Idempotent: missing file is not an error.
pub fn delete_spill(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(PylonError::Io(std::io::Error::new(
            e.kind(),
            format!("removing spill file {}: {e}", path.display()),
        ))),
    }
}

/// Convenience: true if `RecordBatch`es were written to `path` and
/// the file exists. Used by the SpillManager to verify the sink.
pub fn spill_exists(path: &Path) -> bool {
    path.exists()
}
