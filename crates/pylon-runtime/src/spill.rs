//! `SpillManager` + `Spillable` trait — connector-backed spill lifecycle.
//!
//! Defined by RFC 0007 §3.2 + §3.3 (trait + file layout) and
//! §5 S2 (the first cut — local FS, single-op spill-and-resume
//! end-to-end). The S3+ phases reroute spill bytes through the
//! `Connector::supports_fte()` path (RFC 0007 §3.4); this file is the
//! placeholder until then.
//!
//! ## File layout
//!
//! Per RFC 0007 §3.3, a spill file is one Arrow IPC streaming
//! message: schema + N `RecordBatch` messages + EOS marker. Filenames
//! for S3 are rooted at the manager's `root` dir and named
//! `spill-<seq>.arrow`. All I/O routes through `pylon-storage`'s
//! `DataSink::append` / `DataSource::next` trait objects per RFC
//! 0007 §2 rule [b]; the manager never touches the file system
//! directly. C4 (S3) swaps the backing store without changing this
//! contract.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use pylon_connector_spi::{ConnectorPage, DataSink, DataSource};
use pylon_storage::s3::{S3DataSink, S3DataSource, S3SpillStore};
use pylon_storage::{create_spill_sink, create_spill_source, delete_spill};
use pylon_types::Result;
use tracing::debug;

/// An opaque reference to a spilled file. Pass back to
/// [`SpillManager::read`] to recover the batches, or to
/// [`SpillManager::delete`] to free the file.
#[derive(Debug, Clone)]
pub struct SpillHandle {
    /// Filesystem path to the spill file.
    pub path: PathBuf,
    /// Number of bytes written to the file at spill time.
    pub bytes: u64,
    /// Sequence number of this spill within the manager (for
    /// diagnostics only; not a load-bearing identifier).
    pub seq: usize,
}

/// The `Spillable` operator contract (RFC 0007 §3.2). Live in
/// `pylon-runtime` (not `pylon-types`) because the methods take
/// `&SpillManager`, which is a concrete runtime type.
pub trait Spillable {
    #[allow(async_fn_in_trait)]
    /// Persist the current working set to `manager`. Returns a handle
    /// that can be passed back to `resume` later.
    async fn spill(&mut self, manager: &SpillManager) -> Result<SpillHandle>;

    #[allow(async_fn_in_trait)]
    /// Reload previously-spilled batches from `handle` and fold them
    /// into the op's in-memory state. Idempotent over `handle` —
    /// the underlying file is consumed once.
    async fn resume(&mut self, manager: &SpillManager, handle: SpillHandle) -> Result<()>;
}

/// Spill manager with pluggable storage backend. `SpillManager::new`
/// uses the local file system; `SpillManager::with_s3` routes spill
/// bytes to an S3-compatible object store (RFC 0007 §5 M4.S4).
#[derive(Debug)]
pub struct SpillManager {
    root: PathBuf,
    next_seq: AtomicUsize,
    s3: Option<S3SpillStore>,
}

impl SpillManager {
    /// Construct a local-FS manager that writes spill files under
    /// `root`. The directory is created if it doesn't exist.
    pub fn new(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            next_seq: AtomicUsize::new(0),
            s3: None,
        })
    }

    /// Construct an S3-backed manager. `root` is the object-key
    /// prefix (e.g. `pylon-spill/q42/stage0/task0`); spill files land
    /// at `${root}/spill-<seq>.arrow` inside the bucket.
    pub fn with_s3(root: impl Into<PathBuf>, store: S3SpillStore) -> Self {
        Self {
            root: root.into(),
            next_seq: AtomicUsize::new(0),
            s3: Some(store),
        }
    }

    /// Returns `true` if this manager writes to S3.
    pub fn is_s3(&self) -> bool {
        self.s3.is_some()
    }

    /// Returns the directory under which spill files live.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn open_sink(
        s3: Option<&S3SpillStore>,
        path: &Path,
        schema: SchemaRef,
    ) -> Result<Box<dyn DataSink>> {
        if let Some(store) = s3 {
            let sink = S3DataSink::new(store.clone(), path.to_string_lossy(), schema)
                .map_err(pylon_types::PylonError::from)?;
            Ok(Box::new(sink))
        } else {
            create_spill_sink(path, schema).map_err(pylon_types::PylonError::from)
        }
    }

    fn open_source(s3: Option<&S3SpillStore>, path: &Path) -> Result<Box<dyn DataSource>> {
        if let Some(store) = s3 {
            let source = S3DataSource::new(store.clone(), path.to_string_lossy())
                .map_err(pylon_types::PylonError::from)?;
            Ok(Box::new(source))
        } else {
            create_spill_source(path).map_err(pylon_types::PylonError::from)
        }
    }

    fn remove(s3: Option<&S3SpillStore>, path: &Path) -> Result<()> {
        if let Some(store) = s3 {
            store
                .delete(&path.to_string_lossy())
                .map_err(pylon_types::PylonError::from)
        } else {
            delete_spill(path)
        }
    }

    /// Write `batches` as one Arrow IPC streaming file at
    /// `${root}/spill-<seq>.arrow` and return a handle to it. The
    /// file is *not* deleted on read; call `delete(handle)` after
    /// `resume` has incorporated the data. All writes go through the
    /// connector `DataSink` (RFC 0007 §2 rule [b]).
    pub fn spill(&self, schema: SchemaRef, batches: &[RecordBatch]) -> Result<SpillHandle> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let path = self.root.join(format!("spill-{seq}.arrow"));

        let mut sink = Self::open_sink(self.s3.as_ref(), &path, schema)?;
        let write_result = (|| -> Result<u64> {
            for b in batches {
                sink.append(ConnectorPage::new(b.clone()))
                    .map_err(pylon_types::PylonError::from)?;
            }
            Ok(sink
                .finish()
                .map_err(pylon_types::PylonError::from)?
                .bytes())
        })();
        let bytes = match write_result {
            Ok(bytes) => bytes,
            Err(e) => {
                // Roll back the partial write so a failed spill does
                // not leave an orphan file / object behind.
                let _ = sink.abort();
                return Err(e);
            }
        };
        debug!(?path, seq, bytes, "spilled");
        Ok(SpillHandle { path, bytes, seq })
    }

    /// Read back batches from a spill file through the connector
    /// `DataSource` (RFC 0007 §2 rule [b]).
    pub fn read(&self, handle: &SpillHandle) -> Result<Vec<RecordBatch>> {
        let mut source = Self::open_source(self.s3.as_ref(), &handle.path)?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        {
            while let Some(page) = source.next().map_err(pylon_types::PylonError::from)? {
                batches.push(page.into_batch());
            }
        }
        debug!(?handle.path, count = batches.len(), "spill resumed");
        Ok(batches)
    }

    /// Unlink a spill file. Idempotent: missing file is not an error.
    pub fn delete(&self, handle: &SpillHandle) -> Result<()> {
        Self::remove(self.s3.as_ref(), &handle.path)
    }

    /// Async variant of [`SpillManager::spill`]. RFC 0007 §2 rule
    /// [c]: the engine must not block a tokio worker on object
    /// storage, so the blocking I/O runs on the spawn_blocking pool
    /// and the driver awaits completion.
    pub async fn spill_async(
        &self,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<SpillHandle> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let path = self.root.join(format!("spill-{seq}.arrow"));
        let s3 = self.s3.clone();
        tokio::task::spawn_blocking(move || -> Result<SpillHandle> {
            let mut sink = Self::open_sink(s3.as_ref(), &path, schema)?;
            let write_result = (|| -> Result<u64> {
                for b in &batches {
                    sink.append(ConnectorPage::new(b.clone()))
                        .map_err(pylon_types::PylonError::from)?;
                }
                Ok(sink
                    .finish()
                    .map_err(pylon_types::PylonError::from)?
                    .bytes())
            })();
            let bytes = match write_result {
                Ok(bytes) => bytes,
                Err(e) => {
                    let _ = sink.abort();
                    return Err(e);
                }
            };
            debug!(?path, seq, bytes, "spilled");
            Ok(SpillHandle { path, bytes, seq })
        })
        .await
        .map_err(|e| pylon_types::PylonError::Internal(format!("spill task: {e}")))?
    }

    /// Async variant of [`SpillManager::read`] (see `spill_async`).
    pub async fn read_async(&self, handle: SpillHandle) -> Result<Vec<RecordBatch>> {
        let s3 = self.s3.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>> {
            let mut source = Self::open_source(s3.as_ref(), &handle.path)?;
            let mut batches: Vec<RecordBatch> = Vec::new();
            while let Some(page) = source.next().map_err(pylon_types::PylonError::from)? {
                batches.push(page.into_batch());
            }
            debug!(?handle.path, count = batches.len(), "spill resumed");
            Ok(batches)
        })
        .await
        .map_err(|e| pylon_types::PylonError::Internal(format!("spill read task: {e}")))?
    }

    /// Async variant of [`SpillManager::delete`] (see `spill_async`).
    pub async fn delete_async(&self, handle: SpillHandle) -> Result<()> {
        let s3 = self.s3.clone();
        tokio::task::spawn_blocking(move || Self::remove(s3.as_ref(), &handle.path))
            .await
            .map_err(|e| pylon_types::PylonError::Internal(format!("spill delete task: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
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

    #[test]
    fn spill_single_batch_roundtrip() {
        // Smoke test: a single batch with no edge cases.
        let tmp = std::env::temp_dir().join(format!(
            "pylon-spill-test-{}-single-batch",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let mgr = SpillManager::new(&tmp).unwrap();
        let b = batch(&[1, 2, 3], &["a", "b", "c"]);
        let handle = mgr.spill(schema(), std::slice::from_ref(&b)).unwrap();
        let back = mgr.read(&handle).unwrap();
        assert_eq!(back.len(), 1, "expected exactly 1 batch back");
        assert_eq!(back[0].num_rows(), 3);
        mgr.delete(&handle).unwrap();
    }

    #[test]
    fn spill_and_read_roundtrip() {
        let tmp = std::env::temp_dir().join(format!(
            "pylon-spill-test-{}-roundtrip-2batches",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let mgr = SpillManager::new(&tmp).unwrap();

        let b1 = batch(&[1, 2, 3], &["a", "b", "c"]);
        let b2 = batch(&[4, 5], &["d", "e"]);
        let handle = mgr.spill(schema(), &[b1, b2]).unwrap();
        assert!(handle.path.exists());
        assert!(handle.bytes > 0);

        let back = mgr.read(&handle).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].num_rows(), 3);
        assert_eq!(back[1].num_rows(), 2);

        mgr.delete(&handle).unwrap();
        assert!(!handle.path.exists());
    }

    #[test]
    fn delete_missing_is_ok() {
        let tmp = std::env::temp_dir().join(format!(
            "pylon-spill-test-{}-delete-missing",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let mgr = SpillManager::new(&tmp).unwrap();
        let handle = SpillHandle {
            path: tmp.join("nonexistent.arrow"),
            bytes: 0,
            seq: 999,
        };
        assert!(mgr.delete(&handle).is_ok());
    }

    #[test]
    fn seq_increments_across_spills() {
        let tmp = std::env::temp_dir().join(format!(
            "pylon-spill-test-{}-seq-increments",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let mgr = SpillManager::new(&tmp).unwrap();
        let b = batch(&[1], &["x"]);
        let h1 = mgr.spill(schema(), std::slice::from_ref(&b)).unwrap();
        let h2 = mgr.spill(schema(), &[b]).unwrap();
        assert_eq!(h1.seq, 0);
        assert_eq!(h2.seq, 1);
        mgr.delete(&h1).unwrap();
        mgr.delete(&h2).unwrap();
    }
}
