use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;
use pylon_types::{PylonError, Result};
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Arrow IPC streaming producer for the M3 Flight client.
///
/// Owns a single `StreamWriter<Vec<u8>>` so the on-the-wire bytes form a
/// well-formed Arrow IPC *streaming* format (one schema message + N
/// RecordBatch messages + one EOS). The previous placeholder re-built a
/// `StreamWriter` per batch, which produced N independent streams and
/// re-emitted the schema / EOS for every batch — M3 task #4c replaces that
/// with a persistent writer.
pub struct PylonFlightClient {
    pub endpoint: String,
    pub descriptor: String,
    state: Mutex<StreamState>,
    /// Tracks whether `close()` was invoked, so `Drop` can warn on
    /// half-finished streams. Held in a separate Mutex to avoid
    /// `UnsafeCell` plumbing.
    closed: Mutex<bool>,
}

enum StreamState {
    /// No batches sent yet. Schema will be fixed by the first non-empty
    /// batch.
    Empty,
    /// Stream open with a fixed schema. Subsequent batches must match.
    Open {
        writer: StreamWriter<Vec<u8>>,
        schema: SchemaRef,
    },
    /// `close()` already wrote the EOS marker; no further writes.
    Closed(StreamWriter<Vec<u8>>),
}

impl PylonFlightClient {
    pub async fn connect(endpoint: String, descriptor: String) -> Result<Self> {
        // M3 task #4c: still defer the real Flight RPC; we cache the encoded
        // IPC stream so downstream Flight transport can drain it.
        Ok(Self {
            endpoint,
            descriptor,
            state: Mutex::new(StreamState::Empty),
            closed: Mutex::new(false),
        })
    }

    /// Append a batch to the IPC stream. The schema is fixed on the first
    /// non-empty batch; later batches must match it.
    pub async fn send(&self, batch: RecordBatch) -> Result<()> {
        if *self.closed.lock().await {
            return Err(PylonError::Internal(format!(
                "flight client already closed: endpoint={}",
                self.endpoint
            )));
        }
        let mut state = self.state.lock().await;
        match &mut *state {
            StreamState::Empty => {
                // Skip empty batches until we see real data — opening the
                // writer on an empty batch would lock in an empty schema.
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                let schema = batch.schema();
                let mut writer = StreamWriter::try_new(Vec::<u8>::new(), schema.as_ref())
                    .map_err(|e| PylonError::Internal(format!("ipc writer: {e}")))?;
                writer
                    .write(&batch)
                    .map_err(|e| PylonError::Internal(format!("ipc write: {e}")))?;
                *state = StreamState::Open { writer, schema };
                debug!(
                    rows = batch.num_rows(),
                    schema = ?batch.schema(),
                    descriptor = %self.descriptor,
                    "flight client opened stream"
                );
                Ok(())
            }
            StreamState::Open { writer, schema } => {
                if schema.as_ref() != batch.schema().as_ref() {
                    return Err(PylonError::InvalidPlan(format!(
                        "schema mismatch on flight stream: expected={}, got={}",
                        schema, batch.schema()
                    )));
                }
                writer
                    .write(&batch)
                    .map_err(|e| PylonError::Internal(format!("ipc write: {e}")))?;
                debug!(
                    rows = batch.num_rows(),
                    descriptor = %self.descriptor,
                    "flight client appended batch"
                );
                Ok(())
            }
            StreamState::Closed(_) => Err(PylonError::Internal(format!(
                "flight client already closed: endpoint={}",
                self.endpoint
            ))),
        }
    }

    /// Mark the stream as finished: write the EOS marker exactly once.
    /// Idempotent — calling `close()` twice is a no-op.
    pub async fn close(&self) -> Result<()> {
        {
            let mut state = self.state.lock().await;
            match &mut *state {
                StreamState::Empty => {
                    // No batches were ever sent. Emit an empty stream that
                    // consumers can still parse (schema + EOS with no
                    // batches in between). We open the writer with a trivial
                    // placeholder schema and never call write().
                    *state = StreamState::Closed(empty_stream_writer()?);
                }
                StreamState::Open { writer, .. } => {
                    writer
                        .finish()
                        .map_err(|e| PylonError::Internal(format!("ipc finish: {e}")))?;
                    // Replace the writer in-place with a placeholder, then
                    // move the finished one into the Closed variant.
                    let placeholder = empty_stream_writer()?;
                    let finished = std::mem::replace(writer, placeholder);
                    *state = StreamState::Closed(finished);
                }
                StreamState::Closed(_) => {
                    // Idempotent close; nothing to do.
                }
            }
        }
        *self.closed.lock().await = true;
        debug!(endpoint = %self.endpoint, "flight client closed");
        Ok(())
    }

    /// Drain the encoded IPC streaming bytes (schema + batches + EOS) so
    /// the eventual Flight RPC transport can ship them as a single body.
    /// If the stream is still open this implicitly calls `finish()` so the
    /// consumer sees a complete stream (with EOS).
    pub async fn take_bytes(&self) -> Vec<u8> {
        let mut state = self.state.lock().await;
        let buf = match &mut *state {
            StreamState::Empty => Vec::new(),
            StreamState::Open { writer, .. } => {
                if let Err(e) = writer.finish() {
                    warn!(error = %e, endpoint = %self.endpoint, "ipc finish on take failed");
                }
                let placeholder = match empty_stream_writer() {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(error = %e, "ipc placeholder failed");
                        return Vec::new();
                    }
                };
                let owned = std::mem::replace(writer, placeholder);
                let buf = match owned.into_inner() {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "ipc into_inner failed");
                        Vec::new()
                    }
                };
                *state = StreamState::Empty;
                buf
            }
            StreamState::Closed(w) => {
                let placeholder = match empty_stream_writer() {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(error = %e, "ipc placeholder failed");
                        return Vec::new();
                    }
                };
                let owned = std::mem::replace(w, placeholder);
                let buf = match owned.into_inner() {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "ipc into_inner failed on closed");
                        Vec::new()
                    }
                };
                *state = StreamState::Empty;
                buf
            }
        };
        *self.closed.lock().await = true;
        buf
    }
}

impl Drop for PylonFlightClient {
    fn drop(&mut self) {
        // Synchronous check: if close/take_bytes never ran, warn. We can't
        // await here, so we just inspect the bool without blocking; the
        // worst case is a stale read of `true` after close, which is
        // benign (no warning emitted, which is the right behavior).
        if let Ok(guard) = self.closed.try_lock() {
            if !*guard {
                warn!(endpoint = %self.endpoint, "flight client dropped without close");
            }
        }
    }
}

/// Construct a placeholder `StreamWriter<Vec<u8>>` around an empty Vec. We
/// need this in a couple of spots to satisfy `mem::replace`. The
/// `StreamWriter` API requires a schema at construction, so we build a
/// trivial single-null column schema.
fn empty_stream_writer() -> Result<StreamWriter<Vec<u8>>> {
    use arrow_schema::{DataType, Field, Schema};
    let schema = Schema::new(vec![Field::new("__pylon_placeholder", DataType::Null, true)]);
    StreamWriter::try_new(Vec::<u8>::new(), &schema)
        .map_err(|e| PylonError::Internal(format!("ipc placeholder writer: {e}")))
}
