use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use pylon_types::PylonError;
use pylon_types::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

pub struct PylonFlightClient {
    pub endpoint: String,
    pub descriptor: String,
    pub writer: Mutex<Option<StreamWriter<Vec<u8>>>>,
    pub bytes: Arc<Mutex<Vec<u8>>>,
    pub finished: bool,
}

impl PylonFlightClient {
    pub async fn connect(endpoint: String, descriptor: String) -> Result<Self> {
        // M3 first cut: defer the real Flight RPC; we just cache state.
        Ok(Self {
            endpoint,
            descriptor,
            writer: Mutex::new(None),
            bytes: Arc::new(Mutex::new(Vec::new())),
            finished: false,
        })
    }

    pub async fn send(&mut self, batch: RecordBatch) -> Result<()> {
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
                .map_err(|e| PylonError::Internal(format!("ipc writer: {e}")))?;
            writer.write(&batch).map_err(|e| PylonError::Internal(format!("ipc write: {e}")))?;
            writer.finish().map_err(|e| PylonError::Internal(format!("ipc finish: {e}")))?;
        }
        debug!(
            bytes = buf.len(),
            descriptor = %self.descriptor,
            "encoded IPC chunk"
        );
        self.bytes.lock().await.extend_from_slice(&buf);
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.finished = true;
        debug!(endpoint = %self.endpoint, "flight client closed");
        Ok(())
    }

    /// Pull all encoded bytes (for use by the eventual Flight RPC client).
    pub async fn take_bytes(&self) -> Vec<u8> {
        let mut buf = self.bytes.lock().await;
        std::mem::take(&mut *buf)
    }
}

impl Drop for PylonFlightClient {
    fn drop(&mut self) {
        if !self.finished {
            warn!(endpoint = %self.endpoint, "flight client dropped without close");
        }
    }
}
