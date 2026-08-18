use arrow_array::RecordBatch;
use pylon_types::PylonError;
use pylon_types::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct FlightDescriptor(pub String);

impl FlightDescriptor {
    pub fn for_task(query_id: u64, stage_id: u64, partition: usize) -> Self {
        Self(format!(
            "pylon://query/{query_id}/stage/{stage_id}/task/{partition}"
        ))
    }
    pub fn as_str(&self) -> &str { &self.0 }
}

#[derive(Default)]
pub struct PylonFlightService {
    streams: Arc<Mutex<HashMap<String, Vec<RecordBatch>>>>,
}

impl PylonFlightService {
    pub fn new() -> Self { Self::default() }

    pub async fn push(&self, descriptor: &FlightDescriptor, batch: RecordBatch) -> Result<()> {
        let mut streams = self.streams.lock().await;
        streams
            .entry(descriptor.0.clone())
            .or_insert_with(Vec::new)
            .push(batch);
        Ok(())
    }

    pub async fn pop(&self, descriptor: &FlightDescriptor) -> Result<Option<RecordBatch>> {
        let mut streams = self.streams.lock().await;
        if let Some(queue) = streams.get_mut(&descriptor.0) {
            if !queue.is_empty() {
                return Ok(Some(queue.remove(0)));
            }
        }
        Ok(None)
    }

    pub async fn pending(&self, descriptor: &FlightDescriptor) -> usize {
        let streams = self.streams.lock().await;
        streams.get(&descriptor.0).map(|q| q.len()).unwrap_or(0)
    }
}
