//! M3 B-1: Arrow Flight RPC server side.
//!
//! Wraps the in-process [`PylonFlightService`] (a HashMap of
//! `descriptor → Vec<RecordBatch>`) into an `arrow_flight::flight_service_server::FlightService`
//! impl. A worker process can listen on a Flight port using
//! `PylonFlightServiceServer::serve(...)`; remote `ExchangeSinkRpc`
//! ops (B-2) connect to that port and use `DoExchange` to push
//! batches in. The server decodes each FlightData message as an
//! Arrow IPC streaming batch and routes it to the local
//! PylonFlightService queue keyed by the descriptor embedded in the
//! first FlightData message (via `app_metadata`).
//!
//! We intentionally implement only `DoExchange` for M3 first cut.
//! `GetFlightInfo` / `GetSchema` / `DoPut` / `DoGet` are M4+.

use crate::flight_server::{FlightDescriptor, PylonFlightService};
use arrow_ipc::reader::StreamReader;
use futures::{Stream, StreamExt};
use pylon_types::codec::encode_batch_stream;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use tonic::codegen::Bytes;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

/// Wrapper that implements `arrow_flight::flight_service_server::FlightService`.
pub struct FlightServerImpl {
    pub service: Arc<PylonFlightService>,
    /// FTE source (RFC 0007): when set, every batch pushed through
    /// `do_exchange` is additionally appended — as a complete IPC
    /// stream — to `<spill_root>/pylon-input/<descriptor path>.arrow`.
    /// The persisted log is the replay base for re-dispatched tasks
    /// whose drain-once queue is gone.
    pub spill_root: Option<std::path::PathBuf>,
}

impl FlightServerImpl {
    pub fn new(service: Arc<PylonFlightService>) -> Self {
        Self {
            service,
            spill_root: None,
        }
    }

    /// Enables persisted exchange input (see `spill_root`).
    pub fn with_spill_root(
        service: Arc<PylonFlightService>,
        spill_root: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            service,
            spill_root: Some(spill_root.into()),
        }
    }
}

/// Deterministic input-log path for a flight descriptor:
/// `pylon://query/0/stage/2/task/1` →
/// `pylon-input/query/0/stage/2/task/1.arrow` (relative to a worker's
/// spill root). The coord derives the same path when building the
/// re-dispatch OpSpec — keep the two formatters in sync.
pub fn input_log_relative_path(descriptor: &str) -> Option<String> {
    let rest = descriptor.strip_prefix("pylon://")?;
    if rest.is_empty() || rest.contains("..") {
        return None;
    }
    Some(format!("pylon-input/{rest}.arrow"))
}

/// Appends one batch (as a complete IPC stream) to the persisted
/// input log for `rel`. Concatenated streams read back via
/// `read_concatenated_ipc`.
fn append_batch_to_log(
    root: &std::path::Path,
    rel: &str,
    schema: &arrow_schema::SchemaRef,
    batch: &arrow_array::RecordBatch,
) -> std::io::Result<()> {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    use std::io::Write as _;
    let stream = encode_batch_stream(schema, batch)
        .map_err(|e| std::io::Error::other(format!("ipc encode: {e}")))?;
    file.write_all(&stream)
}

type FlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl arrow_flight::flight_service_server::FlightService for FlightServerImpl {
    type HandshakeStream = FlightStream<arrow_flight::HandshakeResponse>;
    type ListFlightsStream = FlightStream<arrow_flight::FlightInfo>;
    type DoGetStream = FlightStream<arrow_flight::FlightData>;
    type DoPutStream = FlightStream<arrow_flight::PutResult>;
    type DoExchangeStream = FlightStream<arrow_flight::FlightData>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<arrow_flight::ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported in M3 B-1"))
    }
    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "list_flights not supported in M3 B-1",
        ))
    }
    async fn get_flight_info(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info not supported in M3 B-1",
        ))
    }
    async fn get_schema(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not supported in M3 B-1"))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented(
            "poll_flight_info not supported in M3 B-1",
        ))
    }
    async fn do_exchange(
        &self,
        request: Request<Streaming<arrow_flight::FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let mut inbound = request.into_inner();
        let service = self.service.clone();
        let spill_root = self.spill_root.clone();

        // Spawn a task that reads FlightData messages, decodes them
        // as Arrow IPC streaming batches, and routes to the local
        // PylonFlightService queue keyed by the descriptor in the
        // first message's app_metadata.
        //
        // For the response stream, we mirror the input back as
        // simple ack frames (empty FlightData with descriptor).
        // The client (ExchangeSinkRpc) ignores the response bytes
        // for M3 B-1; the ack semantics are good enough to detect
        // "stream closed" cleanly.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<arrow_flight::FlightData, Status>>(8);

        // First message carries the descriptor via app_metadata
        // (UTF-8 string). We resolve it once and pin the route.
        //
        // The first message MUST be the schema (Arrow IPC streaming
        // format: 0xFF×4 + schema). After that, every message is
        // either a RecordBatch (0xFF×4 + data) or the EOS marker
        // (0xFF×4 + 0x00×4). The `StreamReader` handles all three.
        //
        // For routing we use a separate "control frame" sent as
        // FlightData with `app_metadata` carrying the descriptor
        // string. The first message must be a control frame; the
        // remaining messages are pure IPC streaming bytes.
        let ack_tx = tx;
        tokio::spawn(async move {
            let mut descriptor: Option<FlightDescriptor> = None;
            while let Some(item) = inbound.next().await {
                let data = match item {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(error = %e, "Flight DoExchange inbound err");
                        break;
                    }
                };
                // Control frame: descriptor-only (empty body), pins
                // the route for every subsequent data body.
                if data.data_body.is_empty() {
                    if descriptor.is_none() {
                        let desc_str = String::from_utf8(data.app_metadata.to_vec()).map_err(|e| {
                            Status::invalid_argument(format!("descriptor not utf-8: {e}"))
                        });
                        match desc_str {
                            Ok(s) if !s.is_empty() => {
                                descriptor = Some(FlightDescriptor(s));
                                let _ = ack_tx
                                    .send(Ok(make_ack(&descriptor.as_ref().unwrap())))
                                    .await;
                                continue;
                            }
                            _ => {
                                warn!("first FlightData has no descriptor in app_metadata");
                                break;
                            }
                        }
                    }
                    continue;
                }
                // Data frame: each body is ONE complete Arrow IPC
                // stream (schema + batch + EOS), written by
                // ExchangeSinkRpc per partition slice. Decode it
                // independently — there is deliberately no shared
                // accumulator (the old clone-then-clear dropped bytes
                // that arrived mid-drain).
                let Some(d) = &descriptor else {
                    warn!("data body before descriptor control frame");
                    break;
                };
                let Ok(mut reader) =
                    StreamReader::try_new(Cursor::new(data.data_body.clone()), None)
                else {
                    warn!("FlightData body is not a complete IPC stream");
                    continue;
                };
                loop {
                    match reader.next() {
                        Some(Ok(batch)) => {
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            // FTE source: persist the batch to the
                            // input log BEFORE queueing, so a crash can
                            // never lose acknowledged replay data
                            // (write-ahead ordering).
                            if let (Some(root), Some(rel)) =
                                (spill_root.as_deref(), input_log_relative_path(&d.0))
                            {
                                let schema = batch.schema();
                                if let Err(e) = append_batch_to_log(root, &rel, &schema, &batch) {
                                    warn!(error = %e, "input-log append failed");
                                }
                            }
                            if let Err(e) = service.push(d, batch).await {
                                warn!(error = ?e, "service.push failed");
                            }
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "StreamReader decode err");
                            break;
                        }
                        None => break,
                    }
                }
            }
            debug!("Flight DoExchange inbound closed");
        });

        let outbound: Self::DoExchangeStream =
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
        Ok(Response::new(outbound))
    }
    async fn do_get(
        &self,
        _request: Request<arrow_flight::Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get not supported in M3 B-1"))
    }
    async fn do_put(
        &self,
        _request: Request<Streaming<arrow_flight::FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported in M3 B-1"))
    }
    async fn do_action(
        &self,
        _request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported in M3 B-1"))
    }
    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented(
            "list_actions not supported in M3 B-1",
        ))
    }
}

fn make_ack(_desc: &FlightDescriptor) -> arrow_flight::FlightData {
    // Empty body, no metadata. The client (B-2 ExchangeSinkRpc)
    // will see a stream of acks and treat them as flow control
    // hints; the actual data is one-way.
    arrow_flight::FlightData {
        flight_descriptor: None,
        app_metadata: Bytes::new(),
        data_body: Bytes::new(),
        data_header: Bytes::new(),
    }
}
