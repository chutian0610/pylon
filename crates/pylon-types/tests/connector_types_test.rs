use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use pylon_types::{
    ConnectorError, ConnectorErrorCode, PylonError, RecordBatch, RecordBatchStream, Result, Schema,
    SchemaRef, SendableRecordBatchStream,
};

struct EmptyBatchStream {
    schema: SchemaRef,
}

impl Stream for EmptyBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl RecordBatchStream for EmptyBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[test]
fn connector_error_maps_to_pylon_error() {
    let connector_error = ConnectorError::new(ConnectorErrorCode::NotFound, "missing table")
        .with_source(std::io::Error::new(std::io::ErrorKind::NotFound, "catalog"));

    assert_eq!(connector_error.code(), ConnectorErrorCode::NotFound);
    assert_eq!(connector_error.message(), "missing table");
    assert!(connector_error.source().is_some());

    let engine_error = PylonError::from(connector_error);
    assert!(engine_error.to_string().contains("missing table"));
}

#[test]
fn batch_stream_alias_accepts_sendable_streams() {
    let schema = Arc::new(Schema::empty());
    let stream: SendableRecordBatchStream = Box::pin(EmptyBatchStream {
        schema: Arc::clone(&schema),
    });

    assert_eq!(stream.schema(), schema);
}
