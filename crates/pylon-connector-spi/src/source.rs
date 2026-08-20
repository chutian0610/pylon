use crate::{ConnectorPage, ConnectorResult};

/// A driver-owned source of connector pages.
pub trait DataSource: Send {
    /// Returns the next page, or `None` at end-of-stream.
    fn next(&mut self) -> ConnectorResult<Option<ConnectorPage>>;

    /// Returns the estimated size of one output row, in bytes.
    fn estimated_row_size(&self) -> usize {
        0
    }

    /// Returns the number of bytes read so far.
    fn completed_bytes(&self) -> u64 {
        0
    }

    /// Returns the number of rows produced so far.
    fn completed_rows(&self) -> u64 {
        0
    }

    /// Requests cancellation of further reads.
    fn cancel(&mut self) {}
}

/// A driver-owned sink for connector pages.
pub trait DataSink: Send {
    /// Appends one page to the connector.
    fn append(&mut self, page: ConnectorPage) -> ConnectorResult<()>;

    /// Finishes the write and returns its statistics.
    fn finish(&mut self) -> ConnectorResult<WriteStats>;

    /// Aborts the write.
    fn abort(&mut self) -> ConnectorResult<()> {
        Ok(())
    }
}

/// Connector-reported statistics for a completed write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteStats {
    rows: u64,
    bytes: u64,
}

impl WriteStats {
    /// Creates completed-write statistics.
    pub const fn new(rows: u64, bytes: u64) -> Self {
        Self { rows, bytes }
    }

    /// Returns the number of written rows.
    pub const fn rows(self) -> u64 {
        self.rows
    }

    /// Returns the number of written bytes.
    pub const fn bytes(self) -> u64 {
        self.bytes
    }
}
