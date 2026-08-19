use thiserror::Error;

pub type Result<T> = std::result::Result<T, PylonError>;

/// A stable, machine-readable connector failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectorErrorCode {
    /// A requested catalog object, table, split, or file was not found.
    NotFound,
    /// Connector input or configuration was invalid.
    InvalidArgument,
    /// An underlying storage or network I/O operation failed.
    Io,
    /// Connector and engine schemas were incompatible.
    Schema,
    /// The connector does not implement the requested operation.
    Unimplemented,
    /// A connector-side resource limit was reached.
    ResourceExhausted,
    /// A connector failure that does not fit another stable category.
    Other,
}

/// An error returned across the connector SPI boundary.
#[derive(Debug, Error)]
#[error("{code:?}: {message}")]
#[non_exhaustive]
pub struct ConnectorError {
    code: ConnectorErrorCode,
    message: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl ConnectorError {
    /// Creates a connector error without an underlying source.
    pub fn new(code: ConnectorErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
        }
    }

    /// Returns the stable error category.
    pub fn code(&self) -> ConnectorErrorCode {
        self.code
    }

    /// Returns the connector-provided error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Attaches the underlying connector or storage error.
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PylonError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("parquet error: {0}")]
    Parquet(String),

    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("invalid plan: {0}")]
    InvalidPlan(String),

    #[error("internal: {0}")]
    Internal(String),

    #[error("connector error: {0}")]
    External(#[from] ConnectorError),
}
