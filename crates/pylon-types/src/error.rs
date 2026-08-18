use thiserror::Error;

pub type Result<T> = std::result::Result<T, PylonError>;

#[derive(Debug, Error)]
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
}
