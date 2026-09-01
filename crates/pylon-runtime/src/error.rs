use pylon_types::PylonError;
use thiserror::Error;

pub type RuntimeResult<T> = std::result::Result<T, RuntimeError>;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("pylon types: {0}")]
    Types(#[from] PylonError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("channel closed unexpectedly")]
    ChannelClosed,

    #[error("op execution failed: {0}")]
    Op(String),

    #[error("internal: {0}")]
    Internal(String),
}
