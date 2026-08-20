//! Connector entry point for Pylon object-storage implementations.
//!
//! Concrete S3/GCS storage behavior remains a later milestone; this crate
//! currently exposes its stable SPI identity and factory.

use async_trait::async_trait;
use pylon_connector_spi::{Connector, ConnectorConfig, ConnectorFactory, ConnectorResult};

/// The storage connector name recognized by the engine.
pub const STORAGE_CONNECTOR_NAME: &str = "storage";

/// An object-storage connector instance.
#[derive(Debug, Default)]
pub struct StorageConnector;

impl Connector for StorageConnector {
    fn name(&self) -> &str {
        STORAGE_CONNECTOR_NAME
    }
}

/// Creates object-storage connector instances.
#[derive(Debug, Default)]
pub struct StorageConnectorFactory;

#[async_trait]
impl ConnectorFactory for StorageConnectorFactory {
    fn name(&self) -> &str {
        STORAGE_CONNECTOR_NAME
    }

    async fn create(&self, _config: ConnectorConfig) -> ConnectorResult<Box<dyn Connector>> {
        Ok(Box::new(StorageConnector))
    }
}
