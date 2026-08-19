//! Connector entry point for Pylon Iceberg table implementations.
//!
//! Iceberg readers and writers remain a later milestone; this crate
//! currently exposes its stable SPI identity and factory.

use async_trait::async_trait;
use pylon_connector_spi::{Connector, ConnectorConfig, ConnectorFactory, ConnectorResult};

/// The Iceberg connector name recognized by the engine.
pub const ICEBERG_CONNECTOR_NAME: &str = "iceberg";

/// An Iceberg connector instance.
#[derive(Debug, Default)]
pub struct IcebergConnector;

impl Connector for IcebergConnector {
    fn name(&self) -> &str {
        ICEBERG_CONNECTOR_NAME
    }
}

/// Creates Iceberg connector instances.
#[derive(Debug, Default)]
pub struct IcebergConnectorFactory;

#[async_trait]
impl ConnectorFactory for IcebergConnectorFactory {
    fn name(&self) -> &str {
        ICEBERG_CONNECTOR_NAME
    }

    async fn create(&self, _config: ConnectorConfig) -> ConnectorResult<Box<dyn Connector>> {
        Ok(Box::new(IcebergConnector))
    }
}
