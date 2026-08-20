//! Connector entry point for Pylon catalog implementations.
//!
//! The Iceberg REST catalog client remains a later milestone; this crate
//! currently exposes its stable SPI identity and factory.

use async_trait::async_trait;
use pylon_connector_spi::{Connector, ConnectorConfig, ConnectorFactory, ConnectorResult};

/// The catalog connector name recognized by the engine.
pub const CATALOG_CONNECTOR_NAME: &str = "catalog";

/// A catalog connector instance.
#[derive(Debug, Default)]
pub struct CatalogConnector;

impl Connector for CatalogConnector {
    fn name(&self) -> &str {
        CATALOG_CONNECTOR_NAME
    }
}

/// Creates catalog connector instances.
#[derive(Debug, Default)]
pub struct CatalogConnectorFactory;

#[async_trait]
impl ConnectorFactory for CatalogConnectorFactory {
    fn name(&self) -> &str {
        CATALOG_CONNECTOR_NAME
    }

    async fn create(&self, _config: ConnectorConfig) -> ConnectorResult<Box<dyn Connector>> {
        Ok(Box::new(CatalogConnector))
    }
}
