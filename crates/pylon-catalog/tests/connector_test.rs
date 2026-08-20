use futures::executor::block_on;
use pylon_catalog::{CATALOG_CONNECTOR_NAME, CatalogConnectorFactory};
use pylon_connector_spi::{ConnectorConfig, ConnectorFactory};

#[test]
fn factory_creates_catalog_connector() {
    let factory = CatalogConnectorFactory;
    let connector = block_on(factory.create(ConnectorConfig::new())).unwrap();

    assert_eq!(factory.name(), CATALOG_CONNECTOR_NAME);
    assert_eq!(connector.name(), CATALOG_CONNECTOR_NAME);
}
