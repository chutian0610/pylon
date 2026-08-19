use futures::executor::block_on;
use pylon_connector_spi::{ConnectorConfig, ConnectorFactory};
use pylon_iceberg::{ICEBERG_CONNECTOR_NAME, IcebergConnectorFactory};

#[test]
fn factory_creates_iceberg_connector() {
    let factory = IcebergConnectorFactory;
    let connector = block_on(factory.create(ConnectorConfig::new())).unwrap();

    assert_eq!(factory.name(), ICEBERG_CONNECTOR_NAME);
    assert_eq!(connector.name(), ICEBERG_CONNECTOR_NAME);
}
