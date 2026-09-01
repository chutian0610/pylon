use futures::executor::block_on;
use pylon_connector_spi::{ConnectorConfig, ConnectorFactory};
use pylon_storage::{STORAGE_CONNECTOR_NAME, StorageConnectorFactory};

#[test]
fn factory_creates_storage_connector() {
    let factory = StorageConnectorFactory;
    let connector = block_on(factory.create(ConnectorConfig::new())).unwrap();

    assert_eq!(factory.name(), STORAGE_CONNECTOR_NAME);
    assert_eq!(connector.name(), STORAGE_CONNECTOR_NAME);
}

#[test]
fn storage_connector_is_fault_tolerant() {
    let connector = StorageConnectorFactory.create(ConnectorConfig::new());
    let connector = futures::executor::block_on(connector).unwrap();
    assert!(connector.capabilities().fault_tolerant());
}
