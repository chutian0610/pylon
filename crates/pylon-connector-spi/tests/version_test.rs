use pylon_connector_spi::{ConnectorError, ConnectorErrorCode, SPI_VERSION, SpiVersion};

#[test]
fn exposes_version_and_shared_error_types() {
    assert_eq!(SPI_VERSION, SpiVersion::new(0, 1, 0));
    assert_eq!(SPI_VERSION.major(), 0);
    assert_eq!(SPI_VERSION.minor(), 1);
    assert_eq!(SPI_VERSION.patch(), 0);

    let error = ConnectorError::new(ConnectorErrorCode::Other, "connector failed");
    assert_eq!(error.message(), "connector failed");
}
