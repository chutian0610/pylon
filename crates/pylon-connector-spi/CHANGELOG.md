# Changelog

All notable changes to the stable connector interface are documented here.

## 0.2.0 - 2026-08-19

- Added object-safe `Connector` and async `ConnectorFactory` traits.
- Added `ConnectorConfig`, `ConnectorPage`, `DataSource`, `DataSink`, and `WriteStats`.
- Added the `ConnectorResult` alias.

## 0.1.0 - 2026-08-19

- Added `SpiVersion` and the `SPI_VERSION` compile-time version constant.
- Re-exported connector errors and record-batch stream types from `pylon-types`.
