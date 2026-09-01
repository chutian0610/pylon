use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::ConnectorResult;

/// Connector configuration supplied by the engine.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectorConfig {
    properties: BTreeMap<String, String>,
}

impl ConnectorConfig {
    /// Creates an empty connector configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces one connector property.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Returns a configured property.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(String::as_str)
    }
}

/// Static capability flags reported by a connector (RFC 0007 §3.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectorCapabilities {
    /// True iff this connector can be the destination of spill files
    /// and FTE-persisted exchange output.
    fault_tolerant: bool,
}

impl ConnectorCapabilities {
    /// Capabilities for a connector that supports fault-tolerant
    /// spill/exchange persistence.
    pub const FAULT_TOLERANT: Self = Self {
        fault_tolerant: true,
    };

    /// Capabilities for a connector without fault-tolerant support.
    pub const NOT_FAULT_TOLERANT: Self = Self {
        fault_tolerant: false,
    };

    /// Returns the `fault_tolerant` flag.
    pub const fn fault_tolerant(self) -> bool {
        self.fault_tolerant
    }
}

impl Default for ConnectorCapabilities {
    fn default() -> Self {
        Self::NOT_FAULT_TOLERANT
    }
}

/// A connector instance owned by the engine.
pub trait Connector: Send + Sync {
    /// Returns the stable connector name.
    fn name(&self) -> &str;

    /// Returns the static capability flags for this connector.
    fn capabilities(&self) -> ConnectorCapabilities;
}

/// Creates connector instances from engine-supplied configuration.
#[async_trait]
pub trait ConnectorFactory: Send + Sync {
    /// Returns the connector name recognized by this factory.
    fn name(&self) -> &str;

    /// Creates a connector instance.
    async fn create(&self, config: ConnectorConfig) -> ConnectorResult<Box<dyn Connector>>;
}
