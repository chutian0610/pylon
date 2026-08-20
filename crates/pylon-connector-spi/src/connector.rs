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

/// A connector instance owned by the engine.
pub trait Connector: Send + Sync {
    /// Returns the stable connector name.
    fn name(&self) -> &str;
}

/// Creates connector instances from engine-supplied configuration.
#[async_trait]
pub trait ConnectorFactory: Send + Sync {
    /// Returns the connector name recognized by this factory.
    fn name(&self) -> &str;

    /// Creates a connector instance.
    async fn create(&self, config: ConnectorConfig) -> ConnectorResult<Box<dyn Connector>>;
}
