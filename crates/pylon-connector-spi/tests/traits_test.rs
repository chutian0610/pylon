use async_trait::async_trait;
use futures::executor::block_on;
use pylon_connector_spi::{
    Connector, ConnectorCapabilities, ConnectorConfig, ConnectorFactory, ConnectorPage,
    ConnectorResult, DataSink, DataSource, WriteStats,
};
use pylon_types::{RecordBatch, Schema};

struct TestConnector;

impl Connector for TestConnector {
    fn name(&self) -> &str {
        "test"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::NOT_FAULT_TOLERANT
    }
}

struct TestFactory;

#[async_trait]
impl ConnectorFactory for TestFactory {
    fn name(&self) -> &str {
        "test"
    }

    async fn create(&self, config: ConnectorConfig) -> ConnectorResult<Box<dyn Connector>> {
        assert_eq!(config.get("endpoint"), Some("memory://"));
        Ok(Box::new(TestConnector))
    }
}

#[derive(Default)]
struct EmptySource;

impl DataSource for EmptySource {
    fn next(&mut self) -> ConnectorResult<Option<ConnectorPage>> {
        Ok(None)
    }
}

#[derive(Default)]
struct CountingSink {
    rows: u64,
}

impl DataSink for CountingSink {
    fn append(&mut self, page: ConnectorPage) -> ConnectorResult<()> {
        self.rows += page.num_rows() as u64;
        Ok(())
    }

    fn finish(&mut self) -> ConnectorResult<WriteStats> {
        Ok(WriteStats::new(self.rows, 0))
    }
}

#[test]
fn connector_traits_are_object_safe() {
    let factory: Box<dyn ConnectorFactory> = Box::new(TestFactory);
    let config = ConnectorConfig::new().with_property("endpoint", "memory://");
    let connector = block_on(factory.create(config)).unwrap();

    assert_eq!(factory.name(), "test");
    assert_eq!(connector.name(), "test");
}

#[test]
fn source_and_sink_use_connector_pages() {
    let mut source: Box<dyn DataSource> = Box::new(EmptySource);
    assert!(source.next().unwrap().is_none());

    let page = ConnectorPage::new(RecordBatch::new_empty(Schema::empty().into()));
    assert_eq!(page.num_rows(), 0);

    let mut sink: Box<dyn DataSink> = Box::new(CountingSink::default());
    sink.append(page).unwrap();
    let stats = sink.finish().unwrap();
    assert_eq!(stats.rows(), 0);
    assert_eq!(stats.bytes(), 0);
}
