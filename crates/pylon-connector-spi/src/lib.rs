//! `pylon-connector-spi` — the stable connector surface for Pylon.
//!
//! See [`docs/rfcs/0005-pipeline-trait-surface.md`] (§ 1 *Module layout*,
//! § 3 *Domain boundaries*, § 4 *Trait signatures* rows 4 / 4a / 4b / 10)
//! for the full design. This crate is the *only* surface connector
//! crates (such as `pylon-catalog`, `pylon-storage`, `pylon-iceberg`) may
//! depend on; in return, this crate depends *only* on the leaf value
//! types in `pylon-types` plus the Arrow family.
//!
//! The `connectors-belong-here-not-in-engine` boundary-check script at
//! [`tools/check-spi-boundaries.sh`] verifies:
//!
//! * `pylon-connector-spi`'s `Cargo.toml` does not depend on any engine
//!   crate (`pylon-plan`, `pylon-runtime`, `pylon-coord`,
//!   `pylon-worker`, `pylon-exchange`).
//! * No `use pylon_…` import inside this crate's source references an
//!   engine crate.
//!
//! The connector traits land separately from the value-type foundation
//! so their object-safe method surface can be reviewed independently.
//!
//! [`docs/rfcs/0005-pipeline-trait-surface.md`]: ../../docs/rfcs/0005-pipeline-trait-surface.md
//! [`tools/check-spi-boundaries.sh`]: ../../tools/check-spi-boundaries.sh

#![warn(missing_docs)]

mod connector;
mod page;
mod source;

pub use pylon_types::{
    ConnectorError, ConnectorErrorCode, RecordBatchStream, SendableRecordBatchStream,
};

pub use connector::{Connector, ConnectorConfig, ConnectorFactory};
pub use page::ConnectorPage;
pub use source::{DataSink, DataSource, WriteStats};

/// A result returned by connector SPI operations.
pub type ConnectorResult<T> = std::result::Result<T, ConnectorError>;

/// A semantic version for the stable connector interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct SpiVersion {
    major: u16,
    minor: u16,
    patch: u16,
}

impl SpiVersion {
    /// Creates an SPI version.
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Returns the major version.
    pub const fn major(self) -> u16 {
        self.major
    }

    /// Returns the minor version.
    pub const fn minor(self) -> u16 {
        self.minor
    }

    /// Returns the patch version.
    pub const fn patch(self) -> u16 {
        self.patch
    }
}

/// The connector interface version implemented by this crate.
pub const SPI_VERSION: SpiVersion = SpiVersion::new(0, 2, 0);
