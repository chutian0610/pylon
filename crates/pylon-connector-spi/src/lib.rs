//! `pylon-connector-spi` — the stable connector surface for Pylon.
//!
//! See [`docs/rfcs/0005-pipeline-trait-surface.md`] (§ 1 *Module layout*,
//! § 3 *Domain boundaries*, § 4 *Trait signatures* rows 4 / 4a / 4b / 10)
//! for the full design. This crate is the *only* surface connector
//! crates (such as `pylon-catalog`, `pylon-storage`, `pylon-iceberg`) may
//! depend on; in return, this crate depends *only* on the leaf value
//! types in `pylon-types` plus the Arrow family.
//!
//! ## Status
//!
//! This is **R0** of the refactor sequence in RFC 0005 § 7: the crate
//! is intentionally empty. Its only job right now is to make the
//! dependency-graph axiom (see rule #1 in RFC 0005 § 3) fail-fast in
//! CI. A `connectors-belong-here-not-in-engine` boundary-check script
//! lives at [`tools/check-spi-boundaries.sh`] and verifies:
//!
//! * `pylon-connector-spi`'s `Cargo.toml` does not depend on any engine
//!   crate (`pylon-plan`, `pylon-runtime`, `pylon-coord`,
//!   `pylon-worker`, `pylon-exchange`).
//! * No `use pylon_…` import inside this crate's source references an
//!   engine crate.
//!
//! ## What's coming
//!
//! **R1** fills this crate with the connector-facing value types and
//! traits: `Connector`, `ConnectorFactory`, `DataSource`, `DataSink`,
//! `ConnectorError`, `ConnectorErrorCode`, `ConnectorPage`,
//! `ConnectorColumns`. Until then, `cargo test -p pylon-connector-spi`
//! compiles and runs the empty crate — the boundary is the artifact.
//!
//! [`docs/rfcs/0005-pipeline-trait-surface.md`]: ../../docs/rfcs/0005-pipeline-trait-surface.md
//! [`tools/check-spi-boundaries.sh`]: ../../tools/check-spi-boundaries.sh

#![warn(missing_docs)]
