//! Transactional append-only tables over immutable Parquet files.
//!
//! The crate currently establishes the Arrow and Parquet foundation. Table
//! metadata, conditional publication, replay safety, and snapshot APIs will be
//! added as their storage contracts are implemented and tested.

#![forbid(unsafe_code)]
