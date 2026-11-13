//! Transactional append-only tables over immutable Parquet files.
//!
//! The crate defines the version-one metadata format and the storage contract
//! used to publish table state. Concrete storage backends and table operations
//! are added separately so their durability and concurrency behavior can be
//! tested against this contract.

#![forbid(unsafe_code)]

pub mod descriptor;
pub mod errors;
pub mod fingerprint;
pub mod metadata;
mod parquet_io;
pub mod storage;
pub mod table;
pub mod verify;
