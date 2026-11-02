use thiserror::Error;
use uuid::Uuid;

use crate::storage::{ObjectKey, StorageOperation};

/// Failures detected while decoding or validating committed metadata.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetadataError {
    /// The metadata belongs to a format version this crate cannot read.
    #[error("unsupported metadata format version {found}")]
    UnsupportedFormat { found: u32 },

    /// Structurally valid JSON violates a persistent-format invariant.
    #[error("corrupt metadata: {message}")]
    CorruptMetadata { message: String },

    /// Linked metadata names a different table identity.
    #[error("table identity mismatch: expected {expected}, found {found}")]
    TableMismatch { expected: Uuid, found: Uuid },

    /// Linked metadata carries a different schema fingerprint.
    #[error("schema fingerprint mismatch")]
    SchemaMismatch,

    /// `HEAD` points to a manifest key that does not match its commit ID.
    #[error("manifest key mismatch: expected {expected}, found {found}")]
    ManifestKeyMismatch {
        expected: ObjectKey,
        found: ObjectKey,
    },
}

impl MetadataError {
    pub(crate) fn corrupt(message: impl Into<String>) -> Self {
        Self::CorruptMetadata {
            message: message.into(),
        }
    }
}

/// Public failures from table creation and opening.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TableError {
    #[error("table.json does not exist")]
    TableNotFound,

    #[error("table initialization is incomplete for table {table_id}")]
    InitializationIncomplete { table_id: Uuid },

    #[error("requested schema does not match the existing table")]
    SchemaMismatch,

    #[error("unsupported metadata format version {found}")]
    UnsupportedFormat { found: u32 },

    #[error("corrupt metadata at {key}: {message}")]
    CorruptMetadata { key: ObjectKey, message: String },

    #[error("storage unavailable during {operation}: {message}")]
    StorageUnavailable {
        operation: StorageOperation,
        message: String,
    },

    #[error("commit status is unknown after {operation} for {key}: {message}")]
    UnknownCommitStatus {
        operation: StorageOperation,
        key: ObjectKey,
        message: String,
    },
}
