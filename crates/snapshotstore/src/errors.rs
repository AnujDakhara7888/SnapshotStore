use thiserror::Error;
use uuid::Uuid;

use crate::storage::ObjectKey;

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
