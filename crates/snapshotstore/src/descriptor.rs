//! Versioned, backend-independent descriptor exported to query engines.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::MetadataError;
use crate::metadata::{
    CanonicalSchemaV1, Checksum, Head, ManifestV1, Sha256Digest, TableMetadata, require_format,
    validate_data_key, validate_snapshot,
};
use crate::storage::ObjectKey;

/// Immutable description of one committed table version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotDescriptorV1 {
    pub format_version: u32,
    pub table_id: Uuid,
    pub version: u64,
    pub commit_id: Uuid,
    pub schema: CanonicalSchemaV1,
    pub schema_fingerprint: Sha256Digest,
    pub files: Vec<SnapshotFileV1>,
}

impl SnapshotDescriptorV1 {
    /// Builds a descriptor only from a validated committed snapshot.
    pub fn from_snapshot(
        table: &TableMetadata,
        head: &Head,
        manifest: &ManifestV1,
    ) -> Result<Self, MetadataError> {
        validate_snapshot(table, head, manifest)?;
        Ok(Self {
            format_version: table.format_version,
            table_id: table.table_id,
            version: manifest.version,
            commit_id: manifest.commit_id,
            schema: table.schema.clone(),
            schema_fingerprint: table.schema_fingerprint.clone(),
            files: manifest
                .files
                .iter()
                .map(|file| SnapshotFileV1 {
                    object_key: file.object_key.clone(),
                    byte_size: file.byte_length,
                    row_count: file.row_count,
                    checksum: file.checksum.clone(),
                    schema_fingerprint: file.schema_fingerprint.clone(),
                })
                .collect(),
        })
    }

    /// Checks the self-contained descriptor invariants.
    pub fn validate(&self) -> Result<(), MetadataError> {
        require_format(self.format_version)?;
        if self.table_id.is_nil() || self.commit_id.is_nil() {
            return Err(MetadataError::corrupt(
                "descriptor table and commit IDs must not be nil",
            ));
        }
        self.schema.validate()?;
        if self.schema.fingerprint() != self.schema_fingerprint {
            return Err(MetadataError::SchemaMismatch);
        }
        if self.version == 0 && !self.files.is_empty() {
            return Err(MetadataError::corrupt(
                "version-0 descriptor must not contain files",
            ));
        }

        let mut keys = HashSet::new();
        for file in &self.files {
            validate_data_key(&file.object_key)?;
            if file.byte_size == 0 || file.row_count == 0 {
                return Err(MetadataError::corrupt(format!(
                    "descriptor file {} must contain bytes and rows",
                    file.object_key
                )));
            }
            if file.schema_fingerprint != self.schema_fingerprint {
                return Err(MetadataError::SchemaMismatch);
            }
            if !keys.insert(file.object_key.clone()) {
                return Err(MetadataError::corrupt(format!(
                    "duplicate descriptor file {}",
                    file.object_key
                )));
            }
        }
        Ok(())
    }
}

/// One immutable data file listed in a snapshot descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotFileV1 {
    pub object_key: ObjectKey,
    pub byte_size: u64,
    pub row_count: u64,
    pub checksum: Checksum,
    pub schema_fingerprint: Sha256Digest,
}
