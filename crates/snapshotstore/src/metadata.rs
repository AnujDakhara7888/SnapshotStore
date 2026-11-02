//! Persistent metadata for SnapshotStore format version 1.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::errors::MetadataError;
use crate::storage::ObjectKey;

/// The only persistent format version supported by this release.
pub const FORMAT_VERSION: u32 = 1;

/// An immutable table identity and fixed logical schema (`table.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMetadata {
    pub format_version: u32,
    pub table_id: Uuid,
    pub schema: CanonicalSchemaV1,
    pub schema_fingerprint: Sha256Digest,
}

impl TableMetadata {
    /// Checks invariants that can be established without reading another object.
    pub fn validate(&self) -> Result<(), MetadataError> {
        require_format(self.format_version)?;
        if self.table_id.is_nil() {
            return Err(MetadataError::corrupt("table_id must not be nil"));
        }
        self.schema.validate()?;
        if self.schema.fingerprint() != self.schema_fingerprint {
            return Err(MetadataError::SchemaMismatch);
        }
        Ok(())
    }
}

/// The only mutable publication pointer (`HEAD`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub table_id: Uuid,
    pub version: u64,
    pub commit_id: Uuid,
    pub manifest_key: ObjectKey,
}

/// One immutable complete table manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestV1 {
    pub format_version: u32,
    pub table_id: Uuid,
    pub version: u64,
    pub commit_id: Uuid,
    pub parent_manifest: Option<ObjectKey>,
    pub schema_fingerprint: Sha256Digest,
    pub row_count: u64,
    pub files: Vec<DataFileDescriptor>,
    pub operation: OperationMetadata,
    pub committed_batches: BTreeMap<BatchId, CommittedBatch>,
}

impl ManifestV1 {
    /// Checks internal manifest invariants without following the parent link.
    pub fn validate(&self) -> Result<(), MetadataError> {
        require_format(self.format_version)?;
        if self.table_id.is_nil() || self.commit_id.is_nil() || self.operation.attempt_id.is_nil() {
            return Err(MetadataError::corrupt(
                "table, commit, and attempt IDs must not be nil",
            ));
        }
        if let Some(parent) = &self.parent_manifest {
            let parent_commit = commit_id_from_manifest_key(parent)?;
            if parent_commit == self.commit_id {
                return Err(MetadataError::corrupt(
                    "manifest must not name itself as its parent",
                ));
            }
        }

        let mut file_keys = HashSet::new();
        let mut files_by_key = HashMap::new();
        let mut total_rows = 0_u64;

        for file in &self.files {
            validate_data_key(&file.object_key)?;
            if file.byte_length == 0 {
                return Err(MetadataError::corrupt(format!(
                    "data file {} has zero bytes",
                    file.object_key
                )));
            }
            if file.row_count == 0 {
                return Err(MetadataError::corrupt(format!(
                    "data file {} has zero rows",
                    file.object_key
                )));
            }
            if file.schema_fingerprint != self.schema_fingerprint {
                return Err(MetadataError::SchemaMismatch);
            }
            if !file_keys.insert(file.object_key.clone()) {
                return Err(MetadataError::corrupt(format!(
                    "duplicate data file {}",
                    file.object_key
                )));
            }
            total_rows = total_rows
                .checked_add(file.row_count)
                .ok_or_else(|| MetadataError::corrupt("manifest row count overflow"))?;
            files_by_key.insert(file.object_key.clone(), file);
        }

        if total_rows != self.row_count {
            return Err(MetadataError::corrupt(format!(
                "manifest row_count {} does not equal file total {total_rows}",
                self.row_count
            )));
        }

        match self.operation.kind {
            OperationKind::Create => self.validate_create()?,
            OperationKind::Append => self.validate_append(&files_by_key)?,
        }

        for batch in self.committed_batches.values() {
            if batch.first_commit_id.is_nil() {
                return Err(MetadataError::corrupt(
                    "batch first_commit_id must not be nil",
                ));
            }
            if batch.first_version == 0 || batch.first_version > self.version {
                return Err(MetadataError::corrupt(format!(
                    "batch first_version {} is invalid at manifest version {}",
                    batch.first_version, self.version
                )));
            }
            if batch.added_rows == 0 {
                return Err(MetadataError::corrupt(
                    "committed batch must contain at least one row",
                ));
            }
        }

        Ok(())
    }

    fn validate_create(&self) -> Result<(), MetadataError> {
        if self.version != 0
            || self.parent_manifest.is_some()
            || self.row_count != 0
            || !self.files.is_empty()
            || !self.committed_batches.is_empty()
            || self.operation.batch_id.is_some()
            || self.operation.added_rows != 0
            || !self.operation.added_files.is_empty()
        {
            return Err(MetadataError::corrupt(
                "create operation must describe an empty version-0 manifest",
            ));
        }
        Ok(())
    }

    fn validate_append(
        &self,
        files_by_key: &HashMap<ObjectKey, &DataFileDescriptor>,
    ) -> Result<(), MetadataError> {
        if self.version == 0 || self.parent_manifest.is_none() {
            return Err(MetadataError::corrupt(
                "append operation requires a positive version and parent manifest",
            ));
        }
        if self.operation.added_rows == 0 || self.operation.added_files.is_empty() {
            return Err(MetadataError::corrupt(
                "append operation must add at least one row and file",
            ));
        }

        let batch_id = self
            .operation
            .batch_id
            .as_ref()
            .ok_or_else(|| MetadataError::corrupt("append operation requires a batch_id"))?;
        let batch = self.committed_batches.get(batch_id).ok_or_else(|| {
            MetadataError::corrupt("append batch is missing from committed registry")
        })?;
        if batch.first_commit_id != self.commit_id
            || batch.first_version != self.version
            || batch.added_rows != self.operation.added_rows
        {
            return Err(MetadataError::corrupt(
                "append registry entry does not match the first commit identity",
            ));
        }

        let mut added_keys = HashSet::new();
        let mut added_rows = 0_u64;
        for key in &self.operation.added_files {
            if !added_keys.insert(key.clone()) {
                return Err(MetadataError::corrupt(format!(
                    "duplicate added file {key}"
                )));
            }
            let file = files_by_key
                .get(key)
                .ok_or_else(|| MetadataError::corrupt(format!("added file {key} is not active")))?;
            let file_attempt = validate_data_key(key)?;
            if file_attempt != self.operation.attempt_id {
                return Err(MetadataError::corrupt(format!(
                    "added file {key} does not belong to attempt {}",
                    self.operation.attempt_id
                )));
            }
            added_rows = added_rows
                .checked_add(file.row_count)
                .ok_or_else(|| MetadataError::corrupt("operation row count overflow"))?;
        }
        if added_rows != self.operation.added_rows {
            return Err(MetadataError::corrupt(format!(
                "operation added_rows {} does not equal added file total {added_rows}",
                self.operation.added_rows
            )));
        }

        Ok(())
    }
}

/// Validates a `HEAD` resolution against its immutable table and manifest.
pub fn validate_snapshot(
    table: &TableMetadata,
    head: &Head,
    manifest: &ManifestV1,
) -> Result<(), MetadataError> {
    table.validate()?;
    manifest.validate()?;

    for found in [head.table_id, manifest.table_id] {
        if found != table.table_id {
            return Err(MetadataError::TableMismatch {
                expected: table.table_id,
                found,
            });
        }
    }
    if manifest.schema_fingerprint != table.schema_fingerprint {
        return Err(MetadataError::SchemaMismatch);
    }
    if head.version != manifest.version || head.commit_id != manifest.commit_id {
        return Err(MetadataError::corrupt(
            "HEAD version/commit does not match its manifest",
        ));
    }

    let expected = manifest_key(manifest.commit_id);
    if head.manifest_key != expected {
        return Err(MetadataError::ManifestKeyMismatch {
            expected,
            found: head.manifest_key.clone(),
        });
    }

    Ok(())
}

/// Returns the required immutable key for a commit manifest.
pub fn manifest_key(commit_id: Uuid) -> ObjectKey {
    ObjectKey::parse(format!("manifests/{commit_id}.json"))
        .expect("UUID manifest keys are always relative and normalized")
}

fn commit_id_from_manifest_key(key: &ObjectKey) -> Result<Uuid, MetadataError> {
    let value = key.as_str();
    let commit = value
        .strip_prefix("manifests/")
        .and_then(|value| value.strip_suffix(".json"))
        .filter(|value| !value.contains('/'))
        .ok_or_else(|| {
            MetadataError::corrupt(format!(
                "manifest key {key} must match manifests/<commit-id>.json"
            ))
        })?;
    Uuid::parse_str(commit).map_err(|_| {
        MetadataError::corrupt(format!(
            "manifest key {key} must contain a valid commit UUID"
        ))
    })
}

pub(crate) fn validate_data_key(key: &ObjectKey) -> Result<Uuid, MetadataError> {
    let mut segments = key.as_str().split('/');
    let prefix = segments.next();
    let attempt = segments.next();
    let file = segments.next();
    if prefix != Some("data") || segments.next().is_some() {
        return Err(MetadataError::corrupt(format!(
            "data key {key} must match data/<attempt-id>/<file-id>.parquet"
        )));
    }

    let attempt = attempt
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or_else(|| MetadataError::corrupt(format!("invalid attempt UUID in {key}")))?;
    file.and_then(|value| value.strip_suffix(".parquet"))
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or_else(|| MetadataError::corrupt(format!("invalid file UUID in {key}")))?;

    Ok(attempt)
}

/// Canonical fixed schema representation used by the v1 format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalSchemaV1 {
    pub fields: Vec<SchemaFieldV1>,
}

impl CanonicalSchemaV1 {
    pub fn validate(&self) -> Result<(), MetadataError> {
        if self.fields.is_empty() {
            return Err(MetadataError::corrupt("schema must contain a field"));
        }
        let mut names = HashSet::new();
        for field in &self.fields {
            if field.name.is_empty() || field.name.chars().any(char::is_control) {
                return Err(MetadataError::corrupt(
                    "schema field names must be nonempty and contain no control characters",
                ));
            }
            if !names.insert(&field.name) {
                return Err(MetadataError::corrupt(format!(
                    "duplicate schema field {}",
                    field.name
                )));
            }
        }
        Ok(())
    }

    /// Computes the v1 fingerprint over ordered fields and explicit type tags.
    pub fn fingerprint(&self) -> Sha256Digest {
        let mut hasher = Sha256::new();
        hasher.update(b"snapshotstore-schema-v1\0");
        hasher.update((self.fields.len() as u64).to_be_bytes());
        for field in &self.fields {
            let name = field.name.as_bytes();
            hasher.update((name.len() as u64).to_be_bytes());
            hasher.update(name);
            hasher.update([field.data_type.fingerprint_tag()]);
            hasher.update([u8::from(field.nullable)]);
        }
        Sha256Digest::from_digest_bytes(hasher.finalize().into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaFieldV1 {
    pub name: String,
    pub data_type: DataTypeV1,
    pub nullable: bool,
}

/// Scalar Arrow-compatible logical types supported by format v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataTypeV1 {
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "uint64")]
    UInt64,
    #[serde(rename = "float64")]
    Float64,
    #[serde(rename = "utf8")]
    Utf8,
}

impl DataTypeV1 {
    fn fingerprint_tag(self) -> u8 {
        match self {
            Self::Boolean => 0,
            Self::Int64 => 1,
            Self::UInt64 => 2,
            Self::Float64 => 3,
            Self::Utf8 => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFileDescriptor {
    pub object_key: ObjectKey,
    pub byte_length: u64,
    pub row_count: u64,
    pub checksum: Checksum,
    pub schema_fingerprint: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    pub algorithm: ChecksumAlgorithm,
    pub value: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    #[serde(rename = "sha256")]
    Sha256,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationMetadata {
    pub kind: OperationKind,
    pub attempt_id: Uuid,
    pub batch_id: Option<BatchId>,
    pub added_rows: u64,
    pub added_files: Vec<ObjectKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    #[serde(rename = "create")]
    Create,
    #[serde(rename = "append")]
    Append,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBatch {
    pub input_fingerprint: Sha256Digest,
    pub first_commit_id: Uuid,
    pub first_version: u64,
    pub added_rows: u64,
}

/// Caller-supplied stable batch identifier, scoped to one table.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchId(String);

impl BatchId {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidBatchId> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(InvalidBatchId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BatchId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("BatchId").field(&self.0).finish()
    }
}

impl Serialize for BatchId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for BatchId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("batch ID must be 1-256 bytes and contain no control characters")]
pub struct InvalidBatchId;

/// A lowercase hexadecimal SHA-256 digest.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidSha256Digest> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(InvalidSha256Digest);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_digest_bytes(bytes: [u8; 32]) -> Self {
        let mut value = String::with_capacity(64);
        for byte in bytes {
            use fmt::Write as _;
            write!(&mut value, "{byte:02x}").expect("writing to a String cannot fail");
        }
        Self(value)
    }
}

impl fmt::Debug for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Sha256Digest")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("SHA-256 digest must be 64 lowercase hexadecimal characters")]
pub struct InvalidSha256Digest;

pub(crate) fn require_format(found: u32) -> Result<(), MetadataError> {
    if found == FORMAT_VERSION {
        Ok(())
    } else {
        Err(MetadataError::UnsupportedFormat { found })
    }
}
