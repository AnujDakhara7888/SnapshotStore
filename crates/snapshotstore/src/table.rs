//! Table creation, recovery, and pinned snapshot opening.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::descriptor::SnapshotDescriptorV1;
use crate::errors::{MetadataError, TableError};
use crate::metadata::{
    CanonicalSchemaV1, FORMAT_VERSION, Head, ManifestV1, OperationKind, OperationMetadata,
    TableMetadata, manifest_key, validate_snapshot,
};
use crate::storage::{ObjectKey, Storage, StorageError};

fn table_key() -> ObjectKey {
    ObjectKey::parse("table.json").expect("static object key is valid")
}

fn head_key() -> ObjectKey {
    ObjectKey::parse("HEAD").expect("static object key is valid")
}

/// One table snapshot pinned to the `HEAD` value observed while opening it.
#[derive(Debug)]
pub struct Table<S: Storage + ?Sized> {
    storage: Arc<S>,
    table_metadata: TableMetadata,
    head: Head,
    manifest: ManifestV1,
}

impl<S: Storage + ?Sized> Table<S> {
    /// Returns the immutable table identity.
    pub fn table_id(&self) -> Uuid {
        self.table_metadata.table_id
    }

    /// Returns the pinned snapshot version.
    pub fn version(&self) -> u64 {
        self.head.version
    }

    /// Returns the pinned commit identity.
    pub fn commit_id(&self) -> Uuid {
        self.head.commit_id
    }

    /// Returns the storage adapter used to open this table.
    pub fn storage(&self) -> &Arc<S> {
        &self.storage
    }

    /// Returns the immutable table metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.table_metadata
    }

    /// Returns the publication pointer observed while opening the table.
    pub fn head(&self) -> &Head {
        &self.head
    }

    /// Returns the immutable manifest named by the pinned publication pointer.
    pub fn manifest(&self) -> &ManifestV1 {
        &self.manifest
    }

    /// Exports the pinned snapshot as the stable v1 descriptor contract.
    pub fn descriptor(&self) -> SnapshotDescriptorV1 {
        SnapshotDescriptorV1::from_snapshot(&self.table_metadata, &self.head, &self.manifest)
            .expect("Table construction validates the complete snapshot")
    }
}

/// Creates an empty version-0 table or completes an interrupted initialization.
///
/// Concurrent creators converge on the first immutable `table.json` identity and
/// the first successfully published `HEAD`. Repeating this call with the same
/// schema is idempotent.
pub async fn create_table<S: Storage + ?Sized>(
    storage: Arc<S>,
    schema: CanonicalSchemaV1,
) -> Result<Table<S>, TableError> {
    schema
        .validate()
        .map_err(|error| metadata_error(&table_key(), error))?;
    let requested_fingerprint = schema.fingerprint();
    let table = ensure_table_metadata(storage.as_ref(), schema).await?;
    if table.schema_fingerprint != requested_fingerprint {
        return Err(TableError::SchemaMismatch);
    }

    match open_table(Arc::clone(&storage)).await {
        Ok(existing) => return Ok(existing),
        Err(TableError::InitializationIncomplete { table_id }) if table_id == table.table_id => {}
        Err(TableError::InitializationIncomplete { table_id }) => {
            return Err(TableError::CorruptMetadata {
                key: head_key(),
                message: format!(
                    "incomplete initialization named unexpected table {table_id}; expected {}",
                    table.table_id
                ),
            });
        }
        Err(error) => return Err(error),
    }

    let commit_id = Uuid::new_v4();
    let manifest = ManifestV1 {
        format_version: FORMAT_VERSION,
        table_id: table.table_id,
        version: 0,
        commit_id,
        parent_manifest: None,
        schema_fingerprint: table.schema_fingerprint.clone(),
        row_count: 0,
        files: Vec::new(),
        operation: OperationMetadata {
            kind: OperationKind::Create,
            attempt_id: Uuid::new_v4(),
            batch_id: None,
            added_rows: 0,
            added_files: Vec::new(),
        },
        committed_batches: BTreeMap::new(),
    };
    manifest
        .validate()
        .map_err(|error| metadata_error(&manifest_key(commit_id), error))?;
    let manifest_key = manifest_key(commit_id);
    let manifest_bytes = encode(&manifest, &manifest_key)?;
    put_immutable_reconciled(storage.as_ref(), &manifest_key, manifest_bytes).await?;

    let head = Head {
        table_id: table.table_id,
        version: 0,
        commit_id,
        manifest_key,
    };
    let key = head_key();
    let bytes = encode(&head, &key)?;
    match storage.create_if_absent(&key, bytes).await {
        Ok(_) | Err(StorageError::AlreadyExists { .. }) => open_table(storage).await,
        Err(StorageError::UnknownWriteOutcome {
            operation,
            key,
            message,
        }) => match open_table(Arc::clone(&storage)).await {
            Ok(table) => Ok(table),
            Err(error) => Err(reconciliation_error(operation, key, message, error)),
        },
        Err(error) => Err(storage_error(&key, error)),
    }
}

/// Opens the snapshot currently published through `HEAD`.
///
/// A returned table remains pinned even if a later writer advances `HEAD`.
pub async fn open_table<S: Storage + ?Sized>(storage: Arc<S>) -> Result<Table<S>, TableError> {
    let table_key = table_key();
    let table_object = match storage.get_with_token(&table_key).await {
        Ok(object) => object,
        Err(StorageError::NotFound { .. }) => return Err(TableError::TableNotFound),
        Err(error) => return Err(storage_error(&table_key, error)),
    };
    let table: TableMetadata = decode(&table_key, &table_object.bytes)?;
    table
        .validate()
        .map_err(|error| metadata_error(&table_key, error))?;

    let head_key = head_key();
    let head_object = match storage.get_with_token(&head_key).await {
        Ok(object) => object,
        Err(StorageError::NotFound { .. }) => {
            return Err(TableError::InitializationIncomplete {
                table_id: table.table_id,
            });
        }
        Err(error) => return Err(storage_error(&head_key, error)),
    };
    let head: Head = decode(&head_key, &head_object.bytes)?;

    let manifest_object = match storage.get_with_token(&head.manifest_key).await {
        Ok(object) => object,
        Err(StorageError::NotFound { .. }) => {
            return Err(TableError::CorruptMetadata {
                key: head.manifest_key.clone(),
                message: "HEAD names a manifest that does not exist".to_owned(),
            });
        }
        Err(error) => return Err(storage_error(&head.manifest_key, error)),
    };
    let manifest: ManifestV1 = decode(&head.manifest_key, &manifest_object.bytes)?;
    validate_snapshot(&table, &head, &manifest)
        .map_err(|error| metadata_error(&head.manifest_key, error))?;

    Ok(Table {
        storage,
        table_metadata: table,
        head,
        manifest,
    })
}

async fn ensure_table_metadata<S: Storage + ?Sized>(
    storage: &S,
    schema: CanonicalSchemaV1,
) -> Result<TableMetadata, TableError> {
    let key = table_key();
    match read_table_metadata(storage, &key).await {
        Ok(table) => return match_schema(table, &schema),
        Err(TableError::TableNotFound) => {}
        Err(error) => return Err(error),
    }

    let candidate = TableMetadata {
        format_version: FORMAT_VERSION,
        table_id: Uuid::new_v4(),
        schema_fingerprint: schema.fingerprint(),
        schema,
    };
    let bytes = encode(&candidate, &key)?;
    match storage.put_immutable(&key, bytes).await {
        Ok(_) => Ok(candidate),
        Err(StorageError::AlreadyExists { .. }) => {
            let winner = read_table_metadata(storage, &key).await?;
            match_schema(winner, &candidate.schema)
        }
        Err(StorageError::UnknownWriteOutcome {
            operation,
            key: outcome_key,
            message,
        }) => match read_table_metadata(storage, &key).await {
            Ok(winner) => match_schema(winner, &candidate.schema),
            Err(error) => Err(reconciliation_error(operation, outcome_key, message, error)),
        },
        Err(error) => Err(storage_error(&key, error)),
    }
}

async fn read_table_metadata<S: Storage + ?Sized>(
    storage: &S,
    key: &ObjectKey,
) -> Result<TableMetadata, TableError> {
    let object = match storage.get_with_token(key).await {
        Ok(object) => object,
        Err(StorageError::NotFound { .. }) => return Err(TableError::TableNotFound),
        Err(error) => return Err(storage_error(key, error)),
    };
    let table: TableMetadata = decode(key, &object.bytes)?;
    table
        .validate()
        .map_err(|error| metadata_error(key, error))?;
    Ok(table)
}

fn match_schema(
    table: TableMetadata,
    requested: &CanonicalSchemaV1,
) -> Result<TableMetadata, TableError> {
    if &table.schema == requested && table.schema_fingerprint == requested.fingerprint() {
        Ok(table)
    } else {
        Err(TableError::SchemaMismatch)
    }
}

async fn put_immutable_reconciled<S: Storage + ?Sized>(
    storage: &S,
    key: &ObjectKey,
    bytes: Bytes,
) -> Result<(), TableError> {
    match storage.put_immutable(key, bytes.clone()).await {
        Ok(_) => Ok(()),
        Err(StorageError::AlreadyExists { .. }) => {
            let found = storage
                .get_with_token(key)
                .await
                .map_err(|error| storage_error(key, error))?;
            if found.bytes == bytes {
                Ok(())
            } else {
                Err(TableError::CorruptMetadata {
                    key: key.clone(),
                    message: "immutable key already contains different bytes".to_owned(),
                })
            }
        }
        Err(StorageError::UnknownWriteOutcome {
            operation,
            key: outcome_key,
            message,
        }) => match storage.get_with_token(key).await {
            Ok(found) if found.bytes == bytes => Ok(()),
            Ok(_) => Err(TableError::CorruptMetadata {
                key: key.clone(),
                message: "immutable key contains different bytes after an unknown write outcome"
                    .to_owned(),
            }),
            Err(_) => Err(TableError::UnknownCommitStatus {
                operation,
                key: outcome_key,
                message,
            }),
        },
        Err(error) => Err(storage_error(key, error)),
    }
}

fn reconciliation_error(
    operation: crate::storage::StorageOperation,
    key: ObjectKey,
    message: String,
    observed: TableError,
) -> TableError {
    match observed {
        TableError::SchemaMismatch
        | TableError::UnsupportedFormat { .. }
        | TableError::CorruptMetadata { .. } => observed,
        TableError::TableNotFound
        | TableError::InitializationIncomplete { .. }
        | TableError::StorageUnavailable { .. }
        | TableError::UnknownCommitStatus { .. } => TableError::UnknownCommitStatus {
            operation,
            key,
            message,
        },
    }
}

fn encode<T: Serialize>(value: &T, key: &ObjectKey) -> Result<Bytes, TableError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| TableError::CorruptMetadata {
            key: key.clone(),
            message: format!("could not encode metadata: {error}"),
        })
}

fn decode<T: DeserializeOwned>(key: &ObjectKey, bytes: &[u8]) -> Result<T, TableError> {
    serde_json::from_slice(bytes).map_err(|error| TableError::CorruptMetadata {
        key: key.clone(),
        message: format!("invalid JSON metadata: {error}"),
    })
}

fn metadata_error(key: &ObjectKey, error: MetadataError) -> TableError {
    match error {
        MetadataError::UnsupportedFormat { found } => TableError::UnsupportedFormat { found },
        error => TableError::CorruptMetadata {
            key: key.clone(),
            message: error.to_string(),
        },
    }
}

fn storage_error(key: &ObjectKey, error: StorageError) -> TableError {
    match error {
        StorageError::Unavailable { operation, message } => {
            TableError::StorageUnavailable { operation, message }
        }
        StorageError::UnknownWriteOutcome {
            operation,
            key,
            message,
        } => TableError::UnknownCommitStatus {
            operation,
            key,
            message,
        },
        error => TableError::CorruptMetadata {
            key: key.clone(),
            message: error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypeV1, SchemaFieldV1};

    #[test]
    fn invalid_requested_schema_is_rejected_before_storage() {
        let schema = CanonicalSchemaV1 { fields: Vec::new() };
        assert!(matches!(
            schema.validate(),
            Err(MetadataError::CorruptMetadata { .. })
        ));
    }

    #[test]
    fn schema_matching_requires_exact_field_order_and_nullability() {
        let schema = CanonicalSchemaV1 {
            fields: vec![SchemaFieldV1 {
                name: "id".to_owned(),
                data_type: DataTypeV1::UInt64,
                nullable: false,
            }],
        };
        let table = TableMetadata {
            format_version: FORMAT_VERSION,
            table_id: Uuid::new_v4(),
            schema_fingerprint: schema.fingerprint(),
            schema: schema.clone(),
        };
        assert!(match_schema(table, &schema).is_ok());
    }

    #[test]
    fn operation_names_remain_available_in_public_errors() {
        let error = TableError::UnknownCommitStatus {
            operation: crate::storage::StorageOperation::CreateIfAbsent,
            key: head_key(),
            message: "response lost".to_owned(),
        };
        assert!(error.to_string().contains("create_if_absent"));
    }
}
