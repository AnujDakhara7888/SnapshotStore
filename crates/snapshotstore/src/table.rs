//! Table creation, recovery, and pinned snapshot opening.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::descriptor::SnapshotDescriptorV1;
use crate::errors::{MetadataError, TableError};
use crate::fingerprint::fingerprint_batches;
use crate::metadata::{
    BatchId, CanonicalSchemaV1, Checksum, ChecksumAlgorithm, CommittedBatch, DataFileDescriptor,
    FORMAT_VERSION, Head, ManifestV1, OperationKind, OperationMetadata, Sha256Digest,
    TableMetadata, manifest_key, validate_snapshot,
};
use crate::parquet_io;
use crate::storage::{ObjectKey, Storage, StorageError, VersionToken};

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
    publication_token: Option<VersionToken>,
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

    /// Reads every immutable Parquet file in this pinned snapshot.
    ///
    /// File length, SHA-256 checksum, Arrow schema, and cumulative row count are
    /// verified before rows are returned.
    pub async fn read_batches(&self) -> Result<Vec<RecordBatch>, TableError> {
        let mut batches = Vec::new();
        let mut rows = 0_u64;
        for file in &self.manifest.files {
            let object = match self.storage.get_with_token(&file.object_key).await {
                Ok(object) => object,
                Err(StorageError::NotFound { .. }) => {
                    return Err(TableError::MissingCommittedFile {
                        key: file.object_key.clone(),
                    });
                }
                Err(error) => return Err(storage_error(&file.object_key, error)),
            };
            if object.bytes.len() as u64 != file.byte_length {
                return Err(TableError::CorruptData {
                    key: file.object_key.clone(),
                    message: format!(
                        "byte length {} does not match manifest length {}",
                        object.bytes.len(),
                        file.byte_length
                    ),
                });
            }
            if Sha256Digest::compute(&object.bytes) != file.checksum.value {
                return Err(TableError::CorruptData {
                    key: file.object_key.clone(),
                    message: "SHA-256 checksum does not match manifest".to_owned(),
                });
            }
            let file_batches = parquet_io::read_batches(
                &file.object_key,
                object.bytes,
                &self.table_metadata.schema,
            )?;
            let file_rows = file_batches.iter().try_fold(0_u64, |total, batch| {
                total.checked_add(batch.num_rows() as u64)
            });
            let file_rows = file_rows.ok_or_else(|| TableError::CorruptData {
                key: file.object_key.clone(),
                message: "decoded row count overflow".to_owned(),
            })?;
            if file_rows != file.row_count {
                return Err(TableError::CorruptData {
                    key: file.object_key.clone(),
                    message: format!(
                        "decoded row count {file_rows} does not match manifest count {}",
                        file.row_count
                    ),
                });
            }
            rows = rows
                .checked_add(file_rows)
                .ok_or_else(|| TableError::CorruptData {
                    key: file.object_key.clone(),
                    message: "snapshot row count overflow".to_owned(),
                })?;
            batches.extend(file_batches);
        }
        if rows != self.manifest.row_count {
            return Err(TableError::CorruptMetadata {
                key: self.head.manifest_key.clone(),
                message: format!(
                    "decoded row count {rows} does not match snapshot count {}",
                    self.manifest.row_count
                ),
            });
        }
        Ok(batches)
    }

    /// Returns reachable snapshot descriptors in ascending version order.
    pub async fn history(&self) -> Result<Vec<SnapshotDescriptorV1>, TableError> {
        let mut snapshots = vec![self.descriptor()];
        let mut child = self.manifest.clone();
        while let Some(parent_key) = child.parent_manifest.clone() {
            let parent =
                read_manifest(self.storage.as_ref(), &self.table_metadata, &parent_key).await?;
            validate_parent(&child, &parent, &parent_key)?;
            let head = head_for_manifest(&parent);
            snapshots.push(
                SnapshotDescriptorV1::from_snapshot(&self.table_metadata, &head, &parent)
                    .map_err(|error| metadata_error(&parent_key, error))?,
            );
            child = parent;
        }
        snapshots.reverse();
        Ok(snapshots)
    }

    /// Opens a historical snapshot reachable from this pinned snapshot.
    pub async fn snapshot_at_version(&self, version: u64) -> Result<Table<S>, TableError> {
        if version > self.version() {
            return Err(TableError::VersionNotFound { version });
        }
        if version == self.version() {
            return Ok(Table {
                storage: Arc::clone(&self.storage),
                table_metadata: self.table_metadata.clone(),
                head: self.head.clone(),
                manifest: self.manifest.clone(),
                publication_token: None,
            });
        }

        let mut child = self.manifest.clone();
        while let Some(parent_key) = child.parent_manifest.clone() {
            let parent =
                read_manifest(self.storage.as_ref(), &self.table_metadata, &parent_key).await?;
            validate_parent(&child, &parent, &parent_key)?;
            if parent.version == version {
                return Ok(Table {
                    storage: Arc::clone(&self.storage),
                    table_metadata: self.table_metadata.clone(),
                    head: head_for_manifest(&parent),
                    manifest: parent,
                    publication_token: None,
                });
            }
            child = parent;
        }
        Err(TableError::VersionNotFound { version })
    }
}

/// Stable identity returned for a first append and every matching replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendReceipt {
    pub table_id: Uuid,
    pub first_commit_id: Uuid,
    pub first_version: u64,
    pub batch_id: BatchId,
    pub fingerprint: Sha256Digest,
    pub added_rows: u64,
    pub replayed: bool,
}

/// The pinned table state and durable receipt resolved by an append call.
#[derive(Debug)]
pub struct AppendResult<S: Storage + ?Sized> {
    pub table: Table<S>,
    pub receipt: AppendReceipt,
}

impl<S: Storage + ?Sized> std::ops::Deref for AppendResult<S> {
    type Target = Table<S>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

/// Maximum number of conditional `HEAD` replacements for one append call.
pub const MAX_PUBLICATION_ATTEMPTS: u32 = 3;

#[derive(Debug)]
struct PreparedAppend {
    attempt_id: Uuid,
    files: Vec<DataFileDescriptor>,
    file_keys: Vec<ObjectKey>,
}

/// Appends one nonempty Arrow batch.
pub async fn append_batch<S: Storage + ?Sized>(
    table: &Table<S>,
    batch_id: BatchId,
    batch: &RecordBatch,
) -> Result<AppendResult<S>, TableError> {
    append_batches(table, batch_id, std::slice::from_ref(batch)).await
}

/// Appends ordered Arrow batches as one replay-safe logical input.
///
/// The logical fingerprint is independent of `RecordBatch` boundaries and
/// physical Parquet encoding. A matching committed ID returns its original
/// receipt without writing objects or advancing `HEAD`; changed content under
/// the same ID is rejected.
pub async fn append_batches<S: Storage + ?Sized>(
    table: &Table<S>,
    batch_id: BatchId,
    batches: &[RecordBatch],
) -> Result<AppendResult<S>, TableError> {
    let fingerprint = fingerprint_batches(&table.table_metadata.schema, batches)?;
    let added_rows = batches.iter().try_fold(0_u64, |total, batch| {
        total.checked_add(batch.num_rows() as u64)
    });
    let added_rows = added_rows.ok_or_else(|| TableError::InvalidBatch {
        message: "logical row count overflow".to_owned(),
    })?;
    if let Some(receipt) = committed_receipt(table, &batch_id, &fingerprint)? {
        return Ok(AppendResult {
            table: clone_table(table),
            receipt,
        });
    }
    table
        .publication_token
        .as_ref()
        .ok_or(TableError::HistoricalSnapshotReadOnly)?;

    let attempt_id = Uuid::new_v4();
    let mut prepared = PreparedAppend {
        attempt_id,
        files: Vec::new(),
        file_keys: Vec::new(),
    };
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        let file_id = Uuid::new_v4();
        let data_key = ObjectKey::parse(format!("data/{attempt_id}/{file_id}.parquet"))
            .expect("UUID data keys are valid");
        let parquet_bytes = parquet_io::write_batch(batch)?;
        let file = DataFileDescriptor {
            object_key: data_key.clone(),
            byte_length: parquet_bytes.len() as u64,
            row_count: batch.num_rows() as u64,
            checksum: Checksum {
                algorithm: ChecksumAlgorithm::Sha256,
                value: Sha256Digest::compute(&parquet_bytes),
            },
            schema_fingerprint: table.table_metadata.schema_fingerprint.clone(),
        };
        put_immutable_reconciled(table.storage.as_ref(), &data_key, parquet_bytes).await?;
        prepared.file_keys.push(data_key);
        prepared.files.push(file);
    }

    let original_version = table.version();
    let mut base = clone_table(table);
    for publication_attempt in 1..=MAX_PUBLICATION_ATTEMPTS {
        if let Some(receipt) = committed_receipt(&base, &batch_id, &fingerprint)? {
            return Ok(AppendResult {
                table: base,
                receipt,
            });
        }
        let expected = base
            .publication_token
            .as_ref()
            .expect("current snapshots retain a publication token")
            .clone();
        let commit_id = Uuid::new_v4();
        let version = base
            .version()
            .checked_add(1)
            .ok_or_else(|| TableError::CorruptMetadata {
                key: base.head.manifest_key.clone(),
                message: "table version overflow".to_owned(),
            })?;
        let row_count = base
            .manifest
            .row_count
            .checked_add(added_rows)
            .ok_or_else(|| TableError::CorruptMetadata {
                key: base.head.manifest_key.clone(),
                message: "table row count overflow".to_owned(),
            })?;
        let mut files = base.manifest.files.clone();
        files.extend(prepared.files.iter().cloned());
        let mut committed_batches = base.manifest.committed_batches.clone();
        committed_batches.insert(
            batch_id.clone(),
            CommittedBatch {
                input_fingerprint: fingerprint.clone(),
                first_commit_id: commit_id,
                first_version: version,
                added_rows,
            },
        );
        let manifest = ManifestV1 {
            format_version: FORMAT_VERSION,
            table_id: base.table_id(),
            version,
            commit_id,
            parent_manifest: Some(base.head.manifest_key.clone()),
            schema_fingerprint: base.table_metadata.schema_fingerprint.clone(),
            row_count,
            files,
            operation: OperationMetadata {
                kind: OperationKind::Append,
                attempt_id: prepared.attempt_id,
                batch_id: Some(batch_id.clone()),
                added_rows,
                added_files: prepared.file_keys.clone(),
            },
            committed_batches,
        };
        let manifest_key = manifest_key(commit_id);
        manifest
            .validate()
            .map_err(|error| metadata_error(&manifest_key, error))?;
        let manifest_bytes = encode(&manifest, &manifest_key)?;
        put_immutable_reconciled(base.storage.as_ref(), &manifest_key, manifest_bytes).await?;

        let head = Head {
            table_id: base.table_id(),
            version,
            commit_id,
            manifest_key,
        };
        let key = head_key();
        let bytes = encode(&head, &key)?;
        match base
            .storage
            .replace_if_token_matches(&key, &expected, bytes)
            .await
        {
            Ok(publication_token) => {
                return Ok(AppendResult {
                    table: Table {
                        storage: Arc::clone(&base.storage),
                        table_metadata: base.table_metadata.clone(),
                        head,
                        manifest,
                        publication_token: Some(publication_token),
                    },
                    receipt: AppendReceipt {
                        table_id: base.table_id(),
                        first_commit_id: commit_id,
                        first_version: version,
                        batch_id,
                        fingerprint,
                        added_rows,
                        replayed: false,
                    },
                });
            }
            Err(StorageError::PreconditionFailed { .. }) => {}
            Err(StorageError::UnknownWriteOutcome {
                operation,
                key: outcome_key,
                message,
            }) => {
                let latest =
                    open_after_unknown(&base, &head, operation, outcome_key, message).await?;
                if latest.commit_id() == commit_id {
                    return Ok(AppendResult {
                        table: latest,
                        receipt: AppendReceipt {
                            table_id: base.table_id(),
                            first_commit_id: commit_id,
                            first_version: version,
                            batch_id,
                            fingerprint,
                            added_rows,
                            replayed: false,
                        },
                    });
                }
                base = latest;
                if let Some(mut receipt) = committed_receipt(&base, &batch_id, &fingerprint)? {
                    if receipt.first_commit_id == commit_id {
                        receipt.replayed = false;
                    }
                    return Ok(AppendResult {
                        table: base,
                        receipt,
                    });
                }
                if publication_attempt == MAX_PUBLICATION_ATTEMPTS {
                    return Err(TableError::Conflict {
                        expected_version: original_version,
                        latest_version: base.version(),
                        attempts: publication_attempt,
                    });
                }
                continue;
            }
            Err(error) => return Err(storage_error(&key, error)),
        }

        base = open_table(Arc::clone(&base.storage)).await?;
        if let Some(receipt) = committed_receipt(&base, &batch_id, &fingerprint)? {
            return Ok(AppendResult {
                table: base,
                receipt,
            });
        }
        if publication_attempt == MAX_PUBLICATION_ATTEMPTS {
            return Err(TableError::Conflict {
                expected_version: original_version,
                latest_version: base.version(),
                attempts: publication_attempt,
            });
        }
    }
    unreachable!("the bounded publication loop always returns")
}

fn clone_table<S: Storage + ?Sized>(table: &Table<S>) -> Table<S> {
    Table {
        storage: Arc::clone(&table.storage),
        table_metadata: table.table_metadata.clone(),
        head: table.head.clone(),
        manifest: table.manifest.clone(),
        publication_token: table.publication_token.clone(),
    }
}

fn committed_receipt<S: Storage + ?Sized>(
    table: &Table<S>,
    batch_id: &BatchId,
    fingerprint: &Sha256Digest,
) -> Result<Option<AppendReceipt>, TableError> {
    let Some(committed) = table.manifest.committed_batches.get(batch_id) else {
        return Ok(None);
    };
    if &committed.input_fingerprint != fingerprint {
        return Err(TableError::BatchIdReuse {
            batch_id: batch_id.as_str().to_owned(),
            committed_fingerprint: committed.input_fingerprint.as_str().to_owned(),
            supplied_fingerprint: fingerprint.as_str().to_owned(),
        });
    }
    Ok(Some(AppendReceipt {
        table_id: table.table_id(),
        first_commit_id: committed.first_commit_id,
        first_version: committed.first_version,
        batch_id: batch_id.clone(),
        fingerprint: fingerprint.clone(),
        added_rows: committed.added_rows,
        replayed: true,
    }))
}

/// Opens a version reachable from the current committed `HEAD` chain.
pub async fn open_table_at_version<S: Storage + ?Sized>(
    storage: Arc<S>,
    version: u64,
) -> Result<Table<S>, TableError> {
    open_table(storage)
        .await?
        .snapshot_at_version(version)
        .await
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
        publication_token: Some(head_object.version),
    })
}

async fn open_after_unknown<S: Storage + ?Sized>(
    table: &Table<S>,
    attempted_head: &Head,
    operation: crate::storage::StorageOperation,
    outcome_key: ObjectKey,
    message: String,
) -> Result<Table<S>, TableError> {
    match open_table(Arc::clone(&table.storage)).await {
        Ok(latest) => {
            if latest.table_id() != table.table_id() {
                return Err(TableError::CorruptMetadata {
                    key: head_key(),
                    message: "table identity changed during outcome reconciliation".to_owned(),
                });
            }
            if latest.head == *attempted_head || latest.version() >= table.version() {
                Ok(latest)
            } else {
                Err(TableError::UnknownCommitStatus {
                    operation,
                    key: outcome_key,
                    message,
                })
            }
        }
        Err(TableError::StorageUnavailable { .. })
        | Err(TableError::UnknownCommitStatus { .. }) => Err(TableError::UnknownCommitStatus {
            operation,
            key: outcome_key,
            message,
        }),
        Err(error) => Err(error),
    }
}

async fn read_manifest<S: Storage + ?Sized>(
    storage: &S,
    table: &TableMetadata,
    key: &ObjectKey,
) -> Result<ManifestV1, TableError> {
    let object = match storage.get_with_token(key).await {
        Ok(object) => object,
        Err(StorageError::NotFound { .. }) => {
            return Err(TableError::CorruptMetadata {
                key: key.clone(),
                message: "committed history names a manifest that does not exist".to_owned(),
            });
        }
        Err(error) => return Err(storage_error(key, error)),
    };
    let manifest: ManifestV1 = decode(key, &object.bytes)?;
    let head = head_for_manifest(&manifest);
    validate_snapshot(table, &head, &manifest).map_err(|error| metadata_error(key, error))?;
    Ok(manifest)
}

fn head_for_manifest(manifest: &ManifestV1) -> Head {
    Head {
        table_id: manifest.table_id,
        version: manifest.version,
        commit_id: manifest.commit_id,
        manifest_key: manifest_key(manifest.commit_id),
    }
}

fn validate_parent(
    child: &ManifestV1,
    parent: &ManifestV1,
    parent_key: &ObjectKey,
) -> Result<(), TableError> {
    let expected_version =
        child
            .version
            .checked_sub(1)
            .ok_or_else(|| TableError::CorruptMetadata {
                key: parent_key.clone(),
                message: "version-0 manifest unexpectedly names a parent".to_owned(),
            })?;
    if parent.version != expected_version {
        return Err(TableError::CorruptMetadata {
            key: parent_key.clone(),
            message: format!(
                "parent version {} does not precede child version {}",
                parent.version, child.version
            ),
        });
    }
    if parent.table_id != child.table_id
        || parent.schema_fingerprint != child.schema_fingerprint
        || manifest_key(parent.commit_id) != *parent_key
    {
        return Err(TableError::CorruptMetadata {
            key: parent_key.clone(),
            message: "parent identity does not match the committed history link".to_owned(),
        });
    }
    if child.files.len() <= parent.files.len() || child.files[..parent.files.len()] != parent.files
    {
        return Err(TableError::CorruptMetadata {
            key: parent_key.clone(),
            message: "child manifest does not preserve the complete parent file list".to_owned(),
        });
    }
    let expected_rows = parent
        .row_count
        .checked_add(child.operation.added_rows)
        .ok_or_else(|| TableError::CorruptMetadata {
            key: parent_key.clone(),
            message: "history row count overflow".to_owned(),
        })?;
    if child.row_count != expected_rows {
        return Err(TableError::CorruptMetadata {
            key: parent_key.clone(),
            message: "child row count does not equal parent plus appended rows".to_owned(),
        });
    }
    if parent
        .committed_batches
        .iter()
        .any(|(batch_id, receipt)| child.committed_batches.get(batch_id) != Some(receipt))
        || child.committed_batches.len() != parent.committed_batches.len() + 1
    {
        return Err(TableError::CorruptMetadata {
            key: parent_key.clone(),
            message: "child manifest does not preserve the cumulative batch registry".to_owned(),
        });
    }
    Ok(())
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
        | TableError::CorruptMetadata { .. }
        | TableError::CorruptData { .. }
        | TableError::MissingCommittedFile { .. }
        | TableError::BatchIdReuse { .. }
        | TableError::Conflict { .. }
        | TableError::VersionNotFound { .. }
        | TableError::HistoricalSnapshotReadOnly
        | TableError::EmptyBatch
        | TableError::InvalidBatch { .. } => observed,
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
