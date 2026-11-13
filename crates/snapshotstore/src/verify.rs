//! Committed-history integrity checks and local orphan diagnostics.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use serde::Serialize;
use uuid::Uuid;

use crate::errors::TableError;
use crate::metadata::manifest_key;
use crate::storage::Storage;
use crate::storage::local::LocalStorage;
use crate::table::open_table;

/// Counts established by reading every committed snapshot in the reachable chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationReport {
    pub table_id: Uuid,
    pub latest_version: u64,
    pub snapshots_verified: u64,
    pub active_files_verified: u64,
    pub rows_verified: u64,
    pub committed_batches_verified: u64,
}

/// A committed verification report plus unreachable local objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LocalVerificationReport {
    #[serde(flatten)]
    pub committed: VerificationReport,
    pub unreachable_data_files: Vec<String>,
    pub unreachable_manifests: Vec<String>,
}

/// Verifies the complete reachable manifest chain and every committed data file.
///
/// Each historical snapshot is reopened through the committed parent chain and
/// read in full, checking file length, checksum, schema, row count, history
/// continuity, and cumulative batch-registry invariants.
pub async fn verify_table<S: Storage + ?Sized>(
    storage: Arc<S>,
) -> Result<VerificationReport, TableError> {
    let latest = open_table(Arc::clone(&storage)).await?;
    let history = latest.history().await?;
    for snapshot in &history {
        latest
            .snapshot_at_version(snapshot.version)
            .await?
            .read_batches()
            .await?;
    }

    Ok(VerificationReport {
        table_id: latest.table_id(),
        latest_version: latest.version(),
        snapshots_verified: history.len() as u64,
        active_files_verified: latest.manifest().files.len() as u64,
        rows_verified: latest.manifest().row_count,
        committed_batches_verified: latest.manifest().committed_batches.len() as u64,
    })
}

/// Verifies committed state and reports conventionally named unreachable local objects.
///
/// Object enumeration is diagnostic only. Reachable state is still discovered
/// exclusively through `HEAD` and its immutable parent-manifest chain.
pub async fn verify_local_table(
    storage: Arc<LocalStorage>,
) -> Result<LocalVerificationReport, TableError> {
    let latest = open_table(Arc::clone(&storage)).await?;
    let history = latest.history().await?;
    let reachable_manifests = history
        .iter()
        .map(|snapshot| manifest_key(snapshot.commit_id).as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let reachable_data = latest
        .manifest()
        .files
        .iter()
        .map(|file| file.object_key.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let committed = verify_table(Arc::clone(&storage)).await?;

    let unreachable_data_files = relative_files(storage.root(), "data")
        .map_err(local_diagnostic_error)?
        .into_iter()
        .filter(|key| !reachable_data.contains(key))
        .collect();
    let unreachable_manifests = relative_files(storage.root(), "manifests")
        .map_err(local_diagnostic_error)?
        .into_iter()
        .filter(|key| !reachable_manifests.contains(key))
        .collect();

    Ok(LocalVerificationReport {
        committed,
        unreachable_data_files,
        unreachable_manifests,
    })
}

fn relative_files(root: &Path, directory: &str) -> io::Result<Vec<String>> {
    let start = root.join(directory);
    if !start.exists() {
        return Ok(Vec::new());
    }
    let mut pending = vec![start];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                let relative = entry
                    .path()
                    .strip_prefix(root)
                    .expect("diagnostic traversal stays under the storage root")
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/");
                files.push(relative);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn local_diagnostic_error(error: io::Error) -> TableError {
    TableError::StorageUnavailable {
        operation: crate::storage::StorageOperation::GetWithToken,
        message: format!("local diagnostic listing failed: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::metadata::{BatchId, CanonicalSchemaV1, DataTypeV1, SchemaFieldV1};
    use crate::storage::{ObjectKey, Storage};
    use crate::table::{append_batch, create_table};

    use super::*;

    #[tokio::test]
    async fn verifies_history_and_reports_local_orphans_without_removing_them() {
        let directory = tempdir().expect("temporary directory");
        let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
        let schema = CanonicalSchemaV1 {
            fields: vec![SchemaFieldV1 {
                name: "value".to_owned(),
                data_type: DataTypeV1::UInt64,
                nullable: false,
            }],
        };
        let table = create_table(Arc::clone(&storage), schema)
            .await
            .expect("create table");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef],
        )
        .expect("fixture batch");
        append_batch(&table, BatchId::parse("batch-a").expect("batch ID"), &batch)
            .await
            .expect("append batch");

        let orphan_data = ObjectKey::parse(
            "data/00000000-0000-4000-8000-000000000001/00000000-0000-4000-8000-000000000002.parquet",
        )
        .expect("orphan data key");
        let orphan_manifest =
            ObjectKey::parse("manifests/00000000-0000-4000-8000-000000000003.json")
                .expect("orphan manifest key");
        storage
            .put_immutable(&orphan_data, Bytes::from_static(b"orphan-data"))
            .await
            .expect("write orphan data");
        storage
            .put_immutable(&orphan_manifest, Bytes::from_static(b"orphan-manifest"))
            .await
            .expect("write orphan manifest");

        let report = verify_local_table(Arc::clone(&storage))
            .await
            .expect("verify table");
        assert_eq!(report.committed.latest_version, 1);
        assert_eq!(report.committed.snapshots_verified, 2);
        assert_eq!(report.committed.active_files_verified, 1);
        assert_eq!(report.committed.rows_verified, 2);
        assert_eq!(report.committed.committed_batches_verified, 1);
        assert_eq!(
            report.unreachable_data_files,
            vec![orphan_data.as_str().to_owned()]
        );
        assert_eq!(
            report.unreachable_manifests,
            vec![orphan_manifest.as_str().to_owned()]
        );
        assert!(directory.path().join(orphan_data.as_str()).exists());
        assert!(directory.path().join(orphan_manifest.as_str()).exists());
    }
}
