use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
use snapshotstore::errors::TableError;
use snapshotstore::metadata::{BatchId, CanonicalSchemaV1, DataTypeV1, SchemaFieldV1};
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::storage::{
    ObjectKey, Storage, StorageError, StorageOperation, StoredObject, VersionToken,
};
use snapshotstore::table::{Table, append_batch, create_table, open_table};
use snapshotstore::verify::{LocalVerificationReport, verify_local_table};
use tempfile::tempdir;
use tokio::sync::Barrier;
use uuid::Uuid;

#[derive(Debug, Serialize)]
struct PinnedReadEvidence {
    pinned_version: u64,
    pinned_values_after_later_commit: Vec<u64>,
    latest_version: u64,
    latest_values: Vec<u64>,
}

#[derive(Debug, Serialize)]
struct ReplayEvidence {
    first_version: u64,
    replayed: bool,
    version_after_replay: u64,
    changed_content_rejected: bool,
}

#[derive(Debug, Serialize)]
struct RaceEvidence {
    committed_versions: Vec<u64>,
    values_after_race: Vec<u64>,
}

#[derive(Debug, Serialize)]
struct RecoveryEvidence {
    failed_before_head: bool,
    version_after_restart: u64,
    recovered_version: u64,
    recovered_values: Vec<u64>,
    unreachable_data_files: usize,
    unreachable_manifests: usize,
}

#[derive(Debug, Serialize)]
struct DemoReport {
    backend: &'static str,
    table_id: Uuid,
    pinned_read: PinnedReadEvidence,
    replay: ReplayEvidence,
    concurrent_writers: RaceEvidence,
    recovery: RecoveryEvidence,
    history_versions: Vec<u64>,
    verification: LocalVerificationReport,
}

#[derive(Debug)]
struct FirstPublishBarrier {
    inner: LocalStorage,
    barrier: Barrier,
    replacements: AtomicUsize,
}

#[async_trait]
impl Storage for FirstPublishBarrier {
    async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
        self.inner.get_with_token(key).await
    }

    async fn put_immutable(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        self.inner.put_immutable(key, bytes).await
    }

    async fn create_if_absent(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        self.inner.create_if_absent(key, bytes).await
    }

    async fn replace_if_token_matches(
        &self,
        key: &ObjectKey,
        expected: &VersionToken,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        let replacement = self.replacements.fetch_add(1, Ordering::SeqCst);
        if key.as_str() == "HEAD" && replacement < 2 {
            self.barrier.wait().await;
        }
        self.inner
            .replace_if_token_matches(key, expected, bytes)
            .await
    }
}

#[derive(Debug)]
struct FailNextHead {
    inner: LocalStorage,
    armed: AtomicBool,
}

#[async_trait]
impl Storage for FailNextHead {
    async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
        self.inner.get_with_token(key).await
    }

    async fn put_immutable(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        self.inner.put_immutable(key, bytes).await
    }

    async fn create_if_absent(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        self.inner.create_if_absent(key, bytes).await
    }

    async fn replace_if_token_matches(
        &self,
        key: &ObjectKey,
        expected: &VersionToken,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        if key.as_str() == "HEAD" && self.armed.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Unavailable {
                operation: StorageOperation::ReplaceIfTokenMatches,
                message: "injected failure before HEAD publication".to_owned(),
            });
        }
        self.inner
            .replace_if_token_matches(key, expected, bytes)
            .await
    }
}

fn schema() -> CanonicalSchemaV1 {
    CanonicalSchemaV1 {
        fields: vec![SchemaFieldV1 {
            name: "value".to_owned(),
            data_type: DataTypeV1::UInt64,
            nullable: false,
        }],
    }
}

fn batch(values: Vec<u64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(values)) as ArrayRef],
    )
    .expect("demo batch matches the table schema")
}

fn batch_id(value: &str) -> BatchId {
    BatchId::parse(value).expect("demo batch ID is valid")
}

async fn values<S: Storage + ?Sized>(table: &Table<S>) -> Vec<u64> {
    table
        .read_batches()
        .await
        .expect("read committed demo rows")
        .into_iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("demo column remains uint64")
                .values()
                .to_vec()
        })
        .collect()
}

async fn run_demo() -> DemoReport {
    let directory = tempdir().expect("create demo directory");
    let local = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let empty = create_table(Arc::clone(&local), schema())
        .await
        .expect("create empty table");
    assert_eq!(empty.version(), 0);

    let first = append_batch(&empty, batch_id("batch-a"), &batch(vec![1, 2]))
        .await
        .expect("append batch A");
    let pinned = open_table(Arc::clone(&local)).await.expect("pin version 1");
    assert_eq!(first.version(), pinned.version());
    let second = append_batch(&pinned, batch_id("batch-b"), &batch(vec![3, 4]))
        .await
        .expect("append batch B");
    assert_eq!(pinned.version(), 1);
    assert_eq!(values(&pinned).await, vec![1, 2]);
    assert_eq!(values(&second).await, vec![1, 2, 3, 4]);

    let replay = append_batch(&second, batch_id("batch-b"), &batch(vec![3, 4]))
        .await
        .expect("replay batch B");
    assert!(replay.receipt.replayed);
    assert_eq!(replay.receipt.first_version, 2);
    assert_eq!(replay.version(), 2);

    let changed_content_rejected = matches!(
        append_batch(&replay, batch_id("batch-b"), &batch(vec![30, 40])).await,
        Err(TableError::BatchIdReuse { .. })
    );
    assert!(changed_content_rejected);

    let racing = Arc::new(FirstPublishBarrier {
        inner: LocalStorage::open(directory.path()).expect("open race storage"),
        barrier: Barrier::new(2),
        replacements: AtomicUsize::new(0),
    });
    let left = open_table(Arc::clone(&racing))
        .await
        .expect("open left writer");
    let right = open_table(Arc::clone(&racing))
        .await
        .expect("open right writer");
    let left_batch = batch(vec![5]);
    let right_batch = batch(vec![6, 7]);
    let (left_result, right_result) = tokio::join!(
        append_batch(&left, batch_id("batch-c"), &left_batch),
        append_batch(&right, batch_id("batch-d"), &right_batch)
    );
    let left_result = left_result.expect("left writer rebases");
    let right_result = right_result.expect("right writer rebases");
    let mut raced = vec![
        (left_result.receipt.first_version, vec![5]),
        (right_result.receipt.first_version, vec![6, 7]),
    ];
    raced.sort_by_key(|(version, _)| *version);
    assert_eq!(
        raced
            .iter()
            .map(|(version, _)| *version)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    let mut reference_values = vec![1, 2, 3, 4];
    for (_, committed_values) in &raced {
        reference_values.extend(committed_values);
    }
    let after_race = open_table(Arc::clone(&local))
        .await
        .expect("open after writer race");
    assert_eq!(values(&after_race).await, reference_values);

    let faulty = Arc::new(FailNextHead {
        inner: LocalStorage::open(directory.path()).expect("open fault storage"),
        armed: AtomicBool::new(true),
    });
    let fault_base = open_table(Arc::clone(&faulty))
        .await
        .expect("open fault base");
    let failed_before_head = matches!(
        append_batch(&fault_base, batch_id("batch-e"), &batch(vec![8, 9])).await,
        Err(TableError::StorageUnavailable {
            operation: StorageOperation::ReplaceIfTokenMatches,
            ..
        })
    );
    assert!(failed_before_head);

    let after_restart = open_table(Arc::clone(&local))
        .await
        .expect("restart after failed publication");
    assert_eq!(after_restart.version(), 4);
    assert_eq!(values(&after_restart).await, reference_values);

    let recovered = append_batch(&after_restart, batch_id("batch-e"), &batch(vec![8, 9]))
        .await
        .expect("retry after restart");
    reference_values.extend([8, 9]);
    assert_eq!(recovered.version(), 5);
    assert_eq!(values(&recovered).await, reference_values);

    let history_versions = recovered
        .history()
        .await
        .expect("read committed history")
        .into_iter()
        .map(|snapshot| snapshot.version)
        .collect::<Vec<_>>();
    assert_eq!(history_versions, vec![0, 1, 2, 3, 4, 5]);

    let verification = verify_local_table(Arc::clone(&local))
        .await
        .expect("verify reachable state");
    assert_eq!(verification.committed.latest_version, 5);
    assert_eq!(verification.committed.snapshots_verified, 6);
    assert_eq!(verification.committed.rows_verified, 9);
    assert_eq!(verification.committed.committed_batches_verified, 5);
    assert!(!verification.unreachable_data_files.is_empty());
    assert!(!verification.unreachable_manifests.is_empty());

    DemoReport {
        backend: "local-filesystem",
        table_id: recovered.table_id(),
        pinned_read: PinnedReadEvidence {
            pinned_version: pinned.version(),
            pinned_values_after_later_commit: values(&pinned).await,
            latest_version: second.version(),
            latest_values: values(&second).await,
        },
        replay: ReplayEvidence {
            first_version: replay.receipt.first_version,
            replayed: replay.receipt.replayed,
            version_after_replay: replay.version(),
            changed_content_rejected,
        },
        concurrent_writers: RaceEvidence {
            committed_versions: raced.iter().map(|(version, _)| *version).collect(),
            values_after_race: values(&after_race).await,
        },
        recovery: RecoveryEvidence {
            failed_before_head,
            version_after_restart: after_restart.version(),
            recovered_version: recovered.version(),
            recovered_values: values(&recovered).await,
            unreachable_data_files: verification.unreachable_data_files.len(),
            unreachable_manifests: verification.unreachable_manifests.len(),
        },
        history_versions,
        verification,
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let report = run_demo().await;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize demo report")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_release_demo_completes() {
        let report = run_demo().await;
        assert_eq!(report.verification.committed.latest_version, 5);
        assert_eq!(report.recovery.recovered_values.len(), 9);
    }
}
