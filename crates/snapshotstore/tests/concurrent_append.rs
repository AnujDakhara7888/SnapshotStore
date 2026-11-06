use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use snapshotstore::errors::TableError;
use snapshotstore::metadata::{BatchId, CanonicalSchemaV1, DataTypeV1, SchemaFieldV1};
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::storage::{ObjectKey, Storage, StorageError, StoredObject, VersionToken};
use snapshotstore::table::{MAX_PUBLICATION_ATTEMPTS, append_batch, create_table, open_table};
use tempfile::tempdir;
use tokio::sync::Barrier;

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
    .expect("fixture matches schema")
}

fn batch_id(value: &str) -> BatchId {
    BatchId::parse(value).expect("test batch ID is valid")
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
        let index = self.replacements.fetch_add(1, Ordering::SeqCst);
        if key.as_str() == "HEAD" && index < 2 {
            self.barrier.wait().await;
        }
        self.inner
            .replace_if_token_matches(key, expected, bytes)
            .await
    }
}

fn racing_storage(path: &std::path::Path) -> Arc<FirstPublishBarrier> {
    Arc::new(FirstPublishBarrier {
        inner: LocalStorage::open(path).expect("open local storage"),
        barrier: Barrier::new(2),
        replacements: AtomicUsize::new(0),
    })
}

#[tokio::test]
async fn concurrent_different_batches_preserve_both_commits() {
    for _ in 0..20 {
        let directory = tempdir().expect("temporary directory");
        let storage = racing_storage(directory.path());
        create_table(Arc::clone(&storage), schema())
            .await
            .expect("create table");
        let left = open_table(Arc::clone(&storage)).await.expect("open left");
        let right = open_table(Arc::clone(&storage)).await.expect("open right");
        let left_batch = batch(vec![10, 11]);
        let right_batch = batch(vec![20]);

        let (left_result, right_result) = tokio::join!(
            append_batch(&left, batch_id("left"), &left_batch),
            append_batch(&right, batch_id("right"), &right_batch)
        );
        let left_result = left_result.expect("left append resolves");
        let right_result = right_result.expect("right append resolves");
        let mut versions = [
            left_result.receipt.first_version,
            right_result.receipt.first_version,
        ];
        versions.sort_unstable();
        assert_eq!(versions, [1, 2]);

        let latest = open_table(Arc::clone(&storage)).await.expect("open latest");
        assert_eq!(latest.version(), 2);
        assert_eq!(latest.manifest().row_count, 3);
        assert_eq!(latest.manifest().files.len(), 2);
        assert_eq!(latest.manifest().committed_batches.len(), 2);
        assert!(
            latest
                .manifest()
                .committed_batches
                .contains_key(&batch_id("left"))
        );
        assert!(
            latest
                .manifest()
                .committed_batches
                .contains_key(&batch_id("right"))
        );
        assert_eq!(
            latest
                .history()
                .await
                .expect("committed history")
                .iter()
                .map(|snapshot| snapshot.version)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let mut values = latest
            .read_batches()
            .await
            .expect("read committed rows")
            .into_iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("uint64 column")
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, vec![10, 11, 20]);
        assert_eq!(storage.replacements.load(Ordering::SeqCst), 3);
    }
}

#[tokio::test]
async fn concurrent_matching_batch_ids_resolve_to_one_receipt() {
    let directory = tempdir().expect("temporary directory");
    let storage = racing_storage(directory.path());
    create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let left = open_table(Arc::clone(&storage)).await.expect("open left");
    let right = open_table(Arc::clone(&storage)).await.expect("open right");
    let left_batch = batch(vec![7, 8]);
    let right_batch = batch(vec![7, 8]);

    let (left_result, right_result) = tokio::join!(
        append_batch(&left, batch_id("same"), &left_batch),
        append_batch(&right, batch_id("same"), &right_batch)
    );
    let left_result = left_result.expect("left append resolves");
    let right_result = right_result.expect("right append resolves");
    assert_eq!(
        left_result.receipt.first_commit_id,
        right_result.receipt.first_commit_id
    );
    assert_eq!(left_result.receipt.first_version, 1);
    assert_eq!(right_result.receipt.first_version, 1);
    assert_ne!(left_result.receipt.replayed, right_result.receipt.replayed);

    let latest = open_table(storage).await.expect("open latest");
    assert_eq!(latest.version(), 1);
    assert_eq!(latest.manifest().row_count, 2);
    assert_eq!(latest.manifest().committed_batches.len(), 1);
}

#[tokio::test]
async fn concurrent_changed_content_under_one_id_is_rejected() {
    let directory = tempdir().expect("temporary directory");
    let storage = racing_storage(directory.path());
    create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let left = open_table(Arc::clone(&storage)).await.expect("open left");
    let right = open_table(Arc::clone(&storage)).await.expect("open right");
    let left_batch = batch(vec![1]);
    let right_batch = batch(vec![2]);

    let (left_result, right_result) = tokio::join!(
        append_batch(&left, batch_id("same"), &left_batch),
        append_batch(&right, batch_id("same"), &right_batch)
    );
    assert!(left_result.is_ok() ^ right_result.is_ok());
    let error = match (left_result, right_result) {
        (Err(error), Ok(_)) | (Ok(_), Err(error)) => error,
        _ => panic!("exactly one changed-content append must fail"),
    };
    assert!(matches!(error, TableError::BatchIdReuse { .. }));

    let latest = open_table(storage).await.expect("open latest");
    assert_eq!(latest.version(), 1);
    assert_eq!(latest.manifest().row_count, 1);
    assert_eq!(latest.manifest().committed_batches.len(), 1);
}

#[derive(Debug)]
struct AlwaysConflictingStorage {
    inner: LocalStorage,
    replacements: AtomicUsize,
}

#[async_trait]
impl Storage for AlwaysConflictingStorage {
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
        _expected: &VersionToken,
        _bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        self.replacements.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::PreconditionFailed { key: key.clone() })
    }
}

#[tokio::test]
async fn repeated_conflicts_stop_at_the_publication_bound() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(AlwaysConflictingStorage {
        inner: LocalStorage::open(directory.path()).expect("open local storage"),
        replacements: AtomicUsize::new(0),
    });
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");

    assert!(matches!(
        append_batch(&table, batch_id("bounded"), &batch(vec![1])).await,
        Err(TableError::Conflict {
            expected_version: 0,
            latest_version: 0,
            attempts: MAX_PUBLICATION_ATTEMPTS,
        })
    ));
    assert_eq!(
        storage.replacements.load(Ordering::SeqCst),
        MAX_PUBLICATION_ATTEMPTS as usize
    );
    assert_eq!(
        open_table(storage)
            .await
            .expect("HEAD remains unchanged")
            .version(),
        0
    );
}
