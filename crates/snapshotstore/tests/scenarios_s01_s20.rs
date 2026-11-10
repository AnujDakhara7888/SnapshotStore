mod support;

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use snapshotstore::errors::TableError;
use snapshotstore::metadata::{BatchId, CanonicalSchemaV1, DataTypeV1, SchemaFieldV1};
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::storage::{
    ObjectKey, Storage, StorageError, StorageOperation, StoredObject, VersionToken,
};
use snapshotstore::table::{
    AppendResult, MAX_PUBLICATION_ATTEMPTS, Table, append_batch, create_table, open_table,
};
use tempfile::{TempDir, tempdir};
use tokio::sync::Barrier;

use support::{AppendReferenceModel, FaultPlan, FaultStorage, WriteBoundary};

fn schema() -> CanonicalSchemaV1 {
    CanonicalSchemaV1 {
        fields: vec![SchemaFieldV1 {
            name: "value".to_owned(),
            data_type: DataTypeV1::UInt64,
            nullable: false,
        }],
    }
}

fn batch(values: &[u64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(values.to_vec())) as ArrayRef],
    )
    .expect("fixture matches schema")
}

fn batch_id(value: &str) -> BatchId {
    BatchId::parse(value).expect("scenario batch ID is valid")
}

async fn local_fixture() -> (TempDir, Arc<LocalStorage>, Table<LocalStorage>) {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    (directory, storage, table)
}

async fn fault_fixture() -> (TempDir, Arc<FaultStorage>, Table<FaultStorage>) {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(FaultStorage::open(directory.path()));
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table before fault");
    (directory, storage, table)
}

fn values_from_batches(batches: &[RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("scenario fixture has one UInt64 column")
                .values()
                .iter()
                .copied()
        })
        .collect()
}

fn count_files(path: &std::path::Path) -> usize {
    if !path.exists() {
        return 0;
    }
    fs::read_dir(path)
        .expect("read scenario directory")
        .map(|entry| entry.expect("read scenario entry").path())
        .map(|path| if path.is_dir() { count_files(&path) } else { 1 })
        .sum()
}

#[derive(Debug)]
struct CreatorRaceStorage {
    inner: LocalStorage,
    barrier: Barrier,
}

#[async_trait]
impl Storage for CreatorRaceStorage {
    async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
        self.inner.get_with_token(key).await
    }

    async fn put_immutable(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        if key.as_str() == "table.json" {
            self.barrier.wait().await;
        }
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
        self.inner
            .replace_if_token_matches(key, expected, bytes)
            .await
    }
}

#[derive(Debug)]
struct ObservedStorage {
    inner: LocalStorage,
    first_replacement_barrier: Option<Barrier>,
    barrier_parties: usize,
    replacements: AtomicUsize,
    precondition_failures: AtomicUsize,
}

impl ObservedStorage {
    fn open(path: &std::path::Path, barrier_parties: usize) -> Self {
        Self {
            inner: LocalStorage::open(path).expect("open observed local storage"),
            first_replacement_barrier: (barrier_parties > 0).then(|| Barrier::new(barrier_parties)),
            barrier_parties,
            replacements: AtomicUsize::new(0),
            precondition_failures: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl Storage for ObservedStorage {
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
        if key.as_str() == "HEAD" && index < self.barrier_parties {
            self.first_replacement_barrier
                .as_ref()
                .expect("positive party count installs a barrier")
                .wait()
                .await;
        }
        let result = self
            .inner
            .replace_if_token_matches(key, expected, bytes)
            .await;
        if matches!(result, Err(StorageError::PreconditionFailed { .. })) {
            self.precondition_failures.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
}

#[derive(Debug)]
struct ArmedConflictStorage {
    inner: LocalStorage,
    armed: AtomicBool,
    rejected_replacements: AtomicUsize,
}

#[async_trait]
impl Storage for ArmedConflictStorage {
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
        if self.armed.load(Ordering::SeqCst) {
            self.rejected_replacements.fetch_add(1, Ordering::SeqCst);
            return Err(StorageError::PreconditionFailed { key: key.clone() });
        }
        self.inner
            .replace_if_token_matches(key, expected, bytes)
            .await
    }
}

#[tokio::test]
async fn s01_two_creators_race() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(CreatorRaceStorage {
        inner: LocalStorage::open(directory.path()).expect("open local storage"),
        barrier: Barrier::new(2),
    });
    let (left, right) = tokio::join!(
        create_table(Arc::clone(&storage), schema()),
        create_table(Arc::clone(&storage), schema())
    );
    let left = left.expect("left creator converges");
    let right = right.expect("right creator converges");
    assert_eq!(left.table_id(), right.table_id());
    assert_eq!(left.commit_id(), right.commit_id());
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}

#[tokio::test]
async fn s02_reader_pinned_before_append() {
    let (_directory, storage, pinned) = local_fixture().await;
    let values = [1, 2];
    let result = append_batch(&pinned, batch_id("first"), &batch(&values))
        .await
        .expect("append first batch");
    assert!(pinned.read_batches().await.expect("read pinned").is_empty());
    assert_eq!(pinned.version(), 0);
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("first", &values, &result.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s03_reader_opens_after_append() {
    let (_directory, storage, table) = local_fixture().await;
    let values = [3, 4, 5];
    let result = append_batch(&table, batch_id("visible"), &batch(&values))
        .await
        .expect("append visible batch");
    let reader = open_table(Arc::clone(&storage)).await.expect("open reader");
    assert_eq!(reader.version(), 1);
    assert_eq!(
        values_from_batches(&reader.read_batches().await.expect("read complete batch")),
        values
    );
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("visible", &values, &result.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s04_two_distinct_writers_share_a_base() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(ObservedStorage::open(directory.path(), 2));
    create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let left = open_table(Arc::clone(&storage)).await.expect("open left");
    let right = open_table(Arc::clone(&storage)).await.expect("open right");
    let left_values = [10];
    let right_values = [20, 21];
    let left_batch = batch(&left_values);
    let right_batch = batch(&right_values);
    let (left, right) = tokio::join!(
        append_batch(&left, batch_id("left"), &left_batch),
        append_batch(&right, batch_id("right"), &right_batch)
    );
    let left = left.expect("left resolves");
    let right = right.expect("right resolves");
    assert_eq!(storage.precondition_failures.load(Ordering::SeqCst), 1);
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("left", &left_values, &left.receipt);
    model.apply_receipt("right", &right_values, &right.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s05_ten_distinct_concurrent_batches() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(ObservedStorage::open(directory.path(), 10));
    create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");

    let mut handles = Vec::new();
    for index in 0..10_u64 {
        let storage = Arc::clone(&storage);
        handles.push(tokio::spawn(async move {
            let id = format!("batch-{index}");
            let values = vec![index * 10, index * 10 + 1];
            let table = open_table(Arc::clone(&storage))
                .await
                .expect("open concurrent writer");
            let result = append_batch(&table, batch_id(&id), &batch(&values)).await;
            (id, values, result)
        }));
    }

    let mut model = AppendReferenceModel::default();
    let mut retry = Vec::new();
    for handle in handles {
        let (id, values, result) = handle.await.expect("writer task completes");
        match result {
            Ok(result) => model.apply_receipt(&id, &values, &result.receipt),
            Err(TableError::Conflict { .. }) => retry.push((id, values)),
            Err(error) => panic!("unexpected concurrent append error: {error}"),
        }
    }
    for (id, values) in retry {
        let table = open_table(Arc::clone(&storage))
            .await
            .expect("open retry base");
        let result = append_batch(&table, batch_id(&id), &batch(&values))
            .await
            .expect("bounded harness retry succeeds");
        model.apply_receipt(&id, &values, &result.receipt);
    }
    assert!(storage.precondition_failures.load(Ordering::SeqCst) >= 9);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s06_concurrent_same_batch_id_and_contents() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(ObservedStorage::open(directory.path(), 2));
    create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let left = open_table(Arc::clone(&storage)).await.expect("open left");
    let right = open_table(Arc::clone(&storage)).await.expect("open right");
    let values = [6, 7];
    let left_batch = batch(&values);
    let right_batch = batch(&values);
    let (left, right) = tokio::join!(
        append_batch(&left, batch_id("same"), &left_batch),
        append_batch(&right, batch_id("same"), &right_batch)
    );
    let mut results = [left.expect("left resolves"), right.expect("right resolves")];
    results.sort_by_key(|result| result.receipt.replayed);
    assert!(!results[0].receipt.replayed);
    assert!(results[1].receipt.replayed);
    assert_eq!(
        results[0].receipt.first_commit_id,
        results[1].receipt.first_commit_id
    );
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("same", &values, &results[0].receipt);
    model.apply_receipt("same", &values, &results[1].receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s07_concurrent_same_batch_id_different_contents() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(ObservedStorage::open(directory.path(), 2));
    create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let left = open_table(Arc::clone(&storage)).await.expect("open left");
    let right = open_table(Arc::clone(&storage)).await.expect("open right");
    let left_values = [70];
    let right_values = [71];
    let left_batch = batch(&left_values);
    let right_batch = batch(&right_values);
    let (left, right) = tokio::join!(
        append_batch(&left, batch_id("same"), &left_batch),
        append_batch(&right, batch_id("same"), &right_batch)
    );
    let (winner_values, winner): (&[u64], AppendResult<ObservedStorage>) = match (left, right) {
        (Ok(winner), Err(TableError::BatchIdReuse { .. })) => (&left_values, winner),
        (Err(TableError::BatchIdReuse { .. }), Ok(winner)) => (&right_values, winner),
        (left, right) => panic!("expected one commit and one ID reuse: {left:?}, {right:?}"),
    };
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("same", winner_values, &winner.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s08_sequential_successful_replay() {
    let (_directory, storage, table) = local_fixture().await;
    let values = [80, 81];
    let first = append_batch(&table, batch_id("replay"), &batch(&values))
        .await
        .expect("first append");
    let replay = append_batch(&first.table, batch_id("replay"), &batch(&values))
        .await
        .expect("matching replay");
    assert_eq!(
        first.receipt.first_commit_id,
        replay.receipt.first_commit_id
    );
    assert_eq!(first.receipt.first_version, replay.receipt.first_version);
    assert!(replay.receipt.replayed);
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("replay", &values, &first.receipt);
    model.apply_receipt("replay", &values, &replay.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s09_sequential_reused_id_changed_contents() {
    let (_directory, storage, table) = local_fixture().await;
    let values = [90];
    let first = append_batch(&table, batch_id("fixed-id"), &batch(&values))
        .await
        .expect("first append");
    assert!(matches!(
        append_batch(&first.table, batch_id("fixed-id"), &batch(&[91])).await,
        Err(TableError::BatchIdReuse { .. })
    ));
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("fixed-id", &values, &first.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s10_crash_before_first_data_upload() {
    let (_directory, storage, table) = fault_fixture().await;
    storage.arm(FaultPlan::unavailable_before(WriteBoundary::Data));
    assert!(matches!(
        append_batch(&table, batch_id("before-data"), &batch(&[100])).await,
        Err(TableError::StorageUnavailable { .. })
    ));
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}

#[tokio::test]
async fn s11_crash_during_upload() {
    let (_directory, storage, table) = fault_fixture().await;
    storage.arm(FaultPlan::unknown_before(WriteBoundary::Data));
    assert!(matches!(
        append_batch(&table, batch_id("during-data"), &batch(&[110])).await,
        Err(TableError::UnknownCommitStatus { .. })
    ));
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}

#[tokio::test]
async fn s12_crash_after_data_before_manifest() {
    let (directory, storage, table) = fault_fixture().await;
    storage.arm(FaultPlan::unavailable_before(WriteBoundary::Manifest));
    assert!(matches!(
        append_batch(&table, batch_id("after-data"), &batch(&[120])).await,
        Err(TableError::StorageUnavailable { .. })
    ));
    assert_eq!(count_files(&directory.path().join("data")), 1);
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}

#[tokio::test]
async fn s13_crash_during_manifest_write() {
    let (directory, storage, table) = fault_fixture().await;
    storage.arm(FaultPlan::unknown_before(WriteBoundary::Manifest));
    assert!(matches!(
        append_batch(&table, batch_id("during-manifest"), &batch(&[130])).await,
        Err(TableError::UnknownCommitStatus { .. })
    ));
    assert_eq!(count_files(&directory.path().join("manifests")), 1);
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}

#[tokio::test]
async fn s14_crash_after_manifest_before_head() {
    let (directory, storage, table) = fault_fixture().await;
    storage.arm(FaultPlan::unavailable_before(WriteBoundary::Head));
    assert!(matches!(
        append_batch(&table, batch_id("after-manifest"), &batch(&[140])).await,
        Err(TableError::StorageUnavailable { .. })
    ));
    assert_eq!(count_files(&directory.path().join("manifests")), 2);
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}

#[tokio::test]
async fn s15_head_success_response_lost() {
    let (_directory, storage, table) = fault_fixture().await;
    storage.arm(FaultPlan::unknown_after(WriteBoundary::Head));
    let values = [150, 151];
    let first = append_batch(&table, batch_id("lost-response"), &batch(&values))
        .await
        .expect("reconcile applied HEAD");
    assert!(!first.receipt.replayed);
    let reopened = open_table(Arc::clone(&storage))
        .await
        .expect("reopen table");
    let replay = append_batch(&reopened, batch_id("lost-response"), &batch(&values))
        .await
        .expect("retry discovers commit");
    assert!(replay.receipt.replayed);
    assert_eq!(
        first.receipt.first_commit_id,
        replay.receipt.first_commit_id
    );
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("lost-response", &values, &first.receipt);
    model.apply_receipt("lost-response", &values, &replay.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s16_timeout_before_head_takes_effect() {
    let (_directory, storage, table) = fault_fixture().await;
    storage.arm(FaultPlan::unknown_before(WriteBoundary::Head));
    let values = [160];
    let first = append_batch(&table, batch_id("head-timeout"), &batch(&values))
        .await
        .expect("retry unapplied HEAD outcome");
    assert_eq!(storage.head_replacements(), 2);
    let replay = append_batch(&first.table, batch_id("head-timeout"), &batch(&values))
        .await
        .expect("matching replay");
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("head-timeout", &values, &first.receipt);
    model.apply_receipt("head-timeout", &values, &replay.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s17_writer_has_stale_head_token() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(ObservedStorage::open(directory.path(), 0));
    create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let winner_base = open_table(Arc::clone(&storage)).await.expect("open winner");
    let stale = open_table(Arc::clone(&storage))
        .await
        .expect("open stale writer");
    let winner_values = [170];
    let stale_values = [171];
    let winner = append_batch(&winner_base, batch_id("winner"), &batch(&winner_values))
        .await
        .expect("winner commits");
    let rebased = append_batch(&stale, batch_id("stale"), &batch(&stale_values))
        .await
        .expect("stale writer rebases");
    assert_eq!(storage.precondition_failures.load(Ordering::SeqCst), 1);
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("winner", &winner_values, &winner.receipt);
    model.apply_receipt("stale", &stale_values, &rebased.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s18_writer_loses_several_consecutive_races() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(ArmedConflictStorage {
        inner: LocalStorage::open(directory.path()).expect("open local storage"),
        armed: AtomicBool::new(false),
        rejected_replacements: AtomicUsize::new(0),
    });
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let winner_values = [180];
    let winner = append_batch(&table, batch_id("preserved"), &batch(&winner_values))
        .await
        .expect("commit preserved batch");
    let base = open_table(Arc::clone(&storage))
        .await
        .expect("open current base");
    storage.armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        append_batch(&base, batch_id("exhausted"), &batch(&[181])).await,
        Err(TableError::Conflict {
            expected_version: 1,
            latest_version: 1,
            attempts: MAX_PUBLICATION_ATTEMPTS,
        })
    ));
    assert_eq!(
        storage.rejected_replacements.load(Ordering::SeqCst),
        MAX_PUBLICATION_ATTEMPTS as usize
    );
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("preserved", &winner_values, &winner.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s19_storage_transient_read_failure() {
    let (_directory, storage, _table) = fault_fixture().await;
    storage.fail_next_read();
    assert!(matches!(
        open_table(Arc::clone(&storage)).await,
        Err(TableError::StorageUnavailable {
            operation: StorageOperation::GetWithToken,
            ..
        })
    ));
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}

#[tokio::test]
async fn s20_storage_transient_write_failure() {
    let (_directory, storage, table) = fault_fixture().await;
    storage.arm(FaultPlan::unavailable_before(WriteBoundary::Data));
    assert!(matches!(
        append_batch(&table, batch_id("write-failure"), &batch(&[200])).await,
        Err(TableError::StorageUnavailable {
            operation: StorageOperation::PutImmutable,
            ..
        })
    ));
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}
