use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use snapshotstore::errors::TableError;
use snapshotstore::metadata::{
    CanonicalSchemaV1, DataTypeV1, FORMAT_VERSION, SchemaFieldV1, TableMetadata,
};
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::storage::{
    ObjectKey, Storage, StorageError, StorageOperation, StoredObject, VersionToken,
};
use snapshotstore::table::{create_table, open_table};
use tempfile::tempdir;
use tokio::sync::Barrier;
use uuid::Uuid;

fn schema() -> CanonicalSchemaV1 {
    CanonicalSchemaV1 {
        fields: vec![
            SchemaFieldV1 {
                name: "event_id".to_owned(),
                data_type: DataTypeV1::UInt64,
                nullable: false,
            },
            SchemaFieldV1 {
                name: "message".to_owned(),
                data_type: DataTypeV1::Utf8,
                nullable: true,
            },
        ],
    }
}

fn changed_schema() -> CanonicalSchemaV1 {
    CanonicalSchemaV1 {
        fields: vec![SchemaFieldV1 {
            name: "event_id".to_owned(),
            data_type: DataTypeV1::UInt64,
            nullable: false,
        }],
    }
}

fn key(value: &str) -> ObjectKey {
    ObjectKey::parse(value).expect("test object key is valid")
}

#[tokio::test]
async fn missing_table_is_distinct_from_incomplete_initialization() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));

    assert!(matches!(
        open_table(storage).await,
        Err(TableError::TableNotFound)
    ));
}

#[tokio::test]
async fn create_open_and_repeat_are_idempotent_at_version_zero() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));

    let created = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let reopened = open_table(Arc::clone(&storage)).await.expect("open table");
    let repeated = create_table(Arc::clone(&storage), schema())
        .await
        .expect("repeat create");

    assert_eq!(created.version(), 0);
    assert_eq!(created.table_id(), reopened.table_id());
    assert_eq!(created.commit_id(), reopened.commit_id());
    assert_eq!(created.table_id(), repeated.table_id());
    assert_eq!(created.commit_id(), repeated.commit_id());
    assert_eq!(created.descriptor().files, Vec::new());
    assert_eq!(created.descriptor().schema, schema());

    let manifest_count = fs::read_dir(directory.path().join("manifests"))
        .expect("manifest directory")
        .count();
    assert_eq!(
        manifest_count, 1,
        "idempotent create must not add a manifest"
    );
}

#[tokio::test]
async fn a_different_schema_never_changes_an_existing_table() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let original = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");

    assert!(matches!(
        create_table(Arc::clone(&storage), changed_schema()).await,
        Err(TableError::SchemaMismatch)
    ));
    let reopened = open_table(storage)
        .await
        .expect("original remains readable");
    assert_eq!(reopened.table_id(), original.table_id());
    assert_eq!(reopened.commit_id(), original.commit_id());
    assert_eq!(reopened.metadata().schema, schema());
}

#[tokio::test]
async fn interrupted_initialization_retains_identity_and_can_be_completed() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table_id = Uuid::new_v4();
    let table = TableMetadata {
        format_version: FORMAT_VERSION,
        table_id,
        schema_fingerprint: schema().fingerprint(),
        schema: schema(),
    };
    storage
        .put_immutable(
            &key("table.json"),
            Bytes::from(serde_json::to_vec(&table).expect("encode table")),
        )
        .await
        .expect("write table identity");

    assert!(matches!(
        open_table(Arc::clone(&storage)).await,
        Err(TableError::InitializationIncomplete { table_id: found }) if found == table_id
    ));
    let recovered = create_table(Arc::clone(&storage), schema())
        .await
        .expect("complete initialization");
    assert_eq!(recovered.table_id(), table_id);
    assert_eq!(recovered.version(), 0);
    assert!(directory.path().join("HEAD").is_file());
}

#[tokio::test]
async fn schema_mismatch_does_not_complete_interrupted_initialization() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table = TableMetadata {
        format_version: FORMAT_VERSION,
        table_id: Uuid::new_v4(),
        schema_fingerprint: schema().fingerprint(),
        schema: schema(),
    };
    storage
        .put_immutable(
            &key("table.json"),
            Bytes::from(serde_json::to_vec(&table).expect("encode table")),
        )
        .await
        .expect("write table identity");

    assert!(matches!(
        create_table(Arc::clone(&storage), changed_schema()).await,
        Err(TableError::SchemaMismatch)
    ));
    assert!(!directory.path().join("HEAD").exists());
}

#[tokio::test]
async fn a_missing_committed_manifest_is_reported_as_corruption() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let created = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let manifest_key = created.head().manifest_key.clone();
    fs::remove_file(directory.path().join(manifest_key.as_str())).expect("remove manifest");

    assert!(matches!(
        open_table(storage).await,
        Err(TableError::CorruptMetadata { key, .. }) if key == manifest_key
    ));
}

#[tokio::test]
async fn a_corrupt_persisted_schema_fingerprint_is_not_a_request_mismatch() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let mut table = TableMetadata {
        format_version: FORMAT_VERSION,
        table_id: Uuid::new_v4(),
        schema_fingerprint: schema().fingerprint(),
        schema: schema(),
    };
    table.schema_fingerprint = changed_schema().fingerprint();
    storage
        .put_immutable(
            &key("table.json"),
            Bytes::from(serde_json::to_vec(&table).expect("encode table")),
        )
        .await
        .expect("write corrupt table metadata");

    assert!(matches!(
        open_table(storage).await,
        Err(TableError::CorruptMetadata { key, .. }) if key.as_str() == "table.json"
    ));
}

#[derive(Debug)]
struct RacingStorage {
    inner: LocalStorage,
    table_write_barrier: Barrier,
}

#[async_trait]
impl Storage for RacingStorage {
    async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
        self.inner.get_with_token(key).await
    }

    async fn put_immutable(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        if key.as_str() == "table.json" {
            self.table_write_barrier.wait().await;
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

#[tokio::test]
async fn concurrent_creators_converge_on_one_identity_and_commit() {
    for _ in 0..20 {
        let directory = tempdir().expect("temporary directory");
        let storage = Arc::new(RacingStorage {
            inner: LocalStorage::open(directory.path()).expect("open local storage"),
            table_write_barrier: Barrier::new(2),
        });

        let (left, right) = tokio::join!(
            create_table(Arc::clone(&storage), schema()),
            create_table(Arc::clone(&storage), schema())
        );
        let left = left.expect("left creator converges");
        let right = right.expect("right creator converges");
        assert_eq!(left.table_id(), right.table_id());
        assert_eq!(left.commit_id(), right.commit_id());
        assert_eq!(left.version(), 0);
        assert_eq!(right.version(), 0);
    }
}

#[derive(Debug, Clone, Copy)]
enum LostResponseTarget {
    Table,
    Manifest,
    Head,
}

#[derive(Debug)]
struct LostResponseStorage {
    inner: LocalStorage,
    target: LostResponseTarget,
    lost: AtomicBool,
}

#[derive(Debug)]
struct PreWriteUnknownStorage {
    inner: LocalStorage,
    target: LostResponseTarget,
}

#[async_trait]
impl Storage for PreWriteUnknownStorage {
    async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
        self.inner.get_with_token(key).await
    }

    async fn put_immutable(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        let targeted = match self.target {
            LostResponseTarget::Table => key.as_str() == "table.json",
            LostResponseTarget::Manifest => key.as_str().starts_with("manifests/"),
            LostResponseTarget::Head => false,
        };
        if targeted {
            Err(LostResponseStorage::lost_response(
                StorageOperation::PutImmutable,
                key,
            ))
        } else {
            self.inner.put_immutable(key, bytes).await
        }
    }

    async fn create_if_absent(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        if matches!(self.target, LostResponseTarget::Head) && key.as_str() == "HEAD" {
            Err(LostResponseStorage::lost_response(
                StorageOperation::CreateIfAbsent,
                key,
            ))
        } else {
            self.inner.create_if_absent(key, bytes).await
        }
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

impl LostResponseStorage {
    fn should_lose(&self, key: &ObjectKey, target: LostResponseTarget) -> bool {
        let matches = match target {
            LostResponseTarget::Table => key.as_str() == "table.json",
            LostResponseTarget::Manifest => key.as_str().starts_with("manifests/"),
            LostResponseTarget::Head => key.as_str() == "HEAD",
        };
        matches && !self.lost.swap(true, Ordering::SeqCst)
    }

    fn lost_response(operation: StorageOperation, key: &ObjectKey) -> StorageError {
        StorageError::UnknownWriteOutcome {
            operation,
            key: key.clone(),
            message: "simulated response loss after durable write".to_owned(),
        }
    }
}

#[async_trait]
impl Storage for LostResponseStorage {
    async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
        self.inner.get_with_token(key).await
    }

    async fn put_immutable(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        let result = self.inner.put_immutable(key, bytes).await;
        if result.is_ok() && self.should_lose(key, self.target) {
            Err(Self::lost_response(StorageOperation::PutImmutable, key))
        } else {
            result
        }
    }

    async fn create_if_absent(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        let result = self.inner.create_if_absent(key, bytes).await;
        if result.is_ok() && self.should_lose(key, self.target) {
            Err(Self::lost_response(StorageOperation::CreateIfAbsent, key))
        } else {
            result
        }
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

#[tokio::test]
async fn response_loss_after_each_initialization_write_is_reconciled() {
    for target in [
        LostResponseTarget::Table,
        LostResponseTarget::Manifest,
        LostResponseTarget::Head,
    ] {
        let directory = tempdir().expect("temporary directory");
        let storage = Arc::new(LostResponseStorage {
            inner: LocalStorage::open(directory.path()).expect("open local storage"),
            target,
            lost: AtomicBool::new(false),
        });

        let created = create_table(Arc::clone(&storage), schema())
            .await
            .expect("response loss reconciles");
        assert_eq!(created.version(), 0);
        assert!(storage.lost.load(Ordering::SeqCst));
        let reopened = open_table(storage).await.expect("published table opens");
        assert_eq!(created.commit_id(), reopened.commit_id());
    }
}

#[tokio::test]
async fn an_unreconciled_write_outcome_is_never_reported_as_success() {
    for target in [
        LostResponseTarget::Table,
        LostResponseTarget::Manifest,
        LostResponseTarget::Head,
    ] {
        let directory = tempdir().expect("temporary directory");
        let storage = Arc::new(PreWriteUnknownStorage {
            inner: LocalStorage::open(directory.path()).expect("open local storage"),
            target,
        });

        assert!(matches!(
            create_table(storage, schema()).await,
            Err(TableError::UnknownCommitStatus { .. })
        ));
    }
}
