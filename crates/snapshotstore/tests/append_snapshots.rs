use std::fs::{self, File};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::basic::Compression;
use parquet::file::reader::{FileReader, SerializedFileReader};
use snapshotstore::errors::TableError;
use snapshotstore::metadata::{
    BatchId, CanonicalSchemaV1, DataTypeV1, OperationKind, SchemaFieldV1,
};
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::table::{append_batch, create_table, open_table, open_table_at_version};
use tempfile::tempdir;

fn table_schema() -> CanonicalSchemaV1 {
    CanonicalSchemaV1 {
        fields: vec![
            SchemaFieldV1 {
                name: "event_id".to_owned(),
                data_type: DataTypeV1::UInt64,
                nullable: false,
            },
            SchemaFieldV1 {
                name: "event_type".to_owned(),
                data_type: DataTypeV1::Utf8,
                nullable: false,
            },
            SchemaFieldV1 {
                name: "amount".to_owned(),
                data_type: DataTypeV1::Float64,
                nullable: true,
            },
        ],
    }
}

fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, true),
    ]))
}

fn batch_a() -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![101, 102])) as ArrayRef,
            Arc::new(StringArray::from(vec!["created", "charged"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(12.5), None])) as ArrayRef,
        ],
    )
    .expect("fixture matches schema")
}

fn batch_b() -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![103])) as ArrayRef,
            Arc::new(StringArray::from(vec!["refunded"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(-4.25)])) as ArrayRef,
        ],
    )
    .expect("fixture matches schema")
}

fn batch_id(value: &str) -> BatchId {
    BatchId::parse(value).expect("test batch ID is valid")
}

#[tokio::test]
async fn appends_publish_full_manifests_and_preserve_pinned_reads() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let version_0 = create_table(Arc::clone(&storage), table_schema())
        .await
        .expect("create table");

    let version_1 = append_batch(&version_0, batch_id("batch-a"), &batch_a())
        .await
        .expect("append batch A");
    assert_eq!(version_1.version(), 1);
    assert_eq!(version_1.manifest().row_count, 2);
    assert_eq!(version_1.manifest().files.len(), 1);
    assert_eq!(version_1.manifest().operation.kind, OperationKind::Append);
    assert_eq!(
        version_1.read_batches().await.expect("read v1"),
        vec![batch_a()]
    );
    let written = File::open(
        directory
            .path()
            .join(version_1.manifest().files[0].object_key.as_str()),
    )
    .expect("open committed Parquet file");
    let reader = SerializedFileReader::new(written).expect("read committed Parquet metadata");
    assert!(
        reader
            .metadata()
            .row_groups()
            .iter()
            .flat_map(|row_group| row_group.columns())
            .all(|column| matches!(column.compression(), Compression::ZSTD(_))),
        "production append files use Zstandard compression"
    );
    assert!(
        version_0
            .read_batches()
            .await
            .expect("read pinned v0")
            .is_empty()
    );

    let version_2 = append_batch(&version_1, batch_id("batch-b"), &batch_b())
        .await
        .expect("append batch B");
    assert_eq!(version_2.version(), 2);
    assert_eq!(version_2.manifest().row_count, 3);
    assert_eq!(version_2.manifest().files.len(), 2);
    assert_eq!(
        version_2.read_batches().await.expect("read v2"),
        vec![batch_a(), batch_b()]
    );
    assert_eq!(
        version_1.read_batches().await.expect("v1 remains pinned"),
        vec![batch_a()]
    );

    let reopened = open_table(Arc::clone(&storage)).await.expect("open latest");
    assert_eq!(reopened.version(), 2);
    assert_eq!(reopened.commit_id(), version_2.commit_id());
    assert_eq!(
        reopened.read_batches().await.expect("read reopened latest"),
        vec![batch_a(), batch_b()]
    );

    let history = reopened.history().await.expect("read history");
    assert_eq!(
        history
            .iter()
            .map(|snapshot| snapshot.version)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(history[0].files.len(), 0);
    assert_eq!(history[1].files.len(), 1);
    assert_eq!(history[2].files.len(), 2);

    let historical = open_table_at_version(Arc::clone(&storage), 1)
        .await
        .expect("open version 1");
    assert_eq!(
        historical.read_batches().await.expect("read historical"),
        vec![batch_a()]
    );
    assert!(matches!(
        append_batch(&historical, batch_id("not-allowed"), &batch_b()).await,
        Err(TableError::HistoricalSnapshotReadOnly)
    ));
    assert!(matches!(
        open_table_at_version(storage, 9).await,
        Err(TableError::VersionNotFound { version: 9 })
    ));
}

#[tokio::test]
async fn empty_and_wrong_schema_batches_do_not_advance_head() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table = create_table(Arc::clone(&storage), table_schema())
        .await
        .expect("create table");
    let empty = RecordBatch::new_empty(arrow_schema());
    assert!(matches!(
        append_batch(&table, batch_id("empty"), &empty).await,
        Err(TableError::EmptyBatch)
    ));

    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "event_id",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(vec![1])) as ArrayRef],
    )
    .expect("wrong-schema fixture is internally valid");
    assert!(matches!(
        append_batch(&table, batch_id("wrong"), &wrong).await,
        Err(TableError::SchemaMismatch)
    ));
    assert_eq!(
        open_table(storage)
            .await
            .expect("open unchanged table")
            .version(),
        0
    );
    assert!(!directory.path().join("data").exists());
}

#[tokio::test]
async fn stale_writer_rebases_without_losing_committed_data() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    create_table(Arc::clone(&storage), table_schema())
        .await
        .expect("create table");
    let left = open_table(Arc::clone(&storage))
        .await
        .expect("open left writer");
    let right = open_table(Arc::clone(&storage))
        .await
        .expect("open right writer");

    let winner = append_batch(&left, batch_id("winner"), &batch_a())
        .await
        .expect("first writer publishes");
    let rebased = append_batch(&right, batch_id("rebased"), &batch_b())
        .await
        .expect("stale writer rebases");
    assert_eq!(winner.table.version(), 1);
    assert_eq!(rebased.table.version(), 2);

    let visible = open_table(storage).await.expect("open committed state");
    assert_eq!(visible.version(), 2);
    assert_eq!(
        visible.read_batches().await.expect("read committed rows"),
        vec![batch_a(), batch_b()]
    );
    assert_eq!(visible.manifest().files.len(), 2);
    assert_eq!(visible.manifest().committed_batches.len(), 2);
}

#[tokio::test]
async fn missing_and_modified_committed_files_are_explicit_errors() {
    let missing_directory = tempdir().expect("temporary directory");
    let missing_storage = Arc::new(
        LocalStorage::open(missing_directory.path()).expect("open local storage for missing file"),
    );
    let missing_table = create_table(Arc::clone(&missing_storage), table_schema())
        .await
        .expect("create missing-file table");
    let missing_table = append_batch(&missing_table, batch_id("missing"), &batch_a())
        .await
        .expect("append missing-file fixture");
    let missing_key = missing_table.manifest().files[0].object_key.clone();
    fs::remove_file(missing_directory.path().join(missing_key.as_str()))
        .expect("remove committed fixture file");
    assert!(matches!(
        missing_table.read_batches().await,
        Err(TableError::MissingCommittedFile { key }) if key == missing_key
    ));

    let corrupt_directory = tempdir().expect("temporary directory");
    let corrupt_storage = Arc::new(
        LocalStorage::open(corrupt_directory.path()).expect("open local storage for corrupt file"),
    );
    let corrupt_table = create_table(Arc::clone(&corrupt_storage), table_schema())
        .await
        .expect("create corrupt-file table");
    let corrupt_table = append_batch(&corrupt_table, batch_id("corrupt"), &batch_a())
        .await
        .expect("append corrupt-file fixture");
    let corrupt_key = corrupt_table.manifest().files[0].object_key.clone();
    fs::write(
        corrupt_directory.path().join(corrupt_key.as_str()),
        b"not parquet",
    )
    .expect("replace committed fixture bytes");
    assert!(matches!(
        corrupt_table.read_batches().await,
        Err(TableError::CorruptData { key, .. }) if key == corrupt_key
    ));
}
