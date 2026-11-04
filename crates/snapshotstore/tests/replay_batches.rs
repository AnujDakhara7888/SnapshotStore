use std::fs;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use snapshotstore::errors::TableError;
use snapshotstore::fingerprint::fingerprint_batches;
use snapshotstore::metadata::{BatchId, CanonicalSchemaV1, DataTypeV1, SchemaFieldV1};
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::table::{append_batch, append_batches, create_table, open_table};
use tempfile::tempdir;

fn schema() -> CanonicalSchemaV1 {
    CanonicalSchemaV1 {
        fields: vec![
            SchemaFieldV1 {
                name: "active".to_owned(),
                data_type: DataTypeV1::Boolean,
                nullable: false,
            },
            SchemaFieldV1 {
                name: "delta".to_owned(),
                data_type: DataTypeV1::Int64,
                nullable: false,
            },
            SchemaFieldV1 {
                name: "event_id".to_owned(),
                data_type: DataTypeV1::UInt64,
                nullable: false,
            },
            SchemaFieldV1 {
                name: "amount".to_owned(),
                data_type: DataTypeV1::Float64,
                nullable: true,
            },
            SchemaFieldV1 {
                name: "message".to_owned(),
                data_type: DataTypeV1::Utf8,
                nullable: true,
            },
        ],
    }
}

fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("active", DataType::Boolean, false),
        Field::new("delta", DataType::Int64, false),
        Field::new("event_id", DataType::UInt64, false),
        Field::new("amount", DataType::Float64, true),
        Field::new("message", DataType::Utf8, true),
    ]))
}

fn logical_batch() -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
            Arc::new(Int64Array::from(vec![-5, 0, 9])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![101, 102, 103])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(12.5), None, Some(-4.25)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alpha"), Some(""), None])) as ArrayRef,
        ],
    )
    .expect("fixture matches schema")
}

fn changed_batch() -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            Arc::new(Int64Array::from(vec![-5])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![999])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(12.5)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alpha")])) as ArrayRef,
        ],
    )
    .expect("fixture matches schema")
}

fn batch_id(value: &str) -> BatchId {
    BatchId::parse(value).expect("test batch ID is valid")
}

fn object_count(root: &std::path::Path, directory: &str) -> usize {
    let directory = root.join(directory);
    if !directory.exists() {
        return 0;
    }
    walkdir(&directory)
}

fn walkdir(path: &std::path::Path) -> usize {
    fs::read_dir(path)
        .expect("read object directory")
        .map(|entry| entry.expect("directory entry").path())
        .map(|path| if path.is_dir() { walkdir(&path) } else { 1 })
        .sum()
}

#[tokio::test]
async fn matching_replay_ignores_chunk_and_file_boundaries() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let whole = logical_batch();
    let empty = RecordBatch::new_empty(arrow_schema());
    let chunks = [whole.slice(0, 1), empty, whole.slice(1, 2)];

    let first = append_batches(&table, batch_id("batch-a"), &chunks)
        .await
        .expect("first append");
    assert!(!first.receipt.replayed);
    assert_eq!(first.receipt.first_version, 1);
    assert_eq!(first.receipt.added_rows, 3);
    assert_eq!(first.table.manifest().files.len(), 2);
    let first_commit = first.receipt.first_commit_id;
    let fingerprint = first.receipt.fingerprint.clone();
    let data_objects = object_count(directory.path(), "data");
    let manifest_objects = object_count(directory.path(), "manifests");

    let replay = append_batch(&first.table, batch_id("batch-a"), &whole)
        .await
        .expect("matching replay");
    assert!(replay.receipt.replayed);
    assert_eq!(replay.receipt.first_commit_id, first_commit);
    assert_eq!(replay.receipt.first_version, 1);
    assert_eq!(replay.receipt.fingerprint, fingerprint);
    assert_eq!(replay.table.version(), 1);
    assert_eq!(object_count(directory.path(), "data"), data_objects);
    assert_eq!(
        object_count(directory.path(), "manifests"),
        manifest_objects
    );

    let second = append_batch(&replay.table, batch_id("batch-b"), &whole)
        .await
        .expect("same rows under a different ID are intentional");
    assert_eq!(second.table.version(), 2);
    assert_eq!(second.table.manifest().row_count, 6);

    let latest = open_table(Arc::clone(&storage)).await.expect("open latest");
    let late_replay = append_batch(&latest, batch_id("batch-a"), &whole)
        .await
        .expect("replay after a newer commit");
    assert!(late_replay.receipt.replayed);
    assert_eq!(late_replay.receipt.first_commit_id, first_commit);
    assert_eq!(late_replay.receipt.first_version, 1);
    assert_eq!(late_replay.table.version(), 2);

    assert!(matches!(
        append_batch(&late_replay.table, batch_id("batch-a"), &changed_batch()).await,
        Err(TableError::BatchIdReuse {
            batch_id,
            committed_fingerprint,
            supplied_fingerprint,
        }) if batch_id == "batch-a" && committed_fingerprint != supplied_fingerprint
    ));
    assert_eq!(
        open_table(storage)
            .await
            .expect("table unchanged")
            .version(),
        2
    );
}

#[test]
fn canonical_encoding_is_stable_and_chunk_independent() {
    let whole = logical_batch();
    let chunks = [whole.slice(0, 1), whole.slice(1, 2)];
    let whole_fingerprint =
        fingerprint_batches(&schema(), std::slice::from_ref(&whole)).expect("fingerprint whole");
    let chunked_fingerprint = fingerprint_batches(&schema(), &chunks).expect("fingerprint chunks");
    assert_eq!(whole_fingerprint, chunked_fingerprint);
    assert_eq!(
        whole_fingerprint.as_str(),
        "abccab4e4c7b1c5a7bb9d1595a5194277797cc3ffb222ee7a4d95fc00a7598d6"
    );
}

#[tokio::test]
async fn signed_zero_is_normalized_and_non_finite_values_are_rejected() {
    let logical_schema = CanonicalSchemaV1 {
        fields: vec![SchemaFieldV1 {
            name: "value".to_owned(),
            data_type: DataTypeV1::Float64,
            nullable: false,
        }],
    };
    let physical_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let batch = |value| {
        RecordBatch::try_new(
            Arc::clone(&physical_schema),
            vec![Arc::new(Float64Array::from(vec![value])) as ArrayRef],
        )
        .expect("float fixture")
    };
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table = create_table(Arc::clone(&storage), logical_schema)
        .await
        .expect("create float table");

    let first = append_batch(&table, batch_id("zero"), &batch(-0.0))
        .await
        .expect("append negative zero");
    let replay = append_batch(&first.table, batch_id("zero"), &batch(0.0))
        .await
        .expect("positive zero is the same logical value");
    assert!(replay.receipt.replayed);
    assert_eq!(replay.table.version(), 1);

    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(matches!(
            append_batch(
                &replay.table,
                batch_id(&format!("nonfinite-{value}")),
                &batch(value)
            )
            .await,
            Err(TableError::InvalidBatch { .. })
        ));
    }
    assert_eq!(
        open_table(storage)
            .await
            .expect("table unchanged")
            .version(),
        1
    );
}
