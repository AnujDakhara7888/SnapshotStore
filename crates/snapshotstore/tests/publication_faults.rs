mod support;

use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use snapshotstore::errors::TableError;
use snapshotstore::metadata::{BatchId, CanonicalSchemaV1, DataTypeV1, SchemaFieldV1};
use snapshotstore::storage::StorageOperation;
use snapshotstore::table::{Table, append_batch, create_table, open_table};
use tempfile::{TempDir, tempdir};

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
    BatchId::parse(value).expect("test batch ID is valid")
}

async fn fixture() -> (TempDir, Arc<FaultStorage>, Table<FaultStorage>) {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(FaultStorage::open(directory.path()));
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table before arming faults");
    (directory, storage, table)
}

#[tokio::test]
async fn definite_failures_before_every_write_boundary_leave_head_unchanged() {
    for (index, (boundary, operation)) in [
        (WriteBoundary::Data, StorageOperation::PutImmutable),
        (WriteBoundary::Manifest, StorageOperation::PutImmutable),
        (WriteBoundary::Head, StorageOperation::ReplaceIfTokenMatches),
    ]
    .into_iter()
    .enumerate()
    {
        let (_directory, storage, table) = fixture().await;
        storage.arm(FaultPlan::unavailable_before(boundary));
        let result = append_batch(
            &table,
            batch_id(&format!("unavailable-{index}")),
            &batch(&[index as u64]),
        )
        .await;

        assert!(matches!(
            result,
            Err(TableError::StorageUnavailable {
                operation: found,
                ..
            }) if found == operation
        ));
        assert_eq!(storage.triggered(), 1);
        AppendReferenceModel::default()
            .assert_matches(Arc::clone(&storage))
            .await;
    }
}

#[tokio::test]
async fn unknown_unapplied_data_and_manifest_writes_remain_invisible() {
    for (index, boundary) in [WriteBoundary::Data, WriteBoundary::Manifest]
        .into_iter()
        .enumerate()
    {
        let (_directory, storage, table) = fixture().await;
        storage.arm(FaultPlan::unknown_before(boundary));
        let result = append_batch(
            &table,
            batch_id(&format!("unknown-{index}")),
            &batch(&[10 + index as u64]),
        )
        .await;

        assert!(matches!(
            result,
            Err(TableError::UnknownCommitStatus {
                operation: StorageOperation::PutImmutable,
                ..
            })
        ));
        assert_eq!(storage.triggered(), 1);
        AppendReferenceModel::default()
            .assert_matches(Arc::clone(&storage))
            .await;
    }
}

#[tokio::test]
async fn unknown_unapplied_head_write_reconciles_and_retries_once() {
    let (_directory, storage, table) = fixture().await;
    storage.arm(FaultPlan::unknown_before(WriteBoundary::Head));
    let values = [20, 21];
    let result = append_batch(&table, batch_id("head-retry"), &batch(&values))
        .await
        .expect("an unapplied HEAD outcome rebases and retries");

    assert_eq!(storage.triggered(), 1);
    assert_eq!(storage.head_replacements(), 2);
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("head-retry", &values, &result.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn applied_unknown_outcomes_are_reconciled_at_every_write_boundary() {
    for (index, boundary) in [
        WriteBoundary::Data,
        WriteBoundary::Manifest,
        WriteBoundary::Head,
    ]
    .into_iter()
    .enumerate()
    {
        let (_directory, storage, table) = fixture().await;
        storage.arm(FaultPlan::unknown_after(boundary));
        let id = format!("applied-{index}");
        let values = [30 + index as u64, 40 + index as u64];
        let result = append_batch(&table, batch_id(&id), &batch(&values))
            .await
            .expect("durable bytes reconcile an applied unknown outcome");

        assert!(!result.receipt.replayed);
        assert_eq!(storage.triggered(), 1);
        let mut model = AppendReferenceModel::default();
        model.apply_receipt(&id, &values, &result.receipt);
        model.assert_matches(Arc::clone(&storage)).await;
    }
}

#[tokio::test]
async fn lost_head_response_with_failed_reconciliation_recovers_by_replay() {
    let (_directory, storage, table) = fixture().await;
    storage.arm(FaultPlan::unknown_after_head_with_failed_reconciliation());
    let values = [50, 51, 52];
    let first = append_batch(&table, batch_id("recoverable"), &batch(&values)).await;
    assert!(matches!(
        first,
        Err(TableError::UnknownCommitStatus {
            operation: StorageOperation::ReplaceIfTokenMatches,
            ..
        })
    ));

    let mut model = AppendReferenceModel::default();
    model.apply_known_commit_without_receipt("recoverable", &values);
    model.assert_matches(Arc::clone(&storage)).await;

    let reopened = open_table(Arc::clone(&storage))
        .await
        .expect("storage recovered");
    let replay = append_batch(&reopened, batch_id("recoverable"), &batch(&values))
        .await
        .expect("retry discovers the committed batch");
    assert!(replay.receipt.replayed);
    model.apply_receipt("recoverable", &values, &replay.receipt);
    model.assert_matches(storage).await;
}
