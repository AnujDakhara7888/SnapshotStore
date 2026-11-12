#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
use snapshotstore::table::{AppendReceipt, append_batch, create_table, open_table};
use tempfile::tempdir;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteBoundary {
    Data,
    Manifest,
    Head,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultTiming {
    Before,
    After,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOutcome {
    Unavailable,
    UnknownWriteOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultPlan {
    pub boundary: WriteBoundary,
    pub timing: FaultTiming,
    pub outcome: FaultOutcome,
    pub fail_head_reconciliation: bool,
}

impl FaultPlan {
    pub fn unavailable_before(boundary: WriteBoundary) -> Self {
        Self {
            boundary,
            timing: FaultTiming::Before,
            outcome: FaultOutcome::Unavailable,
            fail_head_reconciliation: false,
        }
    }

    pub fn unknown_before(boundary: WriteBoundary) -> Self {
        Self {
            boundary,
            timing: FaultTiming::Before,
            outcome: FaultOutcome::UnknownWriteOutcome,
            fail_head_reconciliation: false,
        }
    }

    pub fn unknown_after(boundary: WriteBoundary) -> Self {
        Self {
            boundary,
            timing: FaultTiming::After,
            outcome: FaultOutcome::UnknownWriteOutcome,
            fail_head_reconciliation: false,
        }
    }

    pub fn unknown_after_head_with_failed_reconciliation() -> Self {
        Self {
            boundary: WriteBoundary::Head,
            timing: FaultTiming::After,
            outcome: FaultOutcome::UnknownWriteOutcome,
            fail_head_reconciliation: true,
        }
    }
}

#[derive(Debug)]
pub struct FaultStorage {
    inner: LocalStorage,
    plan: Mutex<Option<FaultPlan>>,
    triggered: AtomicUsize,
    head_replacements: AtomicUsize,
    fail_next_read: AtomicBool,
    fail_next_head_read: AtomicBool,
}

impl FaultStorage {
    pub fn open(path: &std::path::Path) -> Self {
        Self {
            inner: LocalStorage::open(path).expect("open local fault-test storage"),
            plan: Mutex::new(None),
            triggered: AtomicUsize::new(0),
            head_replacements: AtomicUsize::new(0),
            fail_next_read: AtomicBool::new(false),
            fail_next_head_read: AtomicBool::new(false),
        }
    }

    pub fn arm(&self, plan: FaultPlan) {
        assert!(
            plan.timing == FaultTiming::Before || plan.outcome == FaultOutcome::UnknownWriteOutcome,
            "an applied write cannot be reported as definitely unavailable"
        );
        let mut current = self.plan.lock().expect("fault plan lock poisoned");
        assert!(current.is_none(), "only one fault may be armed at a time");
        *current = Some(plan);
    }

    pub fn triggered(&self) -> usize {
        self.triggered.load(Ordering::SeqCst)
    }

    pub fn head_replacements(&self) -> usize {
        self.head_replacements.load(Ordering::SeqCst)
    }

    pub fn fail_next_read(&self) {
        self.fail_next_read.store(true, Ordering::SeqCst);
    }

    fn take_plan(&self, boundary: WriteBoundary) -> Option<FaultPlan> {
        let mut plan = self.plan.lock().expect("fault plan lock poisoned");
        if plan.as_ref().is_some_and(|plan| plan.boundary == boundary) {
            self.triggered.fetch_add(1, Ordering::SeqCst);
            plan.take()
        } else {
            None
        }
    }

    fn injected_error(
        plan: FaultPlan,
        operation: StorageOperation,
        key: &ObjectKey,
    ) -> StorageError {
        match plan.outcome {
            FaultOutcome::Unavailable => StorageError::Unavailable {
                operation,
                message: format!("injected storage failure for {key}"),
            },
            FaultOutcome::UnknownWriteOutcome => StorageError::UnknownWriteOutcome {
                operation,
                key: key.clone(),
                message: format!("injected unknown outcome for {key}"),
            },
        }
    }

    fn boundary_for_immutable(key: &ObjectKey) -> Option<WriteBoundary> {
        if key.as_str().starts_with("data/") {
            Some(WriteBoundary::Data)
        } else if key.as_str().starts_with("manifests/") {
            Some(WriteBoundary::Manifest)
        } else {
            None
        }
    }
}

#[async_trait]
impl Storage for FaultStorage {
    async fn get_with_token(&self, key: &ObjectKey) -> Result<StoredObject, StorageError> {
        if self.fail_next_read.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Unavailable {
                operation: StorageOperation::GetWithToken,
                message: format!("injected transient read failure for {key}"),
            });
        }
        if key.as_str() == "HEAD" && self.fail_next_head_read.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Unavailable {
                operation: StorageOperation::GetWithToken,
                message: "injected reconciliation read failure".to_owned(),
            });
        }
        self.inner.get_with_token(key).await
    }

    async fn put_immutable(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<VersionToken, StorageError> {
        let Some(boundary) = Self::boundary_for_immutable(key) else {
            return self.inner.put_immutable(key, bytes).await;
        };
        let plan = self.take_plan(boundary);
        if let Some(plan) = plan.filter(|plan| plan.timing == FaultTiming::Before) {
            return Err(Self::injected_error(
                plan,
                StorageOperation::PutImmutable,
                key,
            ));
        }
        let result = self.inner.put_immutable(key, bytes).await;
        if let Some(plan) = plan.filter(|plan| plan.timing == FaultTiming::After) {
            result?;
            return Err(Self::injected_error(
                plan,
                StorageOperation::PutImmutable,
                key,
            ));
        }
        result
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
        self.head_replacements.fetch_add(1, Ordering::SeqCst);
        let plan = self.take_plan(WriteBoundary::Head);
        if let Some(plan) = plan.filter(|plan| plan.timing == FaultTiming::Before) {
            return Err(Self::injected_error(
                plan,
                StorageOperation::ReplaceIfTokenMatches,
                key,
            ));
        }
        let result = self
            .inner
            .replace_if_token_matches(key, expected, bytes)
            .await;
        if let Some(plan) = plan.filter(|plan| plan.timing == FaultTiming::After) {
            result?;
            if plan.fail_head_reconciliation {
                self.fail_next_head_read.store(true, Ordering::SeqCst);
            }
            return Err(Self::injected_error(
                plan,
                StorageOperation::ReplaceIfTokenMatches,
                key,
            ));
        }
        result
    }
}

#[derive(Debug)]
struct ModelBatch {
    batch_id: String,
    values: Vec<u64>,
    file_count: usize,
    first_commit_id: Option<Uuid>,
}

#[derive(Debug, Default)]
pub struct AppendReferenceModel {
    batches: BTreeMap<u64, ModelBatch>,
}

impl AppendReferenceModel {
    pub fn apply_receipt(&mut self, batch_id: &str, values: &[u64], receipt: &AppendReceipt) {
        self.apply_receipt_with_file_count(batch_id, values, 1, receipt);
    }

    pub fn apply_receipt_with_file_count(
        &mut self,
        batch_id: &str,
        values: &[u64],
        file_count: usize,
        receipt: &AppendReceipt,
    ) {
        assert!(file_count > 0, "a committed batch has at least one file");
        assert_eq!(receipt.batch_id.as_str(), batch_id);
        assert_eq!(receipt.added_rows, values.len() as u64);
        let committed_version = self
            .batches
            .iter()
            .find_map(|(version, batch)| (batch.batch_id == batch_id).then_some(*version));
        if let Some(committed_version) = committed_version {
            let committed = self
                .batches
                .get_mut(&committed_version)
                .expect("observed model version remains present");
            assert!(receipt.replayed, "an existing model batch must be a replay");
            assert_eq!(committed.values, values);
            assert_eq!(committed.file_count, file_count);
            assert_eq!(receipt.first_version, committed_version);
            if let Some(first_commit_id) = committed.first_commit_id {
                assert_eq!(receipt.first_commit_id, first_commit_id);
            } else {
                committed.first_commit_id = Some(receipt.first_commit_id);
            }
            return;
        }

        assert!(!receipt.replayed, "a new model batch cannot be a replay");
        assert!(receipt.first_version > 0);
        assert!(
            self.batches
                .insert(
                    receipt.first_version,
                    ModelBatch {
                        batch_id: batch_id.to_owned(),
                        values: values.to_vec(),
                        file_count,
                        first_commit_id: Some(receipt.first_commit_id),
                    },
                )
                .is_none(),
            "only one model batch may occupy a committed version"
        );
    }

    pub fn apply_known_commit_without_receipt(&mut self, batch_id: &str, values: &[u64]) {
        assert!(
            self.batches
                .values()
                .all(|batch| batch.batch_id != batch_id),
            "the reference model never commits one batch ID twice"
        );
        let version = self.batches.len() as u64 + 1;
        self.batches.insert(
            version,
            ModelBatch {
                batch_id: batch_id.to_owned(),
                values: values.to_vec(),
                file_count: 1,
                first_commit_id: None,
            },
        );
    }

    pub async fn assert_matches<S: Storage + ?Sized>(&self, storage: Arc<S>) {
        let table = open_table(storage).await.expect("open committed table");
        assert_eq!(table.version(), self.batches.len() as u64);
        assert_eq!(
            table.manifest().files.len(),
            self.batches
                .values()
                .map(|batch| batch.file_count)
                .sum::<usize>()
        );
        assert_eq!(table.manifest().committed_batches.len(), self.batches.len());
        assert_eq!(
            self.batches.keys().copied().collect::<Vec<_>>(),
            (1..=self.batches.len() as u64).collect::<Vec<_>>(),
            "model commit versions must be contiguous"
        );

        let expected_ids = self
            .batches
            .values()
            .map(|batch| batch.batch_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(expected_ids.len(), self.batches.len());
        let committed_ids = table
            .manifest()
            .committed_batches
            .keys()
            .map(BatchId::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(committed_ids, expected_ids);

        for (version, expected) in &self.batches {
            let batch_id = BatchId::parse(&expected.batch_id).expect("model batch ID is valid");
            let committed = table
                .manifest()
                .committed_batches
                .get(&batch_id)
                .expect("model batch is committed");
            assert_eq!(committed.first_version, *version);
            assert_eq!(committed.added_rows, expected.values.len() as u64);
            if let Some(first_commit_id) = expected.first_commit_id {
                assert_eq!(committed.first_commit_id, first_commit_id);
            }
        }

        let expected_values = self
            .batches
            .values()
            .flat_map(|batch| batch.values.iter().copied())
            .collect::<Vec<_>>();
        let mut actual_values = Vec::new();
        for batch in table.read_batches().await.expect("read committed rows") {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("reference fixture has one UInt64 column");
            actual_values.extend(values.values().iter().copied());
        }
        assert_eq!(actual_values, expected_values);
        assert_eq!(table.manifest().row_count, expected_values.len() as u64);
        assert_eq!(
            table
                .history()
                .await
                .expect("read committed history")
                .into_iter()
                .map(|snapshot| snapshot.version)
                .collect::<Vec<_>>(),
            (0..=self.batches.len() as u64).collect::<Vec<_>>()
        );
    }
}

#[derive(Debug)]
struct SeededGenerator(u64);

impl SeededGenerator {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

fn seeded_schema() -> CanonicalSchemaV1 {
    CanonicalSchemaV1 {
        fields: vec![SchemaFieldV1 {
            name: "value".to_owned(),
            data_type: DataTypeV1::UInt64,
            nullable: false,
        }],
    }
}

fn seeded_batch(values: &[u64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(values.to_vec())) as ArrayRef],
    )
    .expect("seeded fixture matches schema")
}

fn seeded_batch_id(value: &str) -> BatchId {
    BatchId::parse(value).expect("seeded batch ID is valid")
}

pub async fn run_seeded_history(seed: u64, steps: u64) {
    eprintln!("S30 seed {seed}: {steps} operations");
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(FaultStorage::open(directory.path()));
    create_table(Arc::clone(&storage), seeded_schema())
        .await
        .expect("create seeded table");
    let mut model = AppendReferenceModel::default();
    let mut committed: Vec<(String, Vec<u64>, AppendReceipt)> = Vec::new();
    let mut generator = SeededGenerator(seed);

    for step in 0..steps {
        let mut action = generator.next() % 7;
        if committed.is_empty() && matches!(action, 1 | 2) {
            action = 0;
        }
        let table = open_table(Arc::clone(&storage))
            .await
            .expect("open seeded step");
        match action {
            0 => {
                let id = format!("seed-{seed}-step-{step}");
                let values = vec![seed, step, generator.next()];
                let result = append_batch(&table, seeded_batch_id(&id), &seeded_batch(&values))
                    .await
                    .expect("seeded append succeeds");
                model.apply_receipt(&id, &values, &result.receipt);
                committed.push((id, values, result.receipt));
            }
            1 => {
                let index = (generator.next() as usize) % committed.len();
                let (id, values, _) = &committed[index];
                let result = append_batch(&table, seeded_batch_id(id), &seeded_batch(values))
                    .await
                    .expect("seeded replay succeeds");
                assert!(result.receipt.replayed);
                model.apply_receipt(id, values, &result.receipt);
            }
            2 => {
                let index = (generator.next() as usize) % committed.len();
                let (id, values, _) = &committed[index];
                let changed = [values[0].wrapping_add(1), values[1]];
                assert!(matches!(
                    append_batch(&table, seeded_batch_id(id), &seeded_batch(&changed)).await,
                    Err(TableError::BatchIdReuse { .. })
                ));
            }
            3 => {
                storage.arm(FaultPlan::unavailable_before(WriteBoundary::Data));
                let id = format!("failed-data-{seed}-{step}");
                assert!(matches!(
                    append_batch(&table, seeded_batch_id(&id), &seeded_batch(&[step])).await,
                    Err(TableError::StorageUnavailable { .. })
                ));
            }
            4 => {
                storage.arm(FaultPlan::unavailable_before(WriteBoundary::Head));
                let id = format!("failed-head-{seed}-{step}");
                assert!(matches!(
                    append_batch(&table, seeded_batch_id(&id), &seeded_batch(&[step])).await,
                    Err(TableError::StorageUnavailable { .. })
                ));
            }
            5 => {
                storage.arm(FaultPlan::unknown_after(WriteBoundary::Head));
                let id = format!("lost-response-{seed}-{step}");
                let values = vec![seed, step];
                let result = append_batch(&table, seeded_batch_id(&id), &seeded_batch(&values))
                    .await
                    .expect("applied unknown HEAD reconciles");
                model.apply_receipt(&id, &values, &result.receipt);
                committed.push((id, values, result.receipt));
            }
            6 => {
                storage.arm(FaultPlan::unknown_after_head_with_failed_reconciliation());
                let id = format!("unknown-commit-{seed}-{step}");
                let values = vec![seed, step, 6];
                assert!(matches!(
                    append_batch(&table, seeded_batch_id(&id), &seeded_batch(&values)).await,
                    Err(TableError::UnknownCommitStatus { .. })
                ));
                model.apply_known_commit_without_receipt(&id, &values);
                model.assert_matches(Arc::clone(&storage)).await;
                let reopened = open_table(Arc::clone(&storage))
                    .await
                    .expect("reopen unknown commit");
                let replay = append_batch(&reopened, seeded_batch_id(&id), &seeded_batch(&values))
                    .await
                    .expect("replay discovers unknown commit");
                assert!(replay.receipt.replayed);
                model.apply_receipt(&id, &values, &replay.receipt);
                committed.push((id, values, replay.receipt));
            }
            _ => unreachable!("action is modulo seven"),
        }
        model.assert_matches(Arc::clone(&storage)).await;
    }
}
