#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::UInt64Array;
use async_trait::async_trait;
use bytes::Bytes;
use snapshotstore::metadata::BatchId;
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::storage::{
    ObjectKey, Storage, StorageError, StorageOperation, StoredObject, VersionToken,
};
use snapshotstore::table::{AppendReceipt, open_table};
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
    first_commit_id: Option<Uuid>,
}

#[derive(Debug, Default)]
pub struct AppendReferenceModel {
    batches: BTreeMap<u64, ModelBatch>,
}

impl AppendReferenceModel {
    pub fn apply_receipt(&mut self, batch_id: &str, values: &[u64], receipt: &AppendReceipt) {
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
                first_commit_id: None,
            },
        );
    }

    pub async fn assert_matches<S: Storage + ?Sized>(&self, storage: Arc<S>) {
        let table = open_table(storage).await.expect("open committed table");
        assert_eq!(table.version(), self.batches.len() as u64);
        assert_eq!(table.manifest().files.len(), self.batches.len());
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
