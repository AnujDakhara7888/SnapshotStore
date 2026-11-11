mod support;

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use snapshotstore::errors::TableError;
use snapshotstore::metadata::{BatchId, CanonicalSchemaV1, DataTypeV1, SchemaFieldV1};
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::storage::{ObjectKey, Storage, StorageError, StoredObject, VersionToken};
use snapshotstore::table::{
    AppendReceipt, append_batch, append_batches, create_table, open_table, open_table_at_version,
};
use tempfile::tempdir;

use support::{AppendReferenceModel, FaultPlan, FaultStorage, WriteBoundary};

const PROCESS_WORKER: &str = "SNAPSHOTSTORE_SCENARIO_WORKER";

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

fn count_files(path: &Path) -> usize {
    if !path.exists() {
        return 0;
    }
    fs::read_dir(path)
        .expect("read scenario directory")
        .map(|entry| entry.expect("read scenario entry").path())
        .map(|path| if path.is_dir() { count_files(&path) } else { 1 })
        .sum()
}

fn required_path(name: &str) -> PathBuf {
    env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("missing worker variable {name}"))
}

fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(Instant::now() < deadline, "timed out waiting for {path:?}");
        thread::sleep(Duration::from_millis(5));
    }
}

#[derive(Debug)]
struct PauseBeforeHeadStorage {
    inner: LocalStorage,
    ready: PathBuf,
    go: PathBuf,
    paused: AtomicBool,
}

#[async_trait]
impl Storage for PauseBeforeHeadStorage {
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
        if key.as_str() == "HEAD" && !self.paused.swap(true, Ordering::SeqCst) {
            fs::write(&self.ready, b"ready").expect("publish append-worker readiness");
            wait_for(&self.go);
        }
        self.inner
            .replace_if_token_matches(key, expected, bytes)
            .await
    }
}

fn spawn_append_worker(
    executable: &Path,
    root: &Path,
    ready: &Path,
    go: &Path,
    result: &Path,
    id: &str,
    value: u64,
) -> Child {
    Command::new(executable)
        .args([
            "--exact",
            "s29_separate_processes_race_and_killed_writer_stays_invisible",
            "--nocapture",
        ])
        .env(PROCESS_WORKER, "1")
        .env("SNAPSHOTSTORE_SCENARIO_ROOT", root)
        .env("SNAPSHOTSTORE_SCENARIO_READY", ready)
        .env("SNAPSHOTSTORE_SCENARIO_GO", go)
        .env("SNAPSHOTSTORE_SCENARIO_RESULT", result)
        .env("SNAPSHOTSTORE_SCENARIO_BATCH_ID", id)
        .env("SNAPSHOTSTORE_SCENARIO_VALUE", value.to_string())
        .spawn()
        .expect("spawn append worker")
}

fn assert_worker_success(output: Output) {
    assert!(
        output.status.success(),
        "worker failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn run_process_worker() {
    let storage = Arc::new(PauseBeforeHeadStorage {
        inner: LocalStorage::open(required_path("SNAPSHOTSTORE_SCENARIO_ROOT"))
            .expect("open worker storage"),
        ready: required_path("SNAPSHOTSTORE_SCENARIO_READY"),
        go: required_path("SNAPSHOTSTORE_SCENARIO_GO"),
        paused: AtomicBool::new(false),
    });
    let result_path = required_path("SNAPSHOTSTORE_SCENARIO_RESULT");
    let id = env::var("SNAPSHOTSTORE_SCENARIO_BATCH_ID").expect("worker batch ID");
    let value = env::var("SNAPSHOTSTORE_SCENARIO_VALUE")
        .expect("worker value")
        .parse::<u64>()
        .expect("worker value is u64");
    let table = open_table(Arc::clone(&storage))
        .await
        .expect("worker opens table");
    let result = append_batch(&table, batch_id(&id), &batch(&[value]))
        .await
        .expect("worker append resolves");
    fs::write(result_path, result.receipt.first_version.to_string()).expect("write worker result");
}

#[tokio::test]
async fn s21_required_committed_data_missing() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let values = [210, 211];
    let result = append_batch(&table, batch_id("missing"), &batch(&values))
        .await
        .expect("append fixture");
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("missing", &values, &result.receipt);
    model.assert_matches(Arc::clone(&storage)).await;
    let key = result.manifest().files[0].object_key.clone();
    fs::remove_file(directory.path().join(key.as_str())).expect("remove committed data");
    assert!(matches!(
        result.read_batches().await,
        Err(TableError::MissingCommittedFile { key: found }) if found == key
    ));
}

#[tokio::test]
async fn s22_committed_manifest_corrupt() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let values = [220];
    let result = append_batch(&table, batch_id("corrupt-manifest"), &batch(&values))
        .await
        .expect("append fixture");
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("corrupt-manifest", &values, &result.receipt);
    model.assert_matches(Arc::clone(&storage)).await;
    let key = result.head().manifest_key.clone();
    fs::write(directory.path().join(key.as_str()), b"{not-json")
        .expect("corrupt committed manifest");
    assert!(matches!(
        open_table(storage).await,
        Err(TableError::CorruptMetadata { key: found, .. }) if found == key
    ));
}

#[tokio::test]
async fn s23_incoming_schema_mismatch() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(arrow::array::Int64Array::from(vec![230_i64])) as ArrayRef],
    )
    .expect("wrong-schema batch is internally valid");
    assert!(matches!(
        append_batch(&table, batch_id("wrong-schema"), &wrong).await,
        Err(TableError::SchemaMismatch)
    ));
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}

#[tokio::test]
async fn s24_empty_incoming_batch() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    assert!(matches!(
        append_batch(
            &table,
            batch_id("empty"),
            &RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::UInt64,
                false,
            )])))
        )
        .await,
        Err(TableError::EmptyBatch)
    ));
    AppendReferenceModel::default()
        .assert_matches(storage)
        .await;
}

#[tokio::test]
async fn s25_one_batch_spans_several_files() {
    let success_directory = tempdir().expect("temporary directory");
    let success_storage = Arc::new(
        LocalStorage::open(success_directory.path()).expect("open successful local storage"),
    );
    let table = create_table(Arc::clone(&success_storage), schema())
        .await
        .expect("create table");
    let values = [250, 251, 252];
    let chunks = [batch(&values[..1]), batch(&values[1..])];
    let result = append_batches(&table, batch_id("multi-file"), &chunks)
        .await
        .expect("publish both files together");
    assert_eq!(result.manifest().files.len(), 2);
    let mut model = AppendReferenceModel::default();
    model.apply_receipt_with_file_count("multi-file", &values, 2, &result.receipt);
    model.assert_matches(success_storage).await;

    let failure_directory = tempdir().expect("temporary directory");
    let failure_storage = Arc::new(FaultStorage::open(failure_directory.path()));
    let table = create_table(Arc::clone(&failure_storage), schema())
        .await
        .expect("create fault table");
    failure_storage.arm(FaultPlan::unavailable_before(WriteBoundary::Head));
    assert!(matches!(
        append_batches(&table, batch_id("invisible-files"), &chunks).await,
        Err(TableError::StorageUnavailable { .. })
    ));
    assert_eq!(count_files(&failure_directory.path().join("data")), 2);
    AppendReferenceModel::default()
        .assert_matches(failure_storage)
        .await;
}

#[tokio::test]
async fn s26_read_old_version_after_several_appends() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let mut table = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let mut model = AppendReferenceModel::default();
    for (id, values) in [("a", vec![260]), ("b", vec![261, 262]), ("c", vec![263])] {
        let result = append_batch(&table, batch_id(id), &batch(&values))
            .await
            .expect("append history fixture");
        model.apply_receipt(id, &values, &result.receipt);
        table = result.table;
    }
    model.assert_matches(Arc::clone(&storage)).await;
    let version_1 = open_table_at_version(Arc::clone(&storage), 1)
        .await
        .expect("open version 1");
    let version_2 = open_table_at_version(storage, 2)
        .await
        .expect("open version 2");
    assert_eq!(
        values_from_batches(&version_1.read_batches().await.expect("read version 1")),
        vec![260]
    );
    assert_eq!(
        values_from_batches(&version_2.read_batches().await.expect("read version 2")),
        vec![260, 261, 262]
    );
}

#[tokio::test]
async fn s27_restart_with_orphan_data_and_manifests() {
    let directory = tempdir().expect("temporary directory");
    {
        let storage = Arc::new(FaultStorage::open(directory.path()));
        let table = create_table(Arc::clone(&storage), schema())
            .await
            .expect("create table");
        storage.arm(FaultPlan::unavailable_before(WriteBoundary::Head));
        assert!(matches!(
            append_batch(&table, batch_id("orphan"), &batch(&[270])).await,
            Err(TableError::StorageUnavailable { .. })
        ));
        assert_eq!(count_files(&directory.path().join("data")), 1);
        assert_eq!(count_files(&directory.path().join("manifests")), 2);
    }
    let restarted = Arc::new(LocalStorage::open(directory.path()).expect("restart storage"));
    AppendReferenceModel::default()
        .assert_matches(Arc::clone(&restarted))
        .await;
    assert_eq!(
        open_table(restarted)
            .await
            .expect("open committed HEAD only")
            .history()
            .await
            .expect("read history")
            .len(),
        1
    );
}

#[tokio::test]
async fn s28_several_pinned_readers_plus_writer() {
    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    let version_0 = create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let version_0_peer = open_table(Arc::clone(&storage))
        .await
        .expect("open v0 peer");
    let a_values = [280, 281];
    let a = append_batch(&version_0, batch_id("a"), &batch(&a_values))
        .await
        .expect("append A");
    let version_1_peer = open_table(Arc::clone(&storage))
        .await
        .expect("open v1 peer");
    let b_values = [282];
    let b = append_batch(&a.table, batch_id("b"), &batch(&b_values))
        .await
        .expect("append B");

    assert!(
        version_0
            .read_batches()
            .await
            .expect("read pinned v0")
            .is_empty()
    );
    assert!(
        version_0_peer
            .read_batches()
            .await
            .expect("read pinned v0 peer")
            .is_empty()
    );
    assert_eq!(
        values_from_batches(&version_1_peer.read_batches().await.expect("read pinned v1")),
        a_values
    );
    assert_eq!(
        values_from_batches(&b.read_batches().await.expect("read latest")),
        vec![280, 281, 282]
    );
    let mut model = AppendReferenceModel::default();
    model.apply_receipt("a", &a_values, &a.receipt);
    model.apply_receipt("b", &b_values, &b.receipt);
    model.assert_matches(storage).await;
}

#[tokio::test]
async fn s29_separate_processes_race_and_killed_writer_stays_invisible() {
    if env::var_os(PROCESS_WORKER).is_some() {
        run_process_worker().await;
        return;
    }

    let directory = tempdir().expect("temporary directory");
    let storage = Arc::new(LocalStorage::open(directory.path()).expect("open local storage"));
    create_table(Arc::clone(&storage), schema())
        .await
        .expect("create table");
    let executable = env::current_exe().expect("current test executable");

    let killed_ready = directory.path().join("killed-ready");
    let killed_go = directory.path().join("killed-go");
    let killed_result = directory.path().join("killed-result");
    let mut killed = spawn_append_worker(
        &executable,
        directory.path(),
        &killed_ready,
        &killed_go,
        &killed_result,
        "killed",
        290,
    );
    wait_for(&killed_ready);
    killed.kill().expect("kill paused writer");
    assert!(!killed.wait().expect("reap killed writer").success());
    assert!(!killed_result.exists());
    AppendReferenceModel::default()
        .assert_matches(Arc::clone(&storage))
        .await;

    let go = directory.path().join("race-go");
    let ready_a = directory.path().join("race-ready-a");
    let ready_b = directory.path().join("race-ready-b");
    let result_a = directory.path().join("race-result-a");
    let result_b = directory.path().join("race-result-b");
    let child_a = spawn_append_worker(
        &executable,
        directory.path(),
        &ready_a,
        &go,
        &result_a,
        "process-a",
        291,
    );
    let child_b = spawn_append_worker(
        &executable,
        directory.path(),
        &ready_b,
        &go,
        &result_b,
        "process-b",
        292,
    );
    wait_for(&ready_a);
    wait_for(&ready_b);
    fs::write(&go, b"go").expect("release process writers");
    assert_worker_success(child_a.wait_with_output().expect("wait for process A"));
    assert_worker_success(child_b.wait_with_output().expect("wait for process B"));

    let mut commits = [
        (
            fs::read_to_string(result_a)
                .expect("process A result")
                .parse::<u64>()
                .expect("process A version"),
            "process-a",
            vec![291],
        ),
        (
            fs::read_to_string(result_b)
                .expect("process B result")
                .parse::<u64>()
                .expect("process B version"),
            "process-b",
            vec![292],
        ),
    ];
    commits.sort_by_key(|commit| commit.0);
    assert_eq!([commits[0].0, commits[1].0], [1, 2]);
    let mut model = AppendReferenceModel::default();
    for (_, id, values) in commits {
        model.apply_known_commit_without_receipt(id, &values);
    }
    model.assert_matches(storage).await;
}

#[derive(Debug)]
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

#[tokio::test]
async fn s30_seeded_mixed_append_retry_crash_history() {
    for seed in [7_u64, 19, 41, 73] {
        let directory = tempdir().expect("temporary directory");
        let storage = Arc::new(FaultStorage::open(directory.path()));
        create_table(Arc::clone(&storage), schema())
            .await
            .expect("create seeded table");
        let mut model = AppendReferenceModel::default();
        let mut committed: Vec<(String, Vec<u64>, AppendReceipt)> = Vec::new();
        let mut rng = Lcg(seed);

        for step in 0..20_u64 {
            let mut action = rng.next() % 7;
            if committed.is_empty() && matches!(action, 1 | 2) {
                action = 0;
            }
            let table = open_table(Arc::clone(&storage))
                .await
                .expect("open seeded step");
            match action {
                0 => {
                    let id = format!("seed-{seed}-step-{step}");
                    let values = vec![seed, step, rng.next()];
                    let result = append_batch(&table, batch_id(&id), &batch(&values))
                        .await
                        .expect("seeded append succeeds");
                    model.apply_receipt(&id, &values, &result.receipt);
                    committed.push((id, values, result.receipt));
                }
                1 => {
                    let index = (rng.next() as usize) % committed.len();
                    let (id, values, _) = &committed[index];
                    let result = append_batch(&table, batch_id(id), &batch(values))
                        .await
                        .expect("seeded replay succeeds");
                    assert!(result.receipt.replayed);
                    model.apply_receipt(id, values, &result.receipt);
                }
                2 => {
                    let index = (rng.next() as usize) % committed.len();
                    let (id, values, _) = &committed[index];
                    let changed = [values[0].wrapping_add(1), values[1]];
                    assert!(matches!(
                        append_batch(&table, batch_id(id), &batch(&changed)).await,
                        Err(TableError::BatchIdReuse { .. })
                    ));
                }
                3 => {
                    storage.arm(FaultPlan::unavailable_before(WriteBoundary::Data));
                    let id = format!("failed-data-{seed}-{step}");
                    assert!(matches!(
                        append_batch(&table, batch_id(&id), &batch(&[step])).await,
                        Err(TableError::StorageUnavailable { .. })
                    ));
                }
                4 => {
                    storage.arm(FaultPlan::unavailable_before(WriteBoundary::Head));
                    let id = format!("failed-head-{seed}-{step}");
                    assert!(matches!(
                        append_batch(&table, batch_id(&id), &batch(&[step])).await,
                        Err(TableError::StorageUnavailable { .. })
                    ));
                }
                5 => {
                    storage.arm(FaultPlan::unknown_after(WriteBoundary::Head));
                    let id = format!("lost-response-{seed}-{step}");
                    let values = vec![seed, step];
                    let result = append_batch(&table, batch_id(&id), &batch(&values))
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
                        append_batch(&table, batch_id(&id), &batch(&values)).await,
                        Err(TableError::UnknownCommitStatus { .. })
                    ));
                    model.apply_known_commit_without_receipt(&id, &values);
                    model.assert_matches(Arc::clone(&storage)).await;
                    let reopened = open_table(Arc::clone(&storage))
                        .await
                        .expect("reopen unknown commit");
                    let replay = append_batch(&reopened, batch_id(&id), &batch(&values))
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
}
