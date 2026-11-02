use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::storage::{ObjectKey, Storage, StorageError};
use tempfile::tempdir;

const WORKER_FLAG: &str = "SNAPSHOTSTORE_PROCESS_WORKER";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build current-thread runtime")
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

#[test]
fn subprocess_cas_worker() {
    if env::var_os(WORKER_FLAG).is_none() {
        return;
    }

    let root = required_path("SNAPSHOTSTORE_PROCESS_ROOT");
    let ready = required_path("SNAPSHOTSTORE_PROCESS_READY");
    let go = required_path("SNAPSHOTSTORE_PROCESS_GO");
    let result = required_path("SNAPSHOTSTORE_PROCESS_RESULT");
    let value = env::var("SNAPSHOTSTORE_PROCESS_VALUE").expect("worker value");

    let storage = LocalStorage::open(root).expect("open local storage in worker");
    let head = ObjectKey::parse("HEAD").expect("valid HEAD key");
    let runtime = runtime();
    let observed = runtime
        .block_on(storage.get_with_token(&head))
        .expect("read base HEAD in worker");

    fs::write(&ready, b"ready").expect("publish worker readiness");
    wait_for(&go);

    let outcome = runtime.block_on(storage.replace_if_token_matches(
        &head,
        &observed.version,
        Bytes::from(value),
    ));
    let label = match outcome {
        Ok(_) => "success",
        Err(StorageError::PreconditionFailed { .. }) => "precondition_failed",
        Err(error) => panic!("unexpected worker result: {error}"),
    };
    fs::write(result, label).expect("write worker result");
}

fn spawn_worker(
    executable: &Path,
    root: &Path,
    ready: &Path,
    go: &Path,
    result: &Path,
    value: &str,
) -> Child {
    Command::new(executable)
        .args(["--exact", "subprocess_cas_worker", "--nocapture"])
        .env(WORKER_FLAG, "1")
        .env("SNAPSHOTSTORE_PROCESS_ROOT", root)
        .env("SNAPSHOTSTORE_PROCESS_READY", ready)
        .env("SNAPSHOTSTORE_PROCESS_GO", go)
        .env("SNAPSHOTSTORE_PROCESS_RESULT", result)
        .env("SNAPSHOTSTORE_PROCESS_VALUE", value)
        .spawn()
        .expect("spawn CAS worker")
}

fn assert_worker_success(output: Output) {
    assert!(
        output.status.success(),
        "worker failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn separate_processes_cannot_both_replace_the_same_token() {
    let directory = tempdir().expect("temporary directory");
    let storage = LocalStorage::open(directory.path()).expect("open local storage");
    let head = ObjectKey::parse("HEAD").expect("valid HEAD key");
    let runtime = runtime();
    runtime
        .block_on(storage.create_if_absent(&head, Bytes::from_static(b"base")))
        .expect("create initial HEAD");

    let ready_a = directory.path().join("ready-a");
    let ready_b = directory.path().join("ready-b");
    let result_a = directory.path().join("result-a");
    let result_b = directory.path().join("result-b");
    let go = directory.path().join("go");
    let executable = env::current_exe().expect("current test executable");

    let child_a = spawn_worker(
        &executable,
        directory.path(),
        &ready_a,
        &go,
        &result_a,
        "writer-a",
    );
    let child_b = spawn_worker(
        &executable,
        directory.path(),
        &ready_b,
        &go,
        &result_b,
        "writer-b",
    );

    wait_for(&ready_a);
    wait_for(&ready_b);
    fs::write(&go, b"go").expect("release both workers");

    assert_worker_success(child_a.wait_with_output().expect("wait for worker A"));
    assert_worker_success(child_b.wait_with_output().expect("wait for worker B"));

    let outcomes = [
        fs::read_to_string(result_a).expect("worker A result"),
        fs::read_to_string(result_b).expect("worker B result"),
    ];
    assert_eq!(
        outcomes.iter().filter(|value| *value == "success").count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|value| *value == "precondition_failed")
            .count(),
        1
    );

    let committed = runtime
        .block_on(storage.get_with_token(&head))
        .expect("read final HEAD")
        .bytes;
    assert!(
        committed == Bytes::from_static(b"writer-a")
            || committed == Bytes::from_static(b"writer-b")
    );
}
