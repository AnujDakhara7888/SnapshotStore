use std::fs::{self, File};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use snapshotstore::metadata::{CanonicalSchemaV1, DataTypeV1, SchemaFieldV1};
use tempfile::tempdir;

fn executable() -> &'static str {
    env!("CARGO_BIN_EXE_snapshotstore")
}

fn run(arguments: &[&str]) -> Output {
    Command::new(executable())
        .args(arguments)
        .output()
        .expect("run snapshotstore CLI")
}

fn success_json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "successful command writes no stderr"
    );
    serde_json::from_slice(&output.stdout).expect("stdout is JSON")
}

fn schema_file(path: &Path) {
    let schema = CanonicalSchemaV1 {
        fields: vec![SchemaFieldV1 {
            name: "value".to_owned(),
            data_type: DataTypeV1::UInt64,
            nullable: false,
        }],
    };
    fs::write(
        path,
        serde_json::to_vec_pretty(&schema).expect("encode schema"),
    )
    .expect("write schema");
}

fn parquet_file(path: &Path, values: Vec<u64>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(values)) as ArrayRef],
    )
    .expect("build fixture batch");
    let mut writer =
        ArrowWriter::try_new(File::create(path).expect("create Parquet"), schema, None)
            .expect("create Parquet writer");
    writer.write(&batch).expect("write Parquet batch");
    writer.close().expect("close Parquet writer");
}

#[test]
fn cli_create_ingest_replay_history_inspect_and_verify() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("table");
    let schema = directory.path().join("schema.json");
    let input = directory.path().join("batch.parquet");
    schema_file(&schema);
    parquet_file(&input, vec![10, 11]);
    let root_arg = root.to_str().expect("UTF-8 path");
    let schema_arg = schema.to_str().expect("UTF-8 path");
    let input_arg = input.to_str().expect("UTF-8 path");

    let created = success_json(run(&[
        "create", "--table", root_arg, "--schema", schema_arg,
    ]));
    assert_eq!(created["snapshot"]["version"], 0);
    assert_eq!(created["row_count"], 0);

    let first = success_json(run(&[
        "ingest",
        "--table",
        root_arg,
        "--input",
        input_arg,
        "--batch-id",
        "batch-a",
    ]));
    assert_eq!(first["receipt"]["first_version"], 1);
    assert_eq!(first["receipt"]["added_rows"], 2);
    assert_eq!(first["receipt"]["replayed"], false);
    assert_eq!(first["current_version"], 1);

    let replay = success_json(run(&[
        "ingest",
        "--table",
        root_arg,
        "--input",
        input_arg,
        "--batch-id",
        "batch-a",
    ]));
    assert_eq!(replay["receipt"]["replayed"], true);
    assert_eq!(replay["receipt"]["first_version"], 1);
    assert_eq!(
        replay["receipt"]["first_commit_id"],
        first["receipt"]["first_commit_id"]
    );
    assert_eq!(replay["current_version"], 1);

    let version_zero = success_json(run(&["inspect", "--table", root_arg, "--version", "0"]));
    assert_eq!(version_zero["snapshot"]["version"], 0);
    assert_eq!(version_zero["row_count"], 0);

    let current = success_json(run(&["inspect", "--table", root_arg]));
    assert_eq!(current["snapshot"]["version"], 1);
    assert_eq!(current["row_count"], 2);
    assert_eq!(current["committed_batches"], 1);

    let history = success_json(run(&["history", "--table", root_arg]));
    assert_eq!(
        history["snapshots"]
            .as_array()
            .expect("history array")
            .len(),
        2
    );
    assert_eq!(history["snapshots"][0]["snapshot"]["version"], 0);
    assert_eq!(history["snapshots"][1]["snapshot"]["version"], 1);

    let orphan_data = root.join(
        "data/00000000-0000-4000-8000-000000000001/00000000-0000-4000-8000-000000000002.parquet",
    );
    let orphan_manifest = root.join("manifests/00000000-0000-4000-8000-000000000003.json");
    fs::create_dir_all(orphan_data.parent().expect("orphan parent"))
        .expect("create orphan data parent");
    fs::write(&orphan_data, b"orphan").expect("write orphan data");
    fs::write(&orphan_manifest, b"orphan").expect("write orphan manifest");

    let verification = success_json(run(&["verify", "--table", root_arg]));
    assert_eq!(verification["latest_version"], 1);
    assert_eq!(verification["snapshots_verified"], 2);
    assert_eq!(verification["rows_verified"], 2);
    assert_eq!(verification["committed_batches_verified"], 1);
    assert_eq!(
        verification["unreachable_data_files"][0],
        "data/00000000-0000-4000-8000-000000000001/00000000-0000-4000-8000-000000000002.parquet"
    );
    assert_eq!(
        verification["unreachable_manifests"][0],
        "manifests/00000000-0000-4000-8000-000000000003.json"
    );
    assert!(orphan_data.exists(), "verify never deletes orphan data");
    assert!(
        orphan_manifest.exists(),
        "verify never deletes orphan manifests"
    );
}

#[test]
fn cli_returns_stable_codes_for_reuse_and_integrity_failures() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("table");
    let schema = directory.path().join("schema.json");
    let first_input = directory.path().join("first.parquet");
    let changed_input = directory.path().join("changed.parquet");
    schema_file(&schema);
    parquet_file(&first_input, vec![1]);
    parquet_file(&changed_input, vec![2]);
    let root_arg = root.to_str().expect("UTF-8 path");
    success_json(run(&[
        "create",
        "--table",
        root_arg,
        "--schema",
        schema.to_str().expect("UTF-8 path"),
    ]));
    success_json(run(&[
        "ingest",
        "--table",
        root_arg,
        "--input",
        first_input.to_str().expect("UTF-8 path"),
        "--batch-id",
        "stable-id",
    ]));

    let reuse = run(&[
        "ingest",
        "--table",
        root_arg,
        "--input",
        changed_input.to_str().expect("UTF-8 path"),
        "--batch-id",
        "stable-id",
    ]);
    assert_eq!(reuse.status.code(), Some(21));
    assert!(reuse.stdout.is_empty());
    let reuse_error: Value = serde_json::from_slice(&reuse.stderr).expect("reuse error JSON");
    assert_eq!(reuse_error["error"], "batch_id_reuse");

    let inspected = success_json(run(&["inspect", "--table", root_arg]));
    let data_key = inspected["snapshot"]["files"][0]["object_key"]
        .as_str()
        .expect("data object key");
    fs::remove_file(root.join(data_key)).expect("remove committed fixture data");
    let verify = run(&["verify", "--table", root_arg]);
    assert_eq!(verify.status.code(), Some(23));
    assert!(verify.stdout.is_empty());
    let verify_error: Value = serde_json::from_slice(&verify.stderr).expect("verify error JSON");
    assert_eq!(verify_error["error"], "missing_committed_file");
}

#[test]
fn clap_usage_errors_return_two() {
    let output = run(&["inspect"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--table"));
}
