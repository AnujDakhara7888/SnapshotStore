use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use snapshotstore::descriptor::SnapshotDescriptorV1;
use snapshotstore::errors::TableError;
use snapshotstore::metadata::{BatchId, CanonicalSchemaV1};
use snapshotstore::storage::local::LocalStorage;
use snapshotstore::table::{
    AppendReceipt, Table, append_batches, create_table, open_table, open_table_at_version,
};
use snapshotstore::verify::verify_local_table;

#[derive(Debug, Parser)]
#[command(
    name = "snapshotstore",
    version,
    about = "Transactional append-only tables over immutable Parquet files"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create an empty table from a canonical v1 schema JSON file.
    Create {
        #[arg(long)]
        table: PathBuf,
        #[arg(long)]
        schema: PathBuf,
    },
    /// Append a Parquet input under a replay-safe batch ID.
    Ingest {
        #[arg(long)]
        table: PathBuf,
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        batch_id: String,
    },
    /// Print every committed version reachable from the current HEAD.
    History {
        #[arg(long)]
        table: PathBuf,
    },
    /// Print the current or a historical committed snapshot.
    Inspect {
        #[arg(long)]
        table: PathBuf,
        #[arg(long)]
        version: Option<u64>,
    },
    /// Validate reachable metadata/data and report local orphan objects.
    Verify {
        #[arg(long)]
        table: PathBuf,
    },
}

#[derive(Debug, Serialize)]
struct SnapshotOutput {
    snapshot: SnapshotDescriptorV1,
    row_count: u64,
    committed_batches: u64,
}

impl SnapshotOutput {
    fn from_table(table: &Table<LocalStorage>) -> Self {
        Self {
            snapshot: table.descriptor(),
            row_count: table.manifest().row_count,
            committed_batches: table.manifest().committed_batches.len() as u64,
        }
    }
}

#[derive(Debug, Serialize)]
struct HistoryOutput {
    snapshots: Vec<SnapshotOutput>,
}

#[derive(Debug, Serialize)]
struct IngestOutput {
    receipt: AppendReceipt,
    current_version: u64,
}

#[derive(Debug, Serialize)]
struct ErrorOutput<'a> {
    error: &'a str,
    message: String,
}

#[derive(Debug)]
struct CliFailure {
    code: u8,
    name: &'static str,
    message: String,
}

impl CliFailure {
    fn io(context: &str, error: impl std::fmt::Display) -> Self {
        Self {
            code: 10,
            name: "io_error",
            message: format!("{context}: {error}"),
        }
    }

    fn input(context: &str, error: impl std::fmt::Display) -> Self {
        Self {
            code: 11,
            name: "invalid_input",
            message: format!("{context}: {error}"),
        }
    }

    fn table(error: TableError) -> Self {
        let (code, name) = match &error {
            TableError::TableNotFound => (20, "table_not_found"),
            TableError::InitializationIncomplete { .. } => (20, "initialization_incomplete"),
            TableError::SchemaMismatch => (21, "schema_mismatch"),
            TableError::EmptyBatch => (21, "empty_batch"),
            TableError::InvalidBatch { .. } => (21, "invalid_batch"),
            TableError::BatchIdReuse { .. } => (21, "batch_id_reuse"),
            TableError::VersionNotFound { .. } => (21, "version_not_found"),
            TableError::HistoricalSnapshotReadOnly => (21, "historical_snapshot_read_only"),
            TableError::Conflict { .. } => (22, "conflict"),
            TableError::MissingCommittedFile { .. } => (23, "missing_committed_file"),
            TableError::CorruptData { .. } => (23, "corrupt_data"),
            TableError::UnsupportedFormat { .. } => (23, "unsupported_format"),
            TableError::CorruptMetadata { .. } => (23, "corrupt_metadata"),
            TableError::StorageUnavailable { .. } => (24, "storage_unavailable"),
            TableError::UnknownCommitStatus { .. } => (25, "unknown_commit_status"),
        };
        Self {
            code,
            name,
            message: error.to_string(),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let output = ErrorOutput {
                error: error.name,
                message: error.message,
            };
            match serde_json::to_string(&output) {
                Ok(json) => eprintln!("{json}"),
                Err(serialization_error) => {
                    eprintln!(
                        "{{\"error\":\"internal_error\",\"message\":\"error output serialization failed: {serialization_error}\"}}"
                    );
                }
            }
            ExitCode::from(error.code)
        }
    }
}

async fn run(cli: Cli) -> Result<(), CliFailure> {
    match cli.command {
        Command::Create { table, schema } => {
            let schema = read_schema(&schema)?;
            let storage = local_storage(&table)?;
            let table = create_table(storage, schema)
                .await
                .map_err(CliFailure::table)?;
            write_json(&SnapshotOutput::from_table(&table))
        }
        Command::Ingest {
            table,
            input,
            batch_id,
        } => {
            let storage = local_storage(&table)?;
            let table = open_table(storage).await.map_err(CliFailure::table)?;
            let batches = read_parquet(&input)?;
            let batch_id =
                BatchId::parse(batch_id).map_err(|error| CliFailure::input("batch ID", error))?;
            let result = append_batches(&table, batch_id, &batches)
                .await
                .map_err(CliFailure::table)?;
            let current_version = result.version();
            write_json(&IngestOutput {
                receipt: result.receipt,
                current_version,
            })
        }
        Command::History { table } => {
            let storage = local_storage(&table)?;
            let latest = open_table(storage).await.map_err(CliFailure::table)?;
            let descriptors = latest.history().await.map_err(CliFailure::table)?;
            let mut snapshots = Vec::with_capacity(descriptors.len());
            for descriptor in descriptors {
                let table = latest
                    .snapshot_at_version(descriptor.version)
                    .await
                    .map_err(CliFailure::table)?;
                snapshots.push(SnapshotOutput::from_table(&table));
            }
            write_json(&HistoryOutput { snapshots })
        }
        Command::Inspect { table, version } => {
            let storage = local_storage(&table)?;
            let table = match version {
                Some(version) => open_table_at_version(storage, version).await,
                None => open_table(storage).await,
            }
            .map_err(CliFailure::table)?;
            write_json(&SnapshotOutput::from_table(&table))
        }
        Command::Verify { table } => {
            let storage = local_storage(&table)?;
            let report = verify_local_table(storage)
                .await
                .map_err(CliFailure::table)?;
            write_json(&report)
        }
    }
}

fn local_storage(path: &Path) -> Result<Arc<LocalStorage>, CliFailure> {
    LocalStorage::open(path)
        .map(Arc::new)
        .map_err(|error| CliFailure::io("open table directory", error))
}

fn read_schema(path: &Path) -> Result<CanonicalSchemaV1, CliFailure> {
    let bytes = std::fs::read(path).map_err(|error| CliFailure::io("read schema", error))?;
    serde_json::from_slice(&bytes).map_err(|error| CliFailure::input("decode schema JSON", error))
}

fn read_parquet(path: &Path) -> Result<Vec<arrow::record_batch::RecordBatch>, CliFailure> {
    let file = File::open(path).map_err(|error| CliFailure::io("open Parquet input", error))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .and_then(|builder| builder.build())
        .map_err(|error| CliFailure::input("open Parquet input", error))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| CliFailure::input("decode Parquet input", error))
}

fn write_json(value: &impl Serialize) -> Result<(), CliFailure> {
    let json = serde_json::to_string_pretty(value)
        .map_err(|error| CliFailure::io("encode JSON output", error))?;
    println!("{json}");
    Ok(())
}
