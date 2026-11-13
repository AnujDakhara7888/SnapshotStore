# Command-line interface

The `snapshotstore` binary manages one local table root and writes structured
JSON to standard output. Errors are written as one JSON object to standard
error, except argument-parsing errors, which use clap's human-readable usage
output.

Build the binary from a pinned checkout:

```sh
cargo build --release --locked -p snapshotstore
```

The examples below use `target/release/snapshotstore`; `cargo run --locked -p
snapshotstore --` can be used during development.

## Create a table

Schema files use the canonical format-v1 field representation:

```json
{
  "fields": [
    {"name": "event_id", "data_type": "uint64", "nullable": false},
    {"name": "category", "data_type": "utf8", "nullable": true}
  ]
}
```

```sh
target/release/snapshotstore create \
  --table ./demo-table \
  --schema ./events-schema.json
```

Creation is idempotent for the same schema. A different schema is rejected;
the existing table identity is never replaced.

## Ingest Parquet

```sh
target/release/snapshotstore ingest \
  --table ./demo-table \
  --input ./batch-a.parquet \
  --batch-id batch-a
```

The input must contain at least one row and match the table schema exactly.
Success returns the table ID, original commit ID and version, batch ID,
logical fingerprint, row count, replay flag, and current table version. A
matching retry returns the original commit identity with `replayed: true` and
does not advance the current version. Reusing the ID for different logical
contents returns `batch_id_reuse`.

## Inspect snapshots and history

```sh
target/release/snapshotstore inspect --table ./demo-table
target/release/snapshotstore inspect --table ./demo-table --version 1
target/release/snapshotstore history --table ./demo-table
```

Inspection returns the stable snapshot descriptor plus cumulative row and
committed-batch counts. Historical versions are resolved only through the
manifest chain reachable from the current `HEAD`.

## Verify committed state

```sh
target/release/snapshotstore verify --table ./demo-table
```

Verification traverses the complete reachable history and reads every
committed snapshot, checking metadata links, object length, SHA-256 checksum,
Parquet decoding, schema, and row counts. For the local backend it also reports
files under `data/` and `manifests/` that are not reachable from `HEAD`.
Diagnostic listing is never used to discover committed state, and verification
does not delete or adopt orphan objects.

## Exit status contract

Successful commands return 0. Stable category codes allow scripts to
distinguish input, concurrency, integrity, and availability failures; the JSON
`error` field provides the specific typed condition.

| Status | Meaning | Example JSON `error` values |
|---:|---|---|
| 2 | Invalid command-line arguments | clap usage output, not JSON |
| 10 | Local file or output I/O | `io_error` |
| 11 | Invalid schema JSON or Parquet input | `invalid_input` |
| 20 | Table absent or incompletely initialized | `table_not_found`, `initialization_incomplete` |
| 21 | Request rejected | `schema_mismatch`, `empty_batch`, `invalid_batch`, `batch_id_reuse`, `version_not_found` |
| 22 | Publication retry bound exhausted | `conflict` |
| 23 | Committed integrity failure | `missing_committed_file`, `corrupt_data`, `unsupported_format`, `corrupt_metadata` |
| 24 | Storage unavailable | `storage_unavailable` |
| 25 | Write outcome cannot be reconciled | `unknown_commit_status` |

The CLI currently supports the durable local backend. AWS S3 behavior must not
be inferred from these commands or local verification results.
