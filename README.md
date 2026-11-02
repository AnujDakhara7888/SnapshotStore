# SnapshotStore

SnapshotStore is a Rust project for building transactional, append-only tables
from immutable Parquet files. It targets a narrow but important storage
problem: making concurrent appends atomic while readers retain a consistent,
pinned view of a table.

## Why this project exists

Object stores provide durable objects, but a useful table needs more than a
directory of files. Writers must not overwrite one another, readers must not
observe half-published batches, and a retried request must not duplicate rows
when the original response is lost. SnapshotStore addresses those problems
with immutable data files, versioned full manifests, one conditionally updated
publication pointer, and a committed batch registry.

## Current implementation

The repository contains the pinned Rust, Apache Arrow, and Apache Parquet
foundation, the version-one persistent metadata model, and the minimal storage
contract needed for conditional publication. The local filesystem adapter adds
interprocess locking, durable create-if-absent, and atomic token-matched
replacement. Executable format fixtures cover an empty table and its first
append. Tests validate their JSON round trips, cross-object identities, row
counts, file sets, batch registry, relative keys, and typed conditional-write
behavior.

A separate integration test writes a nullable Arrow `RecordBatch` to a
Zstandard-compressed Parquet file and reads it back without changing its schema,
values, nulls, or row order.

Transactional metadata and table APIs are under active development. The
following behavior is design intent until its implementation and tests land:

- immutable Parquet data files and complete versioned manifests;
- optimistic compare-and-swap publication for local storage and AWS S3;
- pinned current and historical snapshots;
- replay-safe batch IDs with canonical logical fingerprints;
- typed conflict, corruption, and unknown-outcome errors.

## Design overview

Each table will have an immutable identity and schema, immutable manifests and
data files, and a single mutable `HEAD` pointer. Writers prepare data and a new
manifest before conditionally replacing `HEAD`; that replacement is the
transaction's visibility point. Readers resolve one committed manifest and do
not refresh it during a scan.

This design intentionally favors a small, auditable correctness model over
metadata scalability. Each manifest carries the complete active file list and
committed batch registry.

See the [architecture](docs/architecture.md), [format v1
contract](docs/format-v1.md), and [conditional-publication decision
record](docs/decisions/0001-conditional-publication.md). Local `fsync`, locking,
and platform assumptions are detailed in the [local durability
contract](docs/local-durability.md).

## Build and test

The repository pins Rust 1.96.0 and exact Arrow/Parquet dependency versions.
With `rustup` installed:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

CI runs the same checks on pushes and pull requests. The toolchain's minimum
supported Rust version is 1.96.

## Project layout

```text
crates/snapshotstore/   Core library and integration tests
.github/workflows/      Read-only build, lint, and test CI
```

## Scope and limitations

SnapshotStore is deliberately append-only. Row updates and deletes, schema
evolution, cross-table transactions, compaction, vacuum, replication,
distributed consensus, and a network service are outside the initial scope.

The current code defines and tests storage and metadata contracts plus a local
durable backend. It does not yet provide table creation, transactional append
execution, replay safety, end-to-end concurrent-writer guarantees, historical
reads, a command-line interface, or validated S3 behavior. The local evidence
covers conditional replacement across two processes, not the complete append
protocol or physical power-loss behavior. Further claims will be added only
with executable evidence.
