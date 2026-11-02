# Architecture

SnapshotStore separates immutable table state from one mutable publication
pointer. That separation keeps the transaction boundary small enough to audit
and test across storage backends.

## Object layout

```text
table.json                         immutable table identity and schema
HEAD                               mutable committed-version pointer
manifests/<commit-id>.json         immutable complete table manifest
data/<attempt-id>/<file-id>.parquet immutable data files
```

`HEAD` is the only object replaced during normal operation. A successful
conditional replacement of `HEAD` is the visibility point for an append.
Objects prepared by a writer that loses publication remain unreachable and do
not become part of table history.

## Storage boundary

The storage trait exposes four operations:

- read bytes and an opaque version token in one response;
- create an immutable object without overwriting an existing key;
- create a publication object only when absent;
- replace a publication object only when its current token matches.

Conditional rejection, temporary unavailability, and an unknown write outcome
are separate error classes. An adapter must report an unknown outcome whenever
it cannot prove whether an attempted mutation took effect. Tokens are passed
back to the backend without interpretation; they are not treated as checksums.

Listing is deliberately excluded from the transactional interface. Readers
discover committed state by resolving `HEAD` once and loading its immutable
manifest. Listing may later support diagnostics, but never commit discovery.

## Metadata model

Every manifest contains the complete active file list and complete committed
batch registry. This makes replay and recovery decisions local to one committed
manifest and avoids a separately published deduplication marker. The tradeoff is
metadata growth and full-manifest rewriting as a table accumulates files and
batches.

The checked-in [format contract](format-v1.md) and JSON fixtures are decoded,
re-encoded, and validated by the test suite.

## Local backend

The local adapter uses an advisory interprocess lock, same-directory staging,
file and directory `fsync`, atomic hard-link creation, and atomic rename
replacement. Its [durability contract and platform limits](local-durability.md)
are documented separately. A deterministic subprocess test verifies that two
writers sharing one stale token cannot both publish.

## Current implementation boundary

The storage and metadata contracts and local filesystem adapter are
implemented. The S3 adapter, table creation, append publication, replay
reconciliation, snapshots, and command-line interface are separate layers and
are not yet available.
