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

## Initialization and opening

Creation first installs `table.json` with immutable create-if-absent semantics.
If another creator wins, both writers adopt the winner's table identity after
checking the requested schema. A creator then writes an empty immutable
version-0 manifest and creates `HEAD`. The first `HEAD` is the initialization
visibility point; losing creators open the winning snapshot instead of
overwriting it.

An absent `table.json` means no table exists. An existing `table.json` without
`HEAD` is an interrupted initialization that a later same-schema create can
complete while retaining the original table UUID. After a write reports an
unknown outcome, creation reads the target state and succeeds only when the
durable bytes prove the write took effect. Otherwise it returns an explicit
unknown-status error.

Opening resolves `table.json`, `HEAD`, and the named immutable manifest once,
then validates their linked identities. The resulting handle remains pinned to
that manifest even if a later writer advances `HEAD`.

## Append and historical reads

An append validates a nonempty Arrow batch against the fixed table schema,
encodes it as Zstandard Parquet, and writes it under a unique attempt path. It
then writes an immutable full manifest containing every active file and the
parent manifest link. Only a token-matched replacement of `HEAD` makes those
prepared objects visible. If another writer has already changed `HEAD`, the
append opens the winning snapshot and rechecks the cumulative batch registry.
A matching replay returns the first receipt; a changed payload under that ID is
rejected; an uncommitted ID is rebased onto the winner. Data files are prepared
once and reused while each retry writes a new immutable full manifest. After
three unsuccessful conditional replacements the call returns a typed conflict.
Unreachable manifests remain harmless orphans.

A table handle owns the manifest and file list it resolved, so later commits do
not alter its reads. Historical lookup walks parent links from a committed
`HEAD`; it never treats an unreferenced manifest as history. Each link must
decrease the version by one and preserve table identity, schema, prior files,
row totals, and the cumulative batch registry.

Reading verifies each committed file's presence, byte length, SHA-256 checksum,
Arrow schema, and decoded row count. Missing or modified committed data is an
error rather than a partial result.

## Replay identity

Each append carries a caller-supplied batch ID and a canonical logical
fingerprint. The fingerprint covers the fixed schema and ordered logical rows,
not Arrow chunk boundaries, Parquet bytes, compression, object paths, or
attempt IDs. A matching ID and fingerprint returns the first commit receipt
from the cumulative manifest registry without writing new objects. A matching
ID with different logical content is rejected.

Receipts retain the table ID, batch ID, logical fingerprint, first successful
commit ID and version, added row count, and whether the current call was a
replay. The first receipt remains authoritative even after later commits.

## Recovery validation

Deterministic integration tests intercept data upload, manifest upload, and
conditional `HEAD` replacement. They simulate failures before application and
lost responses after application. Each observed durable state is compared with
a small append-only reference model built from the test inputs rather than the
table metadata. The comparison covers exact row order, versions, history,
files, row totals, and unique committed batch IDs. The full boundary behavior
and its limitations are documented in the [testing guide](testing.md).

## Local backend

The local adapter uses an advisory interprocess lock, same-directory staging,
file and directory `fsync`, atomic hard-link creation, and atomic rename
replacement. Its [durability contract and platform limits](local-durability.md)
are documented separately. A deterministic subprocess test verifies that two
writers sharing one stale token cannot both publish.

## Current implementation boundary

The storage and metadata contracts, table initialization, bounded append
publication, pinned and historical snapshots, stable descriptor, and local
filesystem adapter are implemented. Concurrent local writers recheck the
replay registry and rebase without re-encoding data. The recovery harness
injects applied and unapplied outcomes at every append write boundary. The S3
adapter, complete recovery scenario matrix, and command-line interface are
separate layers and are not yet available.
