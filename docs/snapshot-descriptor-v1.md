# Snapshot descriptor v1

`SnapshotDescriptorV1` is the stable, backend-independent view exported to a
query integration. It describes exactly one committed snapshot and contains no
storage credentials, absolute paths, or mutable pointer state.

## Contract

The top-level fields are:

- `format_version`: persistent format version, currently `1`;
- `table_id`: immutable table UUID;
- `version`: committed table version;
- `commit_id`: immutable manifest UUID;
- `schema`: ordered canonical fields;
- `schema_fingerprint`: format-v1 schema digest;
- `files`: active data files in manifest order.

Each file contains its relative `object_key`, `byte_size`, `row_count`, SHA-256
`checksum`, and `schema_fingerprint`. File keys follow
`data/<attempt-id>/<file-id>.parquet`. Duplicate keys, empty files, zero-row
files, and schema mismatches are invalid.

Version 0 is always empty. Later versions may contain files. The descriptor is
constructed only after `table.json`, `HEAD`, and the named manifest pass their
individual and cross-object checks, so its table, version, commit, and schema
identities refer to one committed state.

## Compatibility and usage

Consumers should reject an unsupported `format_version` and should not infer
commit state from object listing. Resolve a table through its API, export the
descriptor, then use its immutable file list for the lifetime of that query.
Refreshing is an explicit new open and may produce a different descriptor.

The checked-in [`snapshot-descriptor-v1.json`](../fixtures/snapshot-descriptor-v1.json)
is decoded, validated, and compared with a descriptor exported from the format
fixtures in the test suite.
