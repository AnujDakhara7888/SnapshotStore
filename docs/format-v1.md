# Persistent format v1

Format v1 represents one fixed-schema append-only table. JSON field names and
example values are executable fixtures under `fixtures/format-v1`.

## `table.json`

`table.json` is created once and never replaced. It contains:

- `format_version`: `1`;
- `table_id`: the table UUID retained through initialization recovery;
- `schema`: ordered fields with a name, supported logical type, and nullability;
- `schema_fingerprint`: a lowercase hexadecimal SHA-256 digest.

Supported scalar types are `boolean`, `int64`, `uint64`, `float64`, and `utf8`.
Field order is significant. Schema evolution is not supported in this format.

## `HEAD`

`HEAD` contains `table_id`, `version`, `commit_id`, and `manifest_key`. The
manifest key must be `manifests/<commit-id>.json`. Every successful publication
uses a new commit ID, so a pointer never returns to byte-identical content.

## Manifests

A manifest includes:

- format, table, version, commit, parent-manifest, and schema identities;
- cumulative row count and the complete active data-file list;
- the operation that created the version;
- the cumulative mapping of batch IDs to their logical input fingerprint and
  first successful commit identity.

Version 0 is an empty `create` manifest with no parent, files, rows, or batch
IDs. Positive versions are `append` manifests with a parent, a nonempty batch,
and at least one file and row.

Each file descriptor records its relative object key, byte length, row count,
SHA-256 content checksum, and schema fingerprint. Committed files with missing
bytes, zero rows, duplicate keys, schema mismatches, or inconsistent cumulative
row counts are rejected as corrupt metadata.

## Keys and digests

Object keys are relative to a configured table root. Absolute paths, backslash
separators, empty segments, `.` segments, and `..` segments are invalid.
Credentials and absolute storage locations never appear in persistent metadata.

SHA-256 values are exactly 64 lowercase hexadecimal characters. The logical
input fingerprint algorithm and canonical row encoding will be specified with
the replay implementation; the current format reserves and validates its
representation without claiming that computation exists yet.

## Compatibility

Readers reject an unsupported format version rather than guessing. Linked
metadata must agree on table UUID, schema fingerprint, version, commit ID, and
manifest key. Unknown JSON fields are currently tolerated by Serde for forward
reader compatibility; writers emit only the documented v1 fields.

## Executable examples

- [`table.json`](../fixtures/format-v1/table.json)
- [`head.json`](../fixtures/format-v1/head.json)
- [`manifest-v0.json`](../fixtures/format-v1/manifest-v0.json)
- [`manifest-v1.json`](../fixtures/format-v1/manifest-v1.json)
