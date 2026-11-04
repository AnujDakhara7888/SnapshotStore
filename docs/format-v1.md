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

The schema fingerprint is SHA-256 over this unambiguous byte sequence:

1. the ASCII domain separator `snapshotstore-schema-v1` followed by a zero byte;
2. the field count as an unsigned 64-bit big-endian integer;
3. for each field in order, its UTF-8 name length as unsigned 64-bit big-endian,
   the name bytes, a one-byte type tag, and a one-byte nullability flag.

Type tags are `0` for boolean, `1` for int64, `2` for uint64, `3` for float64,
and `4` for utf8. The nullability byte is `0` or `1`. This encoding is part of
format v1 and cannot change without a new format version.

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

Committed history is the parent chain reachable from `HEAD`. Every positive
version links to exactly the preceding version and carries forward the complete
parent file list and cumulative batch registry. Unreachable manifests and data
objects are not committed table state.

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

The public [snapshot descriptor v1](snapshot-descriptor-v1.md) is derived from
a validated table, `HEAD`, and manifest. It is an integration contract, not an
additional persisted commit object.

## Executable examples

- [`table.json`](../fixtures/format-v1/table.json)
- [`head.json`](../fixtures/format-v1/head.json)
- [`manifest-v0.json`](../fixtures/format-v1/manifest-v0.json)
- [`manifest-v1.json`](../fixtures/format-v1/manifest-v1.json)
