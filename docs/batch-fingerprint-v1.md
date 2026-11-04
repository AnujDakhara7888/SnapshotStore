# Logical batch fingerprint v1

Replay safety compares caller-supplied batch IDs with a SHA-256 digest of their
logical schema and ordered rows. The encoding is stable across Arrow
`RecordBatch` boundaries, Parquet file count, row-group layout, compression,
attempt IDs, and object paths.

## Encoding

The SHA-256 input is this exact byte sequence:

1. the ASCII domain separator `snapshotstore-logical-batch-v1` followed by a
   zero byte;
2. the table schema fingerprint as 64 lowercase ASCII hexadecimal bytes;
3. the field count as an unsigned 64-bit big-endian integer;
4. for each field in order: its UTF-8 name length as unsigned 64-bit big-endian,
   its name bytes, its one-byte type tag, and a one-byte nullability flag;
5. the total logical row count as unsigned 64-bit big-endian;
6. each row in order, with each value encoded as its one-byte type tag, a
   one-byte presence marker (`0` for null or `1` for present), and its payload
   when present.

Type tags are `0` for boolean, `1` for int64, `2` for uint64, `3` for float64,
and `4` for utf8. Boolean payloads are one byte (`0` or `1`). Signed and unsigned
integers are eight-byte big-endian values. UTF-8 payloads have an unsigned
64-bit big-endian byte length followed by the bytes.

Float payloads are their IEEE 754 binary64 bits in big-endian order. Positive
and negative zero are normalized to positive zero. NaN and positive or negative
infinity are rejected in format v1 rather than assigned replay semantics.

## Replay behavior

The cumulative committed-batch registry stores the fingerprint, first commit
ID and version, and added row count. Repeating an ID with the same fingerprint
returns that first receipt and does not write data, create a manifest, or
advance `HEAD`. Repeating an ID with a different fingerprint is an error. The
same logical rows under a different batch ID are an intentional new append.

The test suite freezes a mixed-type fixture digest and verifies that one Arrow
batch and differently split batches produce the same value.
