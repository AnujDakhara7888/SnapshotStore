# ADR 0003: Bound optimistic append rebasing

## Status

Accepted.

## Context

Writers prepare immutable Parquet files and full manifests from a pinned table
version. A token-matched `HEAD` replacement prevents lost updates, but simply
returning a conflict forces callers to repeat file encoding and upload even
when their batch is independent of the winning append. Unbounded automatic
retry can starve under sustained contention and makes latency unpredictable.

## Decision

One append call makes at most three conditional `HEAD` replacement attempts.
Its Parquet files are prepared once under one attempt ID. After a conditional
rejection, the writer opens the latest committed snapshot and checks its
cumulative batch registry before retrying:

- the same batch ID and fingerprint returns the original durable receipt;
- the same ID with a different fingerprint returns `BatchIdReuse`;
- an absent ID builds a new full manifest on the latest committed parent and
  retries with that parent's publication token.

Every publication attempt uses a new commit ID and immutable manifest. A
successful `HEAD` replacement is still the only visibility point. After the
third rejection, the writer returns a typed conflict containing the starting
version, latest observed version, and attempt count.

## Consequences

Independent writers preserve both batches without re-encoding Parquet, and
concurrent matching retries converge on one first-commit receipt. Failed
attempts may leave unreachable manifests, while a losing changed-content writer
may also leave its prepared data file. Listing never makes those objects
visible, and automatic deletion remains outside v1 because it could race a
reader or an uncertain publication outcome.

The fixed bound limits work and latency but does not guarantee progress under
continuous contention. A caller may retry a typed conflict as a new operation.
