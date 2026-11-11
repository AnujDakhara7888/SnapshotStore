# Correctness testing

SnapshotStore treats recovery and concurrency claims as executable properties,
not assumptions derived from the implementation. The integration suite uses a
real local storage root and wraps its storage contract with deterministic fault
injection.

## Publication fault boundaries

Append tests can fail each write immediately before the backend call or return
an unknown result immediately after durable application:

1. immutable Parquet data upload;
2. immutable manifest upload;
3. conditional `HEAD` replacement.

An unapplied data or manifest write with an unknown result remains invisible
and returns unknown status because storage cannot prove the object exists. An
unapplied `HEAD` result can be reconciled against the still-current snapshot and
retried within the publication bound. Applied data and manifest writes are
accepted only when a read returns the exact bytes. An applied `HEAD` update is
accepted only after committed state proves which batch and commit became
visible. If that reconciliation read is unavailable, the API returns
`UnknownCommitStatus`; reopening and replaying the same batch ID discovers the
original receipt without adding rows.

## Independent append model

The recovery suite keeps a small append-only model containing only the inputs
the test chose to commit: ordered batch IDs and ordered `u64` values. It does
not construct expected rows or versions from SnapshotStore manifests. After a
fault or recovery, it opens the durable table and compares:

- the committed version and complete version history;
- exact row values and order;
- active file count and row count;
- the complete set of batch IDs, with no duplicates;
- each batch's first version and added-row count.

This model is intentionally narrower than the production format. It is an
oracle for append visibility and replay behavior, not another implementation
of metadata publication.

## Named recovery matrix

Scenarios S01-S30 cover creator and writer races, pinned and historical
readers, replay and ID reuse, interruption at each append publication boundary,
stale writers, retry exhaustion, integrity failures, multi-file atomicity,
restart with orphans, subprocess termination, and seeded mixed histories. The
[scenario report](recovery-scenarios.md) records each required observation and
the command that runs the matrix. A machine-readable result can be generated
with `./scripts/test-snapshot-scenarios --report <path>`; it records the current
source SHA only after all thirty tests pass.

## Limits

The deterministic wrapper validates protocol behavior for applied and
unapplied storage outcomes. It does not emulate physical power loss, filesystem
firmware behavior, or a cloud provider. Local durability assumptions are
documented separately, and real S3 claims require dedicated backend tests.
