# ADR 0002: Local atomic publication with an advisory process lock

## Status

Accepted.

## Context

Thread-only synchronization cannot protect a table when separate CLI processes
append concurrently. Replacing a file without flushing its content and parent
directory can also report success before the new directory entry is durable.

## Decision

Serialize local mutations with a whole-file advisory lock shared by every
process. Stage bytes in the target directory, sync the file, publish new keys
with an atomic hard link, publish replacements with same-directory rename, and
sync the directory before success.

Use a digest of the observed bytes as the opaque local compare-and-swap token.
Return an unknown-outcome error if failure occurs after the atomic publication
step but before directory durability is confirmed.

## Consequences

- Separate protocol-compliant processes cannot silently overwrite one another.
- Readers see either the old complete file or the new complete file.
- Failed writers can leave hidden temporary files; successful operations clean
  them, and later verification can report leftovers.
- Correctness depends on local POSIX semantics and protocol-compliant access.
  Network filesystems and physical power-loss guarantees require separate
  evidence.
