# Local storage durability

The local backend implements the storage contract with ordinary files under one
canonical root directory. It is designed for Linux/POSIX filesystems where
same-directory rename is atomic and directory file descriptors can be synced.

## Mutation protocol

All protocol mutations acquire an exclusive advisory lock on
`.snapshotstore.lock`. The lock is owned by the open file handle and is released
automatically if the process exits.

For a new immutable object or create-if-absent pointer:

1. Create missing parent directories and sync each directory that receives a
   new child directory.
2. Create a unique temporary file in the target directory.
3. Write all bytes and call `fsync` on the temporary file.
4. Create the target with an atomic same-filesystem hard link. This fails rather
   than overwriting an existing key.
5. Remove the temporary name and `fsync` the target directory.

For token-matched replacement:

1. Acquire the interprocess lock.
2. Read the current bytes and derive their opaque local token.
3. Reject a stale token without writing anything.
4. Write and `fsync` a unique same-directory temporary file.
5. Atomically rename it over the target.
6. `fsync` the target directory before returning success.

The local token is the SHA-256 digest of the exact observed bytes. Transactional
metadata uses a new commit ID for each valid publication, so a successful HEAD
update cannot intentionally return to identical pointer content.

## Error boundary

An error before the atomic link or rename is reported as unavailable and leaves
the target unchanged. A directory-sync error after publication is reported as
an unknown write outcome because the new name is visible but its crash
durability cannot be proven. The caller must reconcile unknown outcomes against
committed metadata.

## Concurrency evidence

The process integration test launches two independent test processes. Both read
the same HEAD token, signal readiness through a deterministic barrier, and then
attempt replacement. The test requires exactly one success and one stale-token
failure and verifies that the final file is one complete writer value.

## Limits

- Locks are advisory. Every writer with filesystem access must follow the same
  protocol; access controls remain part of deployment.
- The release target is Linux on a local filesystem. Network filesystems and
  platform-specific rename/directory-sync behavior are not yet validated.
- Passing process-crash tests and using `fsync` do not prove behavior across a
  physical power loss or faulty storage hardware.
- The current adapter performs blocking filesystem calls inside the async
  storage interface. It is suitable for the bounded CLI workload; a highly
  concurrent service would move these calls to dedicated blocking workers.
