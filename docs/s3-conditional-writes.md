# AWS S3 conditional-write check

SnapshotStore relies on atomic conditional writes to publish table versions.
This check exercises that primitive using two independently constructed AWS S3
clients from the pinned `object_store` 0.14.2 dependency. The example explicitly
selects `S3ConditionalPut::ETagMatch`, which is also the default in that version.
Create uses `If-None-Match: *` and replace uses `If-Match` with the opaque ETag
returned by the latest object read.
[Client source](https://docs.rs/object_store/0.14.2/src/object_store/aws/mod.rs.html)
· [AWS S3 conditional-write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

Use an existing AWS S3 general-purpose bucket and a disposable prefix that you
are authorized to write to. Allow `s3:GetObject` and `s3:PutObject` only for the
test prefix. Provide `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`
(or `AWS_DEFAULT_REGION`) through your environment. Include `AWS_SESSION_TOKEN`
when using temporary credentials.

```sh
./scripts/test-snapshot-s3 --prefix "$SNAPSHOTSTORE_TEST_PREFIX" \
  --report artifacts/s3-conditional-writes.json
```

The runner accepts the same prefix through `SNAPSHOTSTORE_TEST_PREFIX` when
`--prefix` is omitted. The prefix must include at least one key segment.
Each run writes one small object at a fresh `snapshotstore-cas-<uuid>` suffix,
printed to stderr for cleanup. Use your test prefix's retention policy or
remove that single object through your authorized cleanup process.

The check creates the object, rejects a duplicate create, replaces it with
the current ETag, rejects a stale ETag, and races two replacements against
the same ETag. Exactly one racing write must succeed, and the final read must
return that writer's bytes. On success the runner prints JSON with the checked
source commit, worktree cleanliness, client configuration, and results.
`--report` writes a new file without overwriting an existing report. Missing
access or a failed check returns a nonzero exit status.

The default local backend works without the optional S3 client. A local HTTP
fixture checks the client's request headers and rejection handling in CI.
AWS verification requires a successful run against an authorized prefix.
The probe covers conditional
object writes. Table publication, replay and pinned reads on AWS require
separate end-to-end evidence.
