# Local release demonstration

The executable local demo walks through SnapshotStore's main transaction and
recovery guarantees with a temporary table. It uses the public table and
storage APIs and prints one JSON report only after every observation succeeds.

Run it from the repository root:

```sh
./scripts/run-local-demo
```

The demonstration:

1. creates an empty table and appends batch A;
2. pins version 1, appends batch B, and proves the pinned reader still sees
   only A while the newer snapshot sees A and B;
3. replays B without advancing the table and rejects changed content under
   B's batch ID;
4. forces two writers to attempt their first publication together and proves
   bounded rebase preserves both batches at versions 3 and 4;
5. injects a definite failure immediately before publishing `HEAD`, restarts
   from the same local directory, and compares the visible rows with an
   independent in-memory sequence;
6. retries the interrupted batch, traverses versions 0 through 5, and verifies
   every reachable file while reporting the abandoned data and manifest
   objects as unreachable.

The JSON output contains the observed versions, row values, replay and
rejection outcomes, history, integrity totals, and orphan counts. UUIDs and the
order of the two racing batches vary from run to run; the committed versions
and verified totals do not.

This is a local-filesystem demonstration. The deterministic fault is injected
through the storage interface before the publication point, so it validates
the transaction protocol but not physical power-loss behavior. It also does
not establish AWS S3 behavior; that requires a real backend and an authorized
disposable test prefix.
