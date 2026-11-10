# Recovery scenarios S01-S20

The first twenty named recovery and concurrency scenarios are executable in
`scenarios_s01_s20.rs`. Every scenario asserts the committed version, active
file count, complete batch-ID set, exact logical rows and row order, and
reachable history against the independent append model where applicable.

| ID | Scenario | Executable observation |
|---|---|---|
| S01 | Two creators race | Both calls converge on one table and version-0 commit. |
| S02 | Reader pinned before append | The pinned reader remains empty while the latest snapshot contains the append. |
| S03 | Reader opens after append | The new reader sees the complete batch and version 1. |
| S04 | Two distinct writers share a base | One initial CAS is rejected; both batches survive bounded rebase. |
| S05 | Ten distinct concurrent batches | A ten-way initial collision plus bounded retries commits every ID exactly once. |
| S06 | Concurrent same ID and contents | Both calls return one first-commit identity; one is a replay. |
| S07 | Concurrent same ID, different contents | One value commits and the other call returns `BatchIdReuse`. |
| S08 | Sequential successful replay | The replay retains the first receipt and does not advance the table. |
| S09 | Sequential changed-content ID reuse | The changed input is rejected without changing durable state. |
| S10 | Stop before first data upload | A definite write failure leaves version 0 unchanged. |
| S11 | Stop during data upload | An unapplied unknown outcome exposes no partial file through a snapshot. |
| S12 | Stop after data, before manifest | The data object is orphaned and the old snapshot remains current. |
| S13 | Stop during manifest write | No candidate manifest becomes visible; the old snapshot remains current. |
| S14 | Stop after manifest, before `HEAD` | The candidate manifest is unreachable and version 0 remains current. |
| S15 | `HEAD` success, response lost | Reconciliation succeeds and a later retry returns the original receipt. |
| S16 | Timeout before `HEAD` takes effect | The call retries once and commits at most one logical batch. |
| S17 | Stale `HEAD` token | The stale CAS is rejected and rebasing preserves both batches. |
| S18 | Several consecutive lost races | Three rejected publications return typed `Conflict` without changing the winner. |
| S19 | Transient storage read failure | Opening returns typed storage unavailability, never an empty table. |
| S20 | Transient storage write failure | Append returns typed storage unavailability and never reports success. |

Run the matrix and optionally write a machine-readable result:

```sh
./scripts/test-snapshot-scenarios
./scripts/test-snapshot-scenarios --report artifacts/snapshot-scenarios-s01-s20.json
```

The report records the tested source SHA and local result counts. These tests
exercise deterministic process-level failure points and real local storage
operations. They do not prove physical power-loss behavior or AWS S3 behavior.
The checked-in [latest S01-S20 result](../artifacts/snapshot-scenarios-s01-s20.json)
is tied to the source commit tested by the report command.
