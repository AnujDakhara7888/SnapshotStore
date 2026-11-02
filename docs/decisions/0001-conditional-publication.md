# ADR 0001: Conditional publication through one pointer

## Status

Accepted.

## Context

Immutable files are easy to write concurrently, but a table needs one ordered
answer to “which files are committed?” Listing a prefix cannot provide that
answer reliably and does not prevent two writers from overwriting each other's
metadata.

## Decision

Use `HEAD` as the only mutable publication object. Writers read `HEAD` with an
opaque version token, prepare immutable data and a complete candidate manifest,
then replace `HEAD` only if that token still matches. Initial publication uses
create-if-absent.

The storage contract distinguishes a rejected condition from storage
unavailability and from a write whose result is unknown. Backend version tokens
remain opaque and are never used as content checksums.

## Consequences

- Successful publications are serialized through one conditional write.
- A reader can pin one immutable manifest for its entire operation.
- Losing writers may leave unreachable objects, but cannot expose partial data.
- All production backends must demonstrate real conditional-write semantics.
- `HEAD` can become a throughput bottleneck; distributed consensus and
  multi-table transactions remain outside this design.
