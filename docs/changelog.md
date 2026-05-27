# Changelog

## 1.2.0

Breaking changes:

- `scanConsumers()` now returns parsed consumer descriptors (`{ name, stream, identifier }`) instead of filename strings.
- Stream/type names produced via `typeAccessor` are validated more strictly; invalid names (for example with whitespace) now fail fast.

New in 1.2:

- Raw EventStreams (`raw=true`) across stream/category/query APIs, emitting NDJSON `Buffer` chunks for zero-deserialization transport paths.
- Raw predicates and object matchers for byte-level filtering, plus fixes for NDJSON/raw behavior in `EventStream` and `JoinEventStream`.
- Read-path performance and correctness improvements (`readRange`, merge/backwards iteration fixes, index sync optimization, partition identifier caching).
- Consumer lifecycle improvements (in-memory registry lookup, `progress` events).

## 1.1.0

Minor breaking change: the low-level `Storage` API now completes opening asynchronously via `open()` / `'opened'`. This does not affect the top-level `EventStore` API layer, which still becomes ready through its existing `'ready'` event.

New in 1.1:

- hierarchical slash-separated stream names, alongside the existing dash-based category naming
- richer query-based commit conditions for dynamic consistency boundary workflows
- additional storage tuning options for large numbers of streams and partitions

## 1.0.0

Baseline 1.0 feature set:

- streams
- commit with expected version
- read-only mode
- consumers
- ESM syntax
