# Changelog

## 1.3.3

- Raised the minimum supported Node.js version to `>=20.0`.
- Improved `ReadOnlyStorage` live-watch behavior for hierarchical partition/index layouts by relying on recursive file watching for nested paths.
- Fixed a bug around `ReadOnlyStorage` when watch events don't come in reliably; ReadOnly mode is much more robust now
- Documented the runtime dependency for read-only file watching: reliable recursive `fs.watch` support is required (currently supported on Windows, macOS, and Linux).

## 1.3.2

- Added `EventStore.makeReadOnly([callback])` as a niche handover API for switching a running writer process into read-only mode without restarting it. For the common case, creating a fresh read-only `EventStore` instance remains the recommended approach.
- Added public `VERSION` export at the package root and mirrored it on `EventStore.VERSION` so dependent layers can read the `event-storage` version directly without resolving/parsing `package.json` at runtime.
- Fixed `ReadOnlyStorage.open()` to keep the same opened-event semantics as the readable and writable storage variants, so read-only reopen paths and the `makeReadOnly()` callback now work consistently.

## 1.3.1

- Added published TypeScript declarations (`index.d.ts`) and package-level `types` metadata so consumers can reference `EventStore` and related public APIs from `event-storage`.
- `EventStream` now exposes `where()` for matcher-based filtering.
- `EventStream.filter()` now supports `Readable.filter(callback, options)` delegation when options are provided.
- Matcher-style `EventStream.filter(matcher)` remains supported for backward compatibility but now emits a deprecation warning and should be migrated to `where()`.

## 1.3.0

New in 1.3:

- Object matcher syntax now supports scalar operators: `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`.
- Operator matchers work consistently across object mode, raw-buffer mode, and DCB/query filters.
- `$eq`-only operator matchers are folded into the scalar fast path for raw matching (same hot path as plain equality).
- Public low-level matcher utilities are now exported from the package root (`matches`, `buildRawBufferMatcher`) for reuse by dependent layers.

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
