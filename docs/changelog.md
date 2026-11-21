# Changelog

Breaking changes come with an [upgrade guide](upgrading/index.md). Documentation for a released version is available through the version selector on Read the Docs.

## Unreleased

- Fix `fromStreams()`, `query()` and category streams returning events backwards when reading from a position beyond the end of the store, e.g. the last event for `query(selector, null, length + 1)`. A join that starts outside the store is now empty, like a single stream; an upper bound beyond the end still reads to the end.

## 1.5.0

See the [upgrade guide](upgrading/1.5.md) for the breaking changes.

- Fix backwards scans of a partition (backwards reads, `findDocument()`, repair) picking a wrong document position, which could make reads fail with "Invalid read position" or return the wrong document. The separator bytes `00 00 1E 0A` can also occur inside a document header (e.g. in the timestamp or as sequence number 7690), so a match is now only accepted if the size footer before it matches the header of the document it ends. A separator crossing the start of the read buffer is no longer skipped.
- Added `CommitCondition#without(...streams)` to narrow a condition, so that new events in those streams (e.g. event types) no longer cause a conflict. The streams are treated as empty, so the condition only gets narrower.
- Added `event-storage/projections` with `createProjection`, `composeProjections` and `buildDecisionModel`, implementing the [dcb.events](https://dcb.events/topics/projections/) projection format (with `when` for the handlers and `commitCondition` for the append condition) for DCB decision models on top of `query()` and `commit()`. `model.update()` applies only the events committed since a decision model was built, e.g. after a conflict or to keep a model in memory. `model.since()` returns the events that block a commit.
- Recover automatically after a crash: the storage lock now records its owner (pid, host name, pid namespace), and a lock whose owner process is gone on the same machine is reclaimed and the store repaired on open, without `LOCK_RECLAIM`. This is the new default `lock: LOCK_AUTO`; previously such a lock always threw `StorageLockedError`. Set `lock: LOCK_THROW` for the old behavior. `StorageLockedError` now names the lock holder, and `Storage.readLockOwner()` returns it. Read-only storages no longer register the lock directory as a partition.
- Added the `syncInterval` storage option for group commit: each partition is `fsync`ed at most every `syncInterval` milliseconds and commit callbacks fire only after that, so acknowledged commits survive an OS crash or power loss at a fraction of the cost of `syncOnFlush`. Added `Partition.sync()`. With `syncInterval` or `syncOnFlush`, the directory is now also synced after creating a new partition or index file.
- Added the `$startsWith` matcher operator for string prefixes: `{ payload: { type: { $startsWith: 'Order' } } }`. In raw mode a lone `$startsWith` compiles to a single byte-pattern check without parsing the value. It can be combined with the scalar comparison operators.
- Keep index entries in memory as chunks of raw entry bytes instead of one object per entry. A fully read index now needs about 16 bytes per entry instead of about 90, opening a large index no longer allocates an array of its full length, and `find()` no longer creates entry objects. `get()` still returns entry objects, but creates them on each call, so the same position no longer returns the identical object twice.
- `fromStreams()`/`query()` now evaluate the selector lazily over the indexes instead of building merged entry arrays: the first event is available immediately, memory no longer grows with the number of matches, and intersections skip non-matching entries with a binary search.
- **Breaking:** `Index.range()` now returns a lazy iterator instead of an array. `from > until` iterates backwards instead of returning `false`, out of bounds positions give an empty iteration, and one entry object is reused during the iteration, so copy what you need to keep. `Index.all()` returns the same kind of iterator over all entries. Removed `Index.validRange()`. `union()` and `intersect()` in `utils/indexUtil.js` now combine two such ranges lazily instead of entry arrays.
- Reject a write with an error before anything is written when the storage already holds 4,294,967,295 documents, or when the document would start beyond the first 4 GiB of its partition. Previously the document was written to the partition and only the index entry failed, leaving partition and index diverged.
- Store the consumer position as an unsigned 32-bit integer, so consumers can persist positions above 2,147,483,647. Existing consumer files read the same as before.
- Fix `Index.close()` dropping buffered entries, and never calling their flush callbacks, when the shared file handle pool had already closed the index file to make room for other files. This could happen with many open streams or a low `maxOpenIndexes`.
- Fix consumer state with non-ASCII characters being truncated on persist, which reset the consumer to its initial state and position 0 after a restart.
- Fix partitions whose names hash to the same 32-bit partition id sharing one file and its metadata. A colliding partition is now stored in a file with a `~<n>` suffix (e.g. `eventstore.account-8444~1`). Documents written into a shared file by earlier versions stay readable there. Partition names ending in `~<number>` are reserved and rejected for new partitions.
- Fix consumers of hierarchical streams (e.g. tag streams like `tags/course/c1`) never persisting their state, because the subdirectory for the consumer file was not created.
- Reject consumer identifiers that would place the consumer file outside of the `consumers` directory (e.g. `../../x`).
- Fix a consumer deleting the persisted state of other consumers whose file name extends its own (e.g. identifier `orders` vs. `orders.archive`, or stream `foo` vs. `foo.bar`). Temporary persist files are now named `<consumer>.<position>.tmp`, and only those are cleaned up on start.

## 1.4.1

- Emit `index-add` event on the `_all` stream, allowing consumers to listen for new events across all streams without needing to subscribe to each individual stream.
- Set `maxListeners` to `0` on all `EventStore` instances to allow unlimited concurrent listeners (fixes `MaxListenersExceededWarning` when many consumers are connected)

## 1.4.0

- **Breaking:** `fromStreams`/`query` no longer throws on non-existing streams; considers them empty.
- **Breaking:** `typeAccessor` must now be a dot-notation string path (e.g. `'type'`); function accessors are rejected. Use `streamSources` with a custom `nameBuilder` for custom stream names.
- Extended `JoinEventStream` with nested selector-algebra composition, available through `fromStreams`.
- Added `DcbQuery` shorthand syntax for `query()`: pass `{ items: [{ types, tags }] }` directly. Requires `typeAccessor` and/or `tagsAccessor` to be configured.
- Added `tagsAccessor` option: dot-notation path to a tag-string array in the event payload; auto-indexes one stream per distinct tag value with O(1) discriminant routing.
- Added `streamSources` option: generic stream-index definitions as `[{ path, nameBuilder }]` entries; `typeAccessor` and `tagsAccessor` are shorthands over this mechanism.
- Added `$has` and `$hasAny` matcher operators for array-containment checks: `{ tags: { $has: 'featured' } }` (single value) and `{ tags: { $hasAny: ['a', 'b'] } }` (any of). Both compile to fast byte-level scans in raw mode and support O(1) `IndexMatcher` routing.

## 1.3.5

- Fix edge case in JoinStream when reading interleaved streams that lead to read buffer backtracking

## 1.3.4

- Fix edge case in JoinStream when range not inside selected streams
- Fix bug with overwriting Stream internals

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
