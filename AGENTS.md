# AGENTS.md

## Principles (in priority order)

1. **Clean API surface** — usage for common cases should be straightforward and well-documented. Avoid breaking changes unless they significantly improve usability.
2. **Understandable code** — cyclomatic complexity per method should stay around or below current levels. Higher-level methods should delegate to clearly-named helpers so the logic reads like pseudo-code. Use utility functions (e.g. `kWayMerge`, binary search) for generic algorithms.
3. **Performance** — maintain good performance across all code paths. The Index and Partition layers are most performance-sensitive and may prefer performance over readability at the margin. Elsewhere, prefer simplicity; simpler code is often faster.

**Keep this file up-to-date**: after any code review that surfaces new architectural insights or a new principle, compact this file and integrate what was learned.

## Architecture

```
EventStore  →  Storage  →  Partition (append-only data files)
            →  Index    →  per-stream index files
            →  EventStream / JoinEventStream (iterators over indexes)
            →  Consumer (durable position tracking)
```

- **Storage/Index/Partition** each follow a 3-tier class hierarchy under `src/<Component>/`: `Readable*` → `ReadOnly*` → `Writable*`. The facade file `src/<Component>.js` re-exports the Writable + ReadOnly variants.
- **EventStore** (`src/EventStore.js`) is the main entry point — owns a `Storage` instance, manages stream indexes in a `streams/` subdirectory, and provides `commit()` / `getEventStream()` / `query()` / `createConsumer()`.
- **DCB concurrency**: `query()` returns a `CommitCondition` capturing the global log position + type/matcher filter. Passing it as `expectedVersion` to `commit()` rejects only when matching events were appended since the query.
- Streams are named indexes over a shared storage file; events are partitioned by `event.stream`. Category queries use `<category>-<id>` naming convention.

## Lifecycle & I/O Patterns

- **Constructor stays sync** — all file I/O is deferred to `open()`.
- **`open()` is the entry point for I/O**: scanning partitions, scanning index files, acquiring locks, and opening file descriptors.
- **`'opened'` event** — emitted by Storage once the first async scan + primary index open completes. EventStore listens to `'opened'` to emit its own `'ready'`.
- **`'index-created'` event** — emitted by Storage during `scanFiles()` for each existing secondary index file found. EventStore uses this to register streams without its own directory scan.
- **Secondary indexes open on demand** — only the primary index is opened eagerly; secondary indexes are opened lazily on first access.
- **`initialized` three-state**: `null` = not started, `false` = scan in progress, `true` = scan done. Re-opens after `close()` are synchronous.
- **`open(callback)` hook** — fires after `openIndexes()` and before `'opened'`. Used by `WritableStorage` for torn-write repair.
- **LOCK_RECLAIM in `open()`** — orphaned lock removal lives in `WritableStorage.open()`, directly before `lock()`; torn-write repair runs via the `open(callback)` hook.
- **EventStore `initialize()`** — register `storage.on('index-created', ...)` *before* calling `storage.open()`.

## Key Commands

```bash
npm test              # runs mocha tests with c8 coverage (test/*.spec.js)
```

No build step; source is plain ESM consumed directly. No linter configured.

## Code Conventions

- **ESM only** — all files use `import`/`export`.
- Test files use `expect.js` (not chai/jest) with `mocha`. Each spec mirrors `src/` naming: `test/<Component>.spec.js`.
- Tests create temp data in `test/data/` and clean with `fs-extra.emptyDirSync` in `beforeEach`.
- Errors are custom classes exported alongside main classes (e.g. `OptimisticConcurrencyError`, `StorageLockedError`).
- The `index.js` barrel re-exports only the public API — keep it in sync when adding exports.
- **No underscore-prefixed names**; use descriptive public names even for internal helpers.
- **Expressive names over comments** — prefer clear method names. Doc blocks only for the *why*, not the *how*.

## File Layout

| Path | Purpose |
|------|---------|
| `src/EventStore.js` | Core: commit, query (DCB), streams, consumers |
| `src/Storage/Writable*.js` | Append-only file storage with locking |
| `src/Partition/Writable*.js` | Binary partition files with headers/metadata |
| `src/Index/Writable*.js` | Persisted stream indexes |
| `src/Consumer.js` | Durable consumer with position tracking and `progress` event |
| `src/EventStream.js` | Iterator over a single stream; `predicate === true` activates raw (NDJSON buffer) mode |
| `src/JoinEventStream.js` | Merges multiple streams; ordering uses epoch-denormalized `time64` + `sequenceNumber` tiebreaker from binary header |
| `src/Clock.js` | Monotonic microsecond clock |
| `src/IndexEntry.js` | Index record serialization |
| `src/IndexMatcher.js` | O(1) discriminant-based matcher classification |
| `src/PartitionPool.js` | LRU-evicting pool for open partition handles |
| `src/Watcher.js` / `src/WatchesFile.js` | Ref-counting directory watcher and mixin |
| `src/fsUtil.js` | `ensureDirectory`, `scanForFiles` |
| `src/metadataUtil.js` | Metadata matching for access control hooks |
| `bench/` | Benchmarks (not part of test suite) |
| `stress-test/` | Crash-safety / recovery validation |

## Testing Notes

- Always call `eventstore.close()` in `afterEach` to release file locks.
- Commits are async with callback: `eventstore.commit(stream, events, [expectedVersion], callback)`.
- `EventStore` emits `'ready'` after opening — wrap test logic inside that event.
- Prefer `once` over `on` for one-shot lifecycle events (`'opened'`, `'ready'`, `'index-created'`).
