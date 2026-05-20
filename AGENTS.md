# AGENTS.md

## Project Overview

Embedded append-only event store for Node.js (ESM-only, >=18). Published as `event-storage` on npm.
Single runtime dependency (`mkdirp`). All storage, indexing, and partitioning is implemented from scratch in pure JS — no database engine underneath.

## Architecture

```
EventStore  →  Storage  →  Partition (append-only data files)
            →  Index    →  per-stream index files
            →  EventStream / JoinEventStream (iterators over indexes)
            →  Consumer (durable position tracking)
```

- **Storage/Index/Partition** each follow a 3-tier class hierarchy under `src/<Component>/`: `Readable*` → `ReadOnly*` → `Writable*`. The facade file `src/<Component>.js` re-exports the Writable + ReadOnly variants.
- **EventStore** (`src/EventStore.js`, ~770 lines) is the main entry point — owns a `Storage` instance, manages stream indexes in a `streams/` subdirectory, and provides `commit()` / `getEventStream()` / `query()` / `createConsumer()`.
- **DCB concurrency**: `query()` returns a `CommitCondition` capturing the global log position + type/matcher filter. Passing it as `expectedVersion` to `commit()` rejects only when matching events were appended since the query — see `CommitCondition` class in `EventStore.js`.
- Streams are **named indexes** over a shared storage file; events are partitioned by `event.stream`. Category queries use `<category>-<id>` naming convention.
- Read-only mode (`config.readOnly: true`) instantiates `ReadOnly*` variants for safe multi-process reads.

## Lifecycle & I/O Patterns

- **Constructor stays sync**: no I/O in constructors. All file I/O is deferred to `open()`.
- **`open()` is the entry point for I/O**: scanning partitions, scanning index files, acquiring locks, and opening file descriptors all happen in `open()` (or methods it calls).
- **`'opened'` event**: emitted by Storage once the first async scan + primary index open completes. EventStore listens to `'opened'` to emit its own `'ready'`.
- **`'index-created'` event**: emitted by Storage during `scanFiles()` for each existing secondary index file found. EventStore uses this to register streams without its own directory scan.
- **Secondary indexes open on demand**: only the primary index is opened eagerly in `openIndexes()`. Secondary indexes are opened lazily on first access.
- **`initialized` three-state**: `null` = not started (or scan cancelled by `close()`), `false` = scan in progress, `true` = scan done. Re-opens after `close()` are synchronous.
- **`open(callback)` hook**: the optional callback passed to `open()` fires after `openIndexes()` and before `'opened'` is emitted — a synchronous alternative to listening for `'opened'`. Used by `WritableStorage` for torn-write repair and available for callers that need to run code immediately after the index is ready.
- **LOCK_RECLAIM in `open()`**: orphaned lock removal lives in `WritableStorage.open()`, directly before the `lock()` call; torn-write repair runs via the `open(callback)` hook. Use a closure-captured local variable (`needsRepair`) — no instance flags.
- **EventStore `initialize()`**: register `storage.on('index-created', ...)` *before* calling `storage.open()` so scan-phase events are not missed.

## Key Commands

```bash
npm test              # runs mocha tests with c8 coverage (test/*.spec.js)
```

There is no build step — source is plain ES6 consumed directly. No linter is configured.

## Code Conventions

- **ESM only** — all files use `import`/`export`. No `require()`.
- Test files use `expect.js` (not chai/jest) with `mocha`. Each spec mirrors `src/` naming: `test/<Component>.spec.js`.
- Tests create temp data in `test/data/` and clean it with `fs-extra.emptyDirSync` in `beforeEach`.
- `__dirname` equivalent is derived via `fileURLToPath(new URL('.', import.meta.url))`.
- Errors are custom classes exported alongside main classes (e.g. `OptimisticConcurrencyError`, `StorageLockedError`, `CorruptFileError`).
- The `index.js` barrel re-exports only the public API (`EventStore`, `EventStream`, `Storage`, `Index`, `Consumer`, `ExpectedVersion`, `OptimisticConcurrencyError`, `CommitCondition`, `LOCK_THROW`, `LOCK_RECLAIM`, `StorageLockedError`) — keep it in sync when adding new exports.
- **No underscore-prefixed names** for methods or properties. Use descriptive public names even for internal helpers (e.g. `scanFiles`, `openIndexes`).
- **Expressive names over comments**: prefer renaming or extracting a method with a clear name rather than adding a comment. Doc blocks only for the *why*, not the *how*; skip them when obvious from the function name. No redundant inline comments.

## File Layout

| Path | Purpose |
|------|---------|
| `src/EventStore.js` | Core: commit, query (DCB), streams, consumers, hooks |
| `src/Storage/Writable*.js` | Append-only file storage with locking |
| `src/Partition/Writable*.js` | Binary partition files with headers/metadata |
| `src/Index/Writable*.js` | Persisted stream indexes |
| `src/Consumer.js` | Durable event consumer with position tracking |
| `src/EventStream.js` | Iterator over a single stream's index |
| `src/JoinEventStream.js` | Merges multiple streams into one iterator |
| `src/Clock.js` | Monotonic microsecond clock for sequential event numbering |
| `src/IndexEntry.js` | Entry interface and implementations for index record serialization |
| `src/IndexMatcher.js` | O(1) discriminant-based matcher classification for secondary indexes |
| `src/PartitionPool.js` | LRU-evicting pool for open partition file handles |
| `src/Watcher.js` | Ref-counting singleton directory watcher |
| `src/WatchesFile.js` | Mixin adding file-change watching to a class |
| `src/fsUtil.js` | Filesystem helpers (`ensureDirectory`, `scanForFiles`) |
| `src/metadataUtil.js` | Metadata matching helper for access control hooks |
| `bench/` | Benchmarks (standalone, not part of test suite) |
| `stress-test/` | Crash-safety / recovery validation scripts |

## Testing Notes

- Always call `eventstore.close()` in `afterEach` to release file locks.
- Commits are async with callback: `eventstore.commit(stream, events, [expectedVersion], callback)`.
- `EventStore` emits `'ready'` after opening — wrap test logic inside that event.
- Prefer `once` over `on` for one-shot assertions on events that fire exactly once per lifecycle (e.g. `'opened'`, `'ready'`).
- After a `close()` + `open()` cycle, resources not eagerly re-opened must be opened explicitly before use.

