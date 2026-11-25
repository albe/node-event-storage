# AGENTS.md

## General guidelines for interaction

Do not repeat yourself. Be concise and precise in your internal thoughts, but especially your answers. No paraphrasing of previous information unless specifically asked for. Only quote code you generated if less than 5 lines, otherwise just refer to the file and line number where it can be found. If you need to refer to a specific part of the code, quote only the relevant lines.
If you run into a decision point, maybe because the instructions given were contradictory or incomplete, try to identify the underlying principle or intent and unless that is absolutely clear, ask for clarification and/or a decision. Explain the issue you stumbled upon and the options you see, and ask for guidance on how to proceed.
The code and documentation language is English, even if the user is communicating in another language.
**Keep this file up-to-date**: after any code review that surfaces new architectural insights or a new principle, compact this file and integrate what was learned.

## Principles (in priority order)

1. **Clean API surface** — usage for common cases should be straightforward and well-documented. Avoid breaking changes unless they significantly improve usability.
2. **Understandable code** — cyclomatic complexity per method should stay around or below current levels (~15). Higher-level methods should delegate to clearly-named helpers so the logic reads like pseudo-code. Use utility functions (e.g. `kWayMerge`, binary search) for generic algorithms.
3. **Performance** — maintain good performance across all code paths. The Index and Partition layers are most performance-sensitive and may prefer performance over readability at the margin. Elsewhere, prefer simplicity; simpler code is often faster.

### Performance trade-off lessons

- **Pre-compile over per-call dispatch**: if a descriptor or configuration object is known at setup time, compile it into closures or lookup structures once rather than re-interpreting it on every hot-path call. The gain compounds with call frequency (e.g. building operator closures once per matcher object instead of running `Object.entries` + switch per document).
- **WeakMap for caching derived state on user objects**: prefer `WeakMap` over Symbol properties or plain properties to attach computed state keyed on user-supplied objects — avoids mutation, works on frozen objects, and is GC-friendly. On the hot path use `cache.get()` directly rather than `has()` + `get()` to avoid a double lookup.
- **Pass cheap prefilter results as hints to expensive scans**: when a cheap pass (e.g. `Buffer.indexOf`) already locates a candidate position, carry that result forward as a hint to the costlier depth- or context-aware pass rather than letting it re-scan from the start. Eliminates a full O(n) scan on every call.
- **Custom implementations only when benchmarks justify the complexity**: a hand-rolled solution may edge out a standard library call by 10–15 %, but the maintenance cost is rarely worth it unless the gain is substantial and measurable at realistic data sizes (e.g. a custom numeric parser vs. `JSON.parse` with try/catch).
- **Prefer a flag on a shared helper over near-duplicate functions**: when two functions differ only in a single behavioral detail, add a boolean parameter to the shared function rather than maintaining two near-identical copies. Keep the flag's semantics explicit and limited to one axis of variation.
- **Inline single-use hot-path helpers once the surrounding flow becomes simpler**: if a helper is only called from one place and its logic can be embedded while keeping the caller below the local complexity/return-count budget, prefer the inline version. Removes call indirection and often makes the hot path easier to read as straight-line pseudo-code.


### Code Quality standards

- Keep it simple! Follow the KISS principle and avoid unnecessary abstractions or optimizations. Simple code is easier to read, test, and maintain and most of the time also more performant.
- Keep number of return statements per method below 6. Multiple return points can be acceptable if they significantly improve readability, but watch for methods that become hard to follow due to too many exit points.
- Avoid deep nesting of conditionals. If you find yourself nesting more than 3 levels deep, consider refactoring to flatten the structure, such as by using guard clauses or extracting nested logic into separate methods.
- In general, optimize the code for readability first, and only introduce complexity when there is a clear performance benefit that has been measured and justified.

## Architecture

```
EventStore  →  Storage  →  Partition (append-only data files)
            →  Index    →  per-stream index files
            →  EventStream / JoinEventStream (iterators over indexes)
            →  Consumer (durable position tracking)
```

- **Storage/Index/Partition** each follow a 3-tier class hierarchy under `src/<Component>/`: `Readable*` → `ReadOnly*` → `Writable*`. The facade file `src/<Component>.js` re-exports the Writable + ReadOnly variants.
- **EventStore** (`src/EventStore.js`) is the main entry point — owns a `Storage` instance, manages stream indexes in a `streams/` subdirectory, and provides `commit()` / `getEventStream()` / `query()` / `getConsumer()`.
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
- **The writer loads every secondary index file it finds** (`WritableStorage.foundIndexFile()`), because torn-write repair (`checkTornWrites()`/`reindex()`) only reaches loaded indexes via `forEachWritableSecondaryIndex`. Anything that registers indexes without loading them (lazy open, a startup manifest, an index that catches up from a persisted position) must truncate or rebuild them inside the repair run itself, not defer it to a later `openIndex()`: its stale-tail check (`lastEntry.number > index.length`) no longer fires once the primary index has grown past the stale entries again.
- **One sync path**: partitions only flush; every partition flush and every new file is handed to the storage-wide `SyncScheduler` (`flushed()`, `created()`), which syncs immediately (`syncOnFlush`), in the next run (`syncInterval`) or never. Files are synced through `FileHandlePool.sync(target[, callback])`: it `fsync`s the pooled descriptor if the file is open (pinned during a background sync, so an eviction meanwhile closes it only after the sync), otherwise it opens the file itself (`syncFile()` in `fsUtil`). Like the rest of the library, the sync methods (`syncFile`, `syncDirectory`, `FileHandlePool.sync`, `SyncScheduler.run`, `checkpoint`) block without a callback and run in the background with one; the runs and the checkpoint pass a callback, `syncOnFlush` and `close()` do not. `close()` closes the files only after the last run. A run syncs its files in parallel in the background; runs never overlap, so callbacks fire in write order. Evicting a handle only flushes.
- **Indexes are not synced on flush**: the storage syncs the modified ones (`index.modified`, `takeModifiedIndexes()`) with the checkpoint, and on close hands them to the `SyncScheduler` for its last run (so not at all without a sync configuration). A repair does not sync what it rebuilt; it keeps the previous checkpoint, so a crash before the next one rebuilds the same entries again. Every 5 min, `WritableStorage.checkpoint()` syncs the modified indexes in the background (through their pool) and then records the primary index length taken before in the lock directory. The repair after a crash rebuilds everything after that position. Keep index syncs off the commit path; they are rebuildable. Never rely on the OS writing files back in any order.
- **EventStore `initialize()`** — register `storage.on('index-created', ...)` *before* calling `storage.open()`.
- **Watcher singleton key includes watch options** — do not share one `DirectoryWatcher` across different `fs.watch` option sets (notably recursive vs non-recursive), or read-only/file watchers can miss events.
- **ReadOnlyStorage watcher filenames may contain leading directories** — normalize watched filenames to the storage-relative segment (starting at `<storageFile>...`) so nested index roots (e.g. `streams/`) and hierarchical stream paths both resolve correctly.

## Key Commands

```bash
npm test              # runs mocha tests with c8 coverage (test/*.spec.js)
```

No build step; source is plain ESM consumed directly. No linter configured.

## Code Conventions

- **ESM only** — all files use `import`/`export`.
- **No underscore-prefixed names** for methods or properties. Use descriptive public names even for internal helpers (e.g. `scanFiles`, `openIndexes`).
- **Do not reuse EventEmitter internals for app state**: in classes extending `EventEmitter`/`Readable`, never use reserved fields like `_events` as custom caches; keep cache state in dedicated properties (e.g. `cachedEvents`) to avoid breaking listener registration/pipe behavior.
- **Expressive names over comments**: prefer renaming or extracting a method with a clear name rather than adding a comment that explains what the code does.
- **Doc blocks only for the "why"**: skip doc blocks whose content is obvious from the function name and signature. When a doc block is warranted, keep it to one or two sentences explaining *why* the code exists, not *how* it works. Avoid restating the code in prose.
- **No redundant inline comments**: a comment like `// Re-open after close()` above a clearly named `openIndexes()` call adds no value. Only add inline comments for non-obvious logic or required context.
- Test files use `expect.js` (not chai/jest) with `mocha`. Each spec mirrors `src/` naming: `test/<Component>.spec.js`.
- Tests create temp data in `test/data/` and clean with `fs-extra.emptyDirSync` in `beforeEach`.
- Errors are custom classes exported alongside main classes (e.g. `OptimisticConcurrencyError`, `StorageLockedError`).
- The `index.js` barrel re-exports only the public API — keep it in sync when adding exports.

## Non-obvious Code Locations

- `src/EventStream.js`: `predicate === true` activates raw (NDJSON buffer) mode.
- `src/JoinEventStream.js`: ordering uses epoch-denormalized `time64` + `sequenceNumber` tiebreaker from the binary header.
- `src/IndexMatcher.js`: O(1) discriminant-based routing of written documents to secondary indexes.
- `src/utils/apiHelpers.js`: argument normalization for public APIs (commit overloads, revision clamping, predicate/raw shorthand).
- `src/utils/metadataUtil.js`: object/raw matcher compilation, operator syntax, HMAC-protected function matchers.
- `bench/` holds benchmarks, `stress-test/` the crash-safety validation; neither is part of `npm test`.

## Documentation

- User docs live in `docs/` (MkDocs, built by Read the Docs per release tag). Write them for the current state only: no "since version x" notes; version history belongs in `docs/changelog.md` and `docs/upgrading.md`.
- When a public API changes, update `docs/api.md`, `index.d.ts`, the relevant guide page and `docs/changelog.md` in the same change. Mark breaking changes as such in the changelog.
- Read the Docs builds one documentation version per release tag (`vX.Y.Z`, activated by an automation rule); `stable` is the latest tag, `latest` is `main`. Link docs as `/en/stable/` from README and package metadata.
- Changes to file layouts, durability or locking must be reflected in `docs/storage-format.md`, `docs/guarantees.md` and, if limits change, `docs/limitations.md`. Claims in `docs/guarantees.md` name the code and tests that back them; keep `docs/verify.md` in sync when tests are renamed.
- Breaking changes get a page under `docs/upgrading/` and a row in its overview table.
- `concepts/` holds internal design drafts. Never link them from user docs; document only what was implemented.

## Testing Notes

- Always call `eventstore.close()` in `afterEach` to release file locks.
- `eventstore.commit(stream, events, [expectedVersion], [metadata], [callback])` throws synchronously on validation/concurrency errors; the callback receives the `commit` object after the flush.
- `EventStore` emits `'ready'` after opening — wrap test logic inside that event.
- Prefer `once` over `on` for one-shot lifecycle events (`'opened'`, `'ready'`, `'index-created'`).
