# Copilot instructions for node-event-storage

## Code style

- **No underscore-prefixed names** for methods or properties. Use descriptive public names even for internal helpers (e.g. `scanFiles`, `openIndexes`).
- **Expressive names over comments**: prefer renaming or extracting a method with a clear name rather than adding a comment that explains what the code does.
- **Doc blocks only for the "why"**: skip doc blocks whose content is obvious from the function name and signature. When a doc block is warranted, keep it to one or two sentences explaining *why* the code exists, not *how* it works. Avoid restating the code in prose.
- **No redundant inline comments**: a comment like `// Re-open after close()` above a clearly named `openIndexes()` call adds no value. Only add inline comments for non-obvious logic or required context.
- **JSDoc `@param`/`@returns`** are useful on public API methods where the types or semantics aren't obvious from context; skip them for trivial wrappers.

## Architecture patterns

- **Constructor stays sync**: no I/O in constructors. All file I/O is deferred to `open()`.
- **`open()` is the entry point for I/O**: scanning partitions, scanning index files, acquiring locks, and opening file descriptors all happen in `open()` (or methods it calls).
- **`'opened'` event**: emitted by Storage once the first async scan + primary index open completes. EventStore listens to `'opened'` to emit its own `'ready'`.
- **`'index-created'` event**: emitted by Storage during `scanFiles()` for each existing secondary index file found. EventStore uses this to register streams without its own directory scan.
- **Secondary indexes open on demand**: only the primary index is opened eagerly in `openIndexes()`. Secondary indexes are opened lazily on first access.
- **`_initialized` three-state**: `null` = not started (or scan cancelled by `close()`), `false` = scan in progress, `true` = scan done. Re-opens after `close()` are synchronous.
- **LOCK_RECLAIM in `open()`**: orphaned lock removal and torn-write repair both live in `WritableStorage.open()`, directly before the `lock()` call. Use a closure-captured local variable (`needsRepair`) — no instance flags.
- **EventStore `initialize()`**: register `storage.on('index-created', ...)` *before* calling `storage.open()` so scan-phase events are not missed.

## Testing

- Use `once('opened', ...)` (not `on`) when waiting for the first-open event in tests.
- Use `once('index-created', ...)` to avoid double-firing when both the scan and a file-watcher can emit the same event.
- After a `close()` + `open()` cycle, secondary indexes are not eagerly re-opened; call `index.open()` explicitly before checking `index.length` in tests.
