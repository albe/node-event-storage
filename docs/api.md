# API Reference

This page lists all constructors and public methods of the three main classes.

!!! note "Stability"
    Methods marked **✅ Stable** are part of the documented public API and follow semantic versioning — they will not change in an incompatible way without a major version bump.

    Methods without that mark are **public but not yet stable**: they exist and work, but their signatures or behaviour may change in a minor release.

---

## EventStore

`EventStore` is the main entry point of the library.

```javascript
import { EventStore } from 'event-storage';
```

`EventStore` extends Node's `EventEmitter`.  After construction it emits a
`'ready'` event when it is safe to read and write.

### Constructor

```javascript
new EventStore([storeName], [config])
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `storeName` | `string` | `'eventstore'` | Prefix used for all storage files. |
| `config.storageDirectory` | `string` | `'./data'` | Directory where data files are stored. |
| `config.streamsDirectory` | `string` | `'{storageDirectory}/streams'` | Directory for stream index files. |
| `config.storageConfig` | `object` | `{}` | Options forwarded to the underlying `Storage` backend. |
| `config.readOnly` | `boolean` | `false` | Mount the store in read-only mode. |
| `config.streamMetadata` | `object\|function` | — | Metadata stored once per stream partition at creation time. Either a `{ streamName: metaObj }` map or a function `(streamName) => object`. |

### Methods

#### `close()` ✅ Stable

```javascript
eventstore.close()
```

Close the event store and free all resources.

---

#### `makeReadOnly([callback])` ✅ Stable

```javascript
eventstore.makeReadOnly([callback])
```

Flush pending writes, close the writable storage, and reopen the store in read-only mode.
The optional `callback` is called after the new read-only storage has emitted `'opened'`.

This is mainly useful for deployment handover workflows where one process stops writing and the same process switches into read-only mode; for most applications, `eventstore.close(); new EventStore(..., { readOnly: true })` is the simpler option.

---

#### `commit(streamName, events, [expectedVersion], [metadata], [callback])` ✅ Stable

```javascript
eventstore.commit(streamName, events [, expectedVersion] [, metadata] [, callback])
```

Append one or more events to a stream.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `streamName` | `string` | — | Name of the target stream. |
| `events` | `object\|Array<object>` | — | Event or array of events to commit. |
| `expectedVersion` | `number` | `ExpectedVersion.Any` | Optimistic concurrency version check. Use `ExpectedVersion.Any` (`-1`) to skip, `ExpectedVersion.EmptyStream` (`0`) for a new stream, or any positive integer for an exact version match. |
| `metadata` | `object` | `{}` | Additional metadata merged into every event's metadata envelope. |
| `callback` | `function(commit)` | — | Called after all events have been persisted. |

Throws `OptimisticConcurrencyError` when the stream is not at `expectedVersion`.

---

#### `getStreamVersion(streamName)` ✅ Stable

```javascript
eventstore.getStreamVersion(streamName) → number
```

Return the current version (event count) of a stream, or `-1` if the stream does not exist.

---

#### `getEventStream(streamName, [minRevision], [maxRevision], [predicate], [raw])` ✅ Stable

```javascript
eventstore.getEventStream(streamName [, minRevision [, maxRevision [, predicate [, raw]]]]) → EventStream | false
```

Return an `EventStream` for the named stream, or `false` if no such stream exists. `minRevision` and `maxRevision` are 1-based and inclusive; negative values count from the end.

- `predicate` supports function and object matchers.
- `raw=true` returns newline-delimited JSON `Buffer` chunks (NDJSON) for direct network streaming.

---

#### `getAllEvents([minRevision], [maxRevision], [predicate], [raw])` ✅ Stable

```javascript
eventstore.getAllEvents([minRevision [, maxRevision [, predicate [, raw]]]]) → EventStream
```

Return an `EventStream` covering every event in the store across all streams.  Equivalent to `getEventStream('_all', ...)`.

---

#### `getEventStreamForCategory(categoryName, [minRevision], [maxRevision], [predicate], [raw])` ✅ Stable

```javascript
eventstore.getEventStreamForCategory(categoryName [, minRevision [, maxRevision [, predicate [, raw]]]]) → EventStream
```

Return a joined `EventStream` for all streams whose names begin with
`categoryName + '-'`.  If a dedicated physical stream named `categoryName`
already exists, that stream is returned directly.

Throws if no streams for the category exist.

---

#### `createEventStream(streamName, matcher)` ✅ Stable

```javascript
eventstore.createEventStream(streamName, matcher) → EventStream
```

Create a new named stream backed by an index that includes every event matching `matcher`. `matcher` can be:

- a plain object using nested equality,
- array values for OR semantics,
- scalar comparison operators (`$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`), or
- a predicate `(event) => boolean`.

For the full object-matcher syntax and raw-mode caveats, see [Event Streams -> Object Matcher Syntax](streams.md#object-matcher-syntax).

Returns an `EventStream` over the pre-existing matching events.

Throws if a stream with that name already exists, or if the store is read-only.

---

#### `deleteEventStream(streamName)` ✅ Stable

```javascript
eventstore.deleteEventStream(streamName)
```

Delete the index for the named stream.  Does nothing if the stream does not exist.  Existing events are not removed; the index will be rebuilt on the next write to that stream name.

Throws if the store is read-only.

---

#### `closeEventStream(streamName)` ✅ Stable

```javascript
eventstore.closeEventStream(streamName)
```

Permanently seal a stream so that no new events are indexed into it.  The stream remains readable; any attempt to write to it will throw.  The closure is persisted by renaming the index file (e.g. `stream-foo.index` → `stream-foo.closed.index`).

Throws if the store is read-only, the stream does not exist, or the stream is already closed.

---

#### `preCommit(hook)` ✅ Stable

```javascript
eventstore.preCommit(hook)
```

Register a hook called synchronously before each event is persisted.  The hook receives `(event, partitionMetadata)` and may throw to abort the write.  Equivalent to `eventstore.on('preCommit', hook)`.

Throws if the store is read-only.

---

#### `preRead(hook)` ✅ Stable

```javascript
eventstore.preRead(hook)
```

Register a hook called synchronously before each event is read.  The hook receives `(position, partitionMetadata)` and may throw to abort the read.  Equivalent to `eventstore.on('preRead', hook)`.

---

#### `length` ✅ Stable

```javascript
eventstore.length → number
```

Total number of events in the store.

---

#### `on(event, listener)` / `addListener(event, listener)`

```javascript
eventstore.on(event, listener) → this
```

Register an event listener.  For `'preCommit'` and `'preRead'` events the listener is forwarded to the underlying storage.  All other events are handled by the standard `EventEmitter`.

---

#### `once(event, listener)`

```javascript
eventstore.once(event, listener) → this
```

Like `on()` but the listener is invoked at most once.

---

#### `off(event, listener)` / `removeListener(event, listener)`

```javascript
eventstore.off(event, listener) → this
```

Remove a previously registered listener.

---

#### `fromStreams(streamName, streamNames, [minRevision], [maxRevision], [predicate], [raw])`

```javascript
eventstore.fromStreams(streamName, streamNames [, minRevision [, maxRevision [, predicate [, raw]]]]) → EventStream
```

Create a virtual `EventStream` by joining the listed streams in sequence-number order.

| Parameter | Type | Description |
|-----------|------|-------------|
| `streamName` | `string` | Transient name for the joined stream. |
| `streamNames` | `Array<string>` | Names of the streams to join. |

Throws if any named stream does not exist.

---

#### `getConsumer(streamName, identifier, [initialState], [since])`

```javascript
eventstore.getConsumer(streamName, identifier [, initialState [, since]]) → Consumer
```

Return a durable `Consumer` that tracks its position across process restarts.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `streamName` | `string` | — | Stream to consume, or `'_all'` for all events. |
| `identifier` | `string` | — | Unique consumer name (used for the state file). |
| `initialState` | `object` | `{}` | Initial consumer state. |
| `since` | `number` | `0` | Stream revision to start from. |

---

#### `scanConsumers(callback)`

```javascript
eventstore.scanConsumers(callback)
```

Asynchronously scan all consumer state files and return their identifiers.

`callback` signature: `(error, consumers: Array<string>)`.

---

#### `consumer.createProjection(projectionFn, [options])`

```javascript
consumer.createProjection(projectionFn [, options])
```

Register a reducer-style projection as the consumer's `'data'` handler and persist it so it is auto-restored when reopening the same consumer.

`projectionFn` can be either:

- a reducer function: `(state, event) => state`
- an object map: `{ [eventType]: (state, event) => state }`

Options:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `options.hmac` | `function(string): string` | storage HMAC | HMAC function used to sign/verify serialized projections. Required when no storage-level HMAC is configured. |

---

### Events emitted

| Event | Payload | Description |
|-------|---------|-------------|
| `'ready'` | — | Emitted once the store is open and indexes are consistent. |
| `'commit'` | `commit` | Emitted after each successful `commit()` call. |
| `'stream-created'` | `streamName` | Emitted when a new stream index is created. |
| `'stream-available'` | `streamName` | Emitted when an existing stream is discovered on disk. |
| `'stream-deleted'` | `streamName` | Emitted after `deleteEventStream()`. |
| `'stream-closed'` | `streamName` | Emitted after `closeEventStream()`. |
| `'unfinished-commit'` | `lastEvent` | Emitted when a partial commit is detected on open. |

---

### Static properties

| Property | Value | Description |
|----------|-------|-------------|
| `EventStore.ExpectedVersion.Any` | `-1` | Skip version check. |
| `EventStore.ExpectedVersion.EmptyStream` | `0` | Assert the stream is new. |
| `EventStore.OptimisticConcurrencyError` | `Error` | Thrown when an expected-version check fails. |
| `EventStore.LOCK_THROW` | constant | Throw if storage lock is held (default). |
| `EventStore.LOCK_RECLAIM` | constant | Forcefully reclaim storage lock on open. |

---

## EventStream

`EventStream` is returned by `EventStore.getEventStream()` and related methods.

```javascript
import { EventStream } from 'event-storage';
```

`EventStream` extends Node's `stream.Readable`.

### Constructor

```javascript
new EventStream(name, eventStore, [minRevision], [maxRevision], [predicate], [raw])
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `string` | — | Stream name (must exist in `eventStore`). |
| `eventStore` | `EventStore` | — | The owning `EventStore`. |
| `minRevision` | `number` | `1` | First event revision to include (1-based, inclusive). Negative values count from the end. |
| `maxRevision` | `number` | `-1` | Last event revision to include (inclusive). `-1` means the last event. |
| `predicate` | `function\|object\|null` | `null` | Optional matcher. In object mode functions receive `(payload, metadata)`. In raw mode functions receive `(buffer)`. |
| `raw` | `boolean` | `false` | Emit NDJSON `Buffer` chunks instead of payload objects. |

In practice, `EventStream` instances are obtained from `EventStore` methods rather than constructed directly.

### Methods

#### `from(revision)` ✅ Stable

```javascript
stream.from(revision) → this
```

Set the starting revision (1-based, inclusive; negative counts from the end).

---

#### `until(revision)` ✅ Stable

```javascript
stream.until(revision) → this
```

Set the ending revision (inclusive; negative counts from the end).

---

#### `fromStart()` ✅ Stable

```javascript
stream.fromStart() → this
```

Reset the start position to the first event in the stream.

---

#### `fromEnd()` ✅ Stable

```javascript
stream.fromEnd() → this
```

Set the start position to the last event in the stream.

---

#### `toEnd()` ✅ Stable

```javascript
stream.toEnd() → this
```

Set the end position to the last event in the stream.

---

#### `toStart()` ✅ Stable

```javascript
stream.toStart() → this
```

Set the end position to the first event in the stream.

---

#### `first(amount)` ✅ Stable

```javascript
stream.first(amount) → this
```

Limit the stream to the first `amount` events in chronological order.

---

#### `last(amount)` ✅ Stable

```javascript
stream.last(amount) → this
```

Limit the stream to the last `amount` events in chronological order.

---

#### `forwards([amount])` ✅ Stable

```javascript
stream.forwards([amount]) → this
```

Read the current range in forward (chronological) order.  If `amount` is given, advance the end of the range by `amount` events from the current start.

---

#### `backwards([amount])` ✅ Stable

```javascript
stream.backwards([amount]) → this
```

Read the current range in backward (reverse-chronological) order.  If `amount` is given, extend the range `amount` events backwards from the current start.

---

#### `forEach(callback)` ✅ Stable

```javascript
stream.forEach(callback)
```

Iterate over every event, providing access to both the event payload and its storage metadata.

`callback` signature: `(event, metadata, streamName)`.

---

#### `previous(amount)`

```javascript
stream.previous(amount) → this
```

Move the end of the range `amount` events before the current start (used internally by `last()` / `backwards()`).

---

#### `following(amount)`

```javascript
stream.following(amount) → this
```

Move the end of the range `amount` events after the current start (used internally by `first()` / `forwards()`).

---

#### `reverse()`

```javascript
stream.reverse() → this
```

Swap `minRevision` and `maxRevision`, reversing the read direction.

---

#### `events`

```javascript
stream.events → Array<object>
```

Return all events in the current range as an array (event payloads only).  The result is cached after the first access; call `reset()` to clear the cache.

---

#### `[Symbol.iterator]()`

```javascript
for (const event of stream) { … }
```

Make the stream iterable in a `for…of` loop, yielding event payloads.

---

#### `reset()`

```javascript
stream.reset() → this
```

Reset the internal iterator and event cache so the stream can be iterated again from scratch.

---

#### `next()`

```javascript
stream.next() → { payload, metadata, stream } | false
```

Return the next event object from the iterator, or `false` when the stream is exhausted.

---

### Raw Streaming (NDJSON)

Use raw mode when you want to stream events directly over sockets/HTTP without deserializing and re-serializing every event in userland.

```javascript
const stream = eventstore.getEventStream('orders', 1, -1, { payload: { type: 'OrderPlaced' } }, true);
stream.pipe(httpResponse);
```

Matcher behavior differs by mode:

- **Object mode (`raw=false`)**
  - Function matcher: `(payload, metadata) => boolean`
  - Object matcher: evaluated against `{ stream, payload, metadata }`
- **Raw mode (`raw=true`)**
  - Function matcher: `(buffer) => boolean` where `buffer` is one compact JSON document
  - Object matcher: byte-level raw matcher over compact JSON bytes (lazy-built at first stream consumption)

Object matchers support nested equality, array values with OR semantics, and scalar comparison operators (`$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`). Multiple operators on the same field are combined with AND semantics.

Prefer plain equality over `$eq` when possible (`{ type: 'Foo' }` instead of `{ type: { $eq: 'Foo' } }`).

Operator matching is intended for scalar values. For arrays, objects, or custom raw encodings, use plain equality or a function matcher.

The raw object matcher requires the default compact JSON serializer format. If you use a custom serializer (including pretty-printed or transformed JSON), object matchers in raw mode are not guaranteed to work; use a function matcher in that case.

---

## Storage

`Storage` is the low-level append-only document store used internally by `EventStore`.  It can be used directly for advanced use cases.

```javascript
import { Storage } from 'event-storage';

// Writable storage (default export)
const store = new Storage('mystore', { dataDirectory: './data' });
store.open();

// Read-only storage
const reader = new Storage.ReadOnly('mystore', { dataDirectory: './data' });
reader.open();
```

`Storage` (writable) extends `Storage.ReadOnly` which in turn extends `EventEmitter`.

### Constructor — `Storage` (writable)

```javascript
new Storage([storageName], [config])
```

Inherits all options from `Storage.ReadOnly` plus:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `storageName` | `string` | `'storage'` | Base name for all storage files. |
| `config.dataDirectory` | `string` | `'.'` | Directory for data files. |
| `config.indexDirectory` | `string` | `config.dataDirectory` | Directory for index files. |
| `config.indexFile` | `string` | `'{storageName}.index'` | File name of the primary index. |
| `config.serializer` | `object` | JSON | Object with `serialize(doc)` and `deserialize(data)` methods. |
| `config.readBufferSize` | `number` | `4096` | Read buffer size in bytes. |
| `config.writeBufferSize` | `number` | `16384` | Write buffer size in bytes. |
| `config.maxWriteBufferDocuments` | `number` | `0` (unlimited) | Maximum number of buffered documents before an automatic flush. |
| `config.syncOnFlush` | `boolean` | `false` | Call `fsync` on flush for strict durability. |
| `config.dirtyReads` | `boolean` | `true` | Allow reading documents that are still in the write buffer. |
| `config.partitioner` | `function(doc, seqNum)` | single partition | Returns the partition name for a document. |
| `config.indexOptions` | `object` | `{}` | Options forwarded to every index on construction. |
| `config.hmacSecret` | `string` | `''` | Secret used to verify stored matcher HMACs. |
| `config.metadata` | `object\|function(partitionName)` | `{}` | Metadata written to each partition header. |
| `config.lock` | `LOCK_THROW\|LOCK_RECLAIM` | `LOCK_THROW` | How to handle an existing lock file on open. |

### Constructor — `Storage.ReadOnly`

```javascript
new Storage.ReadOnly([storageName], [config])
```

Accepts the same options as `Storage` (writable) except for write-specific fields (`writeBufferSize`, `maxWriteBufferDocuments`, `syncOnFlush`, `dirtyReads`, `partitioner`, `lock`).  In addition to the read API it watches the data directory for new partitions and index files created by a concurrent writer.

### Shared methods (readable and writable)

#### `open()` ✅ Stable

```javascript
storage.open() → boolean
```

Open the storage and all loaded indexes.  Emits `'opened'`.  The writable variant also acquires a lock; throws `StorageLockedError` if the storage is already locked by another process (unless `config.lock = LOCK_RECLAIM`).

---

#### `close()` ✅ Stable

```javascript
storage.close()
```

Close the storage and release all resources (file handles, indexes, write buffers, file watcher).  Emits `'closed'`.

---

#### `preRead(hook)` ✅ Stable

```javascript
storage.preRead(hook)
```

Register a synchronous hook called before every document read.  The hook receives `(position, partitionMetadata)` and may throw to abort the read.  Equivalent to `storage.on('preRead', hook)`.

---

#### `read(number, [index])` ✅ Stable

```javascript
storage.read(number [, index]) → object | false
```

Read a single document by its 1-based position inside the given index (or the primary index when omitted).  Returns `false` when the position is out of range.

---

#### `readRange(from, [until], [index])` ✅ Stable

```javascript
storage.readRange(from [, until [, index]]) → Generator<object>
```

Return a generator that yields documents in the range `[from, until]` (1-based, inclusive).  Negative values count from the end.  Pass `index = false` to bypass the primary index and iterate partitions directly in sequence-number order.

If `from > until` (after normalisation) the range is yielded in reverse order.

---

#### `openReadonlyIndex(name)` ✅ Stable

```javascript
storage.openReadonlyIndex(name) → ReadableIndex
```

Open an existing index file that carries a status marker in its name (e.g. `stream-foo.closed`) without registering it in the secondary-index write path.

Throws if the index file does not exist.

---

#### `openIndex(name, [matcher])` ✅ Stable

```javascript
storage.openIndex(name [, matcher]) → ReadableIndex
```

Open an existing secondary index for reading.  If `matcher` is provided its HMAC is validated against the value stored in the index metadata.

Throws if the index does not exist or if the HMAC validation fails.

---

#### `length`

```javascript
storage.length → number
```

Number of documents in the primary index.

---

### Writable-only methods

#### `preCommit(hook)` ✅ Stable

```javascript
storage.preCommit(hook)
```

Register a synchronous hook called before each document is written.  The hook receives `(document, partitionMetadata)` and may throw to abort the write.  Equivalent to `storage.on('preCommit', hook)`.

---

#### `write(document, [callback])` ✅ Stable

```javascript
storage.write(document [, callback]) → number
```

Append a document to storage and return its 1-based sequence number.

`callback` is invoked once the document (and its index entry) has been flushed to disk.

---

#### `ensureIndex(name, [matcher])` ✅ Stable

```javascript
storage.ensureIndex(name [, matcher]) → ReadableIndex
```

Return an existing secondary index by name, or create it if it does not exist.  When creating, `matcher` is required and can be either a property-equality object or a predicate `(document) => boolean`.

Throws if the index doesn't exist and no `matcher` was given.

---

#### `flush()` ✅ Stable

```javascript
storage.flush() → boolean
```

Flush all write buffers for all partitions and indexes to disk synchronously.  Returns `true` if any data was actually written.

---

#### `reindex([fromSequenceNumber])` ✅ Stable

```javascript
storage.reindex([fromSequenceNumber])
```

Rebuild the primary index and all currently loaded secondary indexes by scanning partition files directly, starting from `fromSequenceNumber` (default `0` = full rebuild).

---

#### `unlock()`

```javascript
storage.unlock()
```

Forcefully remove the lock file, regardless of which process created it.  Only call this when you are certain no other process has the storage open for writing.

---

#### `lock()`

```javascript
storage.lock() → boolean
```

Attempt to acquire the write lock.  Returns `false` if this instance already holds the lock, and throws `StorageLockedError` if another process holds it.  Called automatically by `open()`.

---

#### `truncate(after)`

```javascript
storage.truncate(after)
```

Truncate all partitions and indexes after the given 1-based sequence number.  Negative values count from the end.  Used internally for crash recovery; use with care.

---

#### `checkTornWrites()`

```javascript
storage.checkTornWrites()
```

Scan every partition for torn writes (documents that extend beyond the end of their file), repair the affected partitions, and rebuild any lagging index entries.  Called automatically by `open()` / `unlock()` when a previous writer did not close cleanly.

---

#### `forEachDistinctPartitionOf(entries, iterationHandler)`

```javascript
storage.forEachDistinctPartitionOf(entries, iterationHandler)
```

Iterate the unique partitions referenced by an iterable list of index entries, invoking `iterationHandler(entry)` once per distinct partition.

---

### ReadOnly-only methods

#### `storageFilesFilter(filename)`

```javascript
storage.storageFilesFilter(filename) → boolean
```

Return `true` when `filename` belongs to this storage instance (used internally as the file-watcher filter callback).

---

### Events emitted by Storage

| Event | Payload | Description |
|-------|---------|-------------|
| `'opened'` | — | Emitted after `open()` completes. |
| `'closed'` | — | Emitted after `close()` completes. |
| `'ready'` | — | Emitted after `open()` on the writable storage. |
| `'wrote'` | `document, entry, indexPosition` | Emitted after each document is indexed. |
| `'index-created'` | `name` | Emitted when a new secondary index is created. |
| `'index-add'` | `name, length, document` | Emitted when a document is added to a secondary index. |
| `'partition-created'` | `id` | Emitted when a new partition file is opened. |
| `'truncate'` | `prevLength, newLength` | Emitted (read-only storage) when the primary index is truncated by the writer. |
