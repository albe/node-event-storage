# Advanced Topics

## ACID Properties

> All of the following applies to a single transaction boundary, which is a single write stream (storage partition).

### Atomicity

A single event write is atomic at the JSON level — an incomplete write cannot be deserialized. For custom serialization formats (msgpack, protobuf, …) you should add a checksum to detect partial writes.

A multi-event commit is atomic end-to-end: on startup the store checks whether the last `commitId` matches the `commitSize`. If not, the incomplete commit is rolled back by truncating the storage to the position before it started.

Write-buffer batching also means logical atomicity can span multiple events. Control this with:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxWriteBufferDocuments` | `number` | unlimited | Maximum number of events in the write buffer before an automatic flush. |
| `writeBufferSize` | `number` | `16384` (16 KB) | Maximum byte size of the write buffer. |

For optimal I/O, avoid limiting `maxWriteBufferDocuments` so that the buffer fills to the page size (typically 4 KB) before being flushed.

### Consistency

The append-only nature guarantees consistency for all successful writes. Torn writes (partial flushes caused by a crash) are detected on startup and truncated when the lock is reclaimed:

```javascript
const eventstore = new EventStore('my-event-store', {
    storageConfig: { lock: EventStore.LOCK_RECLAIM }
});
```

> **Warning:** `LOCK_RECLAIM` bypasses the single-writer lock. Do not use it when two processes might write concurrently.

### Isolation

- Only a single writer is permitted per store (enforced by a lock directory).
- Reads are always isolated: a read only sees writes that have already completed at the moment the stream is retrieved (MVCC).
- *Dirty reads* (reading events from the write buffer before they are flushed) are enabled by default for writers. To disable:

```javascript
const eventstore = new EventStore('my-event-store', {
    storageConfig: { dirtyReads: false }
});
```

Disabling dirty reads is only recommended for in-memory models that maintain their own uncommitted state.

### Durability

Strict durability is intentionally traded for write performance:

- Events are buffered in memory before being flushed to disk.
- Flush does **not** call `fsync` by default, so OS/disk write-cache can still absorb and potentially lose recent writes on a crash.

To enable strict durability (at significant performance cost):

```javascript
const eventstore = new EventStore('my-event-store', {
    storageConfig: { syncOnFlush: true }
});
```

## Storage Configuration

Pass these options inside `config.storageConfig`:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `dataDirectory` | `string` | `storageDirectory` | Where partition (event) files are stored. |
| `indexDirectory` | `string` | `storageDirectory/streams` | Where index files are stored. |
| `partitioner` | `function` | stream-per-event | `(document, sequenceNumber) => partitionName`. |
| `serializer` | `object` | JSON | `{ serialize(doc), deserialize(string) }` — custom serialization. |
| `writeBufferSize` | `number` | `16384` | Write-buffer size in bytes. |
| `maxWriteBufferDocuments` | `number` | unlimited | Maximum events per write buffer. |
| `syncOnFlush` | `boolean` | `false` | Call `fsync` on every flush. |
| `dirtyReads` | `boolean` | `true` | Allow reading from the unflushed write buffer. |
| `lock` | `symbol` | strict lock | Pass `EventStore.LOCK_RECLAIM` to reclaim stale locks and auto-repair indexes. |
| `hmacSecret` | `string` | `''` | Secret used to fingerprint persisted matcher functions (see [Security](#security)). |
| `metadata` | `function` | — | `(partitionName) => object` — per-partition metadata (lower-level alternative to `config.streamMetadata`). |

## Global Order

The storage maintains a global primary index that gives every event a monotonically increasing sequence number. This guarantees a consistent order when reading across multiple write streams (e.g. in joined streams or category streams).

Since version 0.7 a monotonic clock stamp and an external sequence number are also stored with each event, so a consistent global order can be reconstructed without the global index if needed.

## Reindexing

If the index becomes inconsistent with the data files (e.g. after manual recovery), rebuild it:

```javascript
eventstore.on('ready', () => {
    // Full rebuild
    eventstore.storage.reindex(0);

    // Partial rebuild from a known-good checkpoint
    eventstore.storage.reindex(1000);
});
```

### Automatic Crash Recovery

With `LOCK_RECLAIM`, the store detects on startup that the primary index lags behind the data and calls `reindex()` automatically. The `'ready'` event fires only after repair is complete:

```javascript
const eventstore = new EventStore('my-event-store', {
    storageDirectory: './data',
    storageConfig: { lock: EventStore.LOCK_RECLAIM }
});

eventstore.on('ready', () => {
    // Fully repaired — safe to read and write
});
```

> **Performance note:** `reindex()` performs a full (or partial) partition scan. For large stores prefer `reindex(knownGoodPosition)` over `reindex(0)`.

## Partitioning

By default each write stream maps to its own file (one partition per stream). This maximises locality and buffer efficiency.

You can override partitioning with the `partitioner` option — a function `(document, sequenceNumber) => partitionName`:

```javascript
// Fixed number of hash partitions
const MAX_PARTITIONS = 8;
const eventstore = new EventStore('my-event-store', {
    storageConfig: {
        partitioner: (doc, seq) => 'partition-' + (seq % MAX_PARTITIONS)
    }
});
```

```javascript
// Time-based chunking (one partition per day)
const eventstore = new EventStore('my-event-store', {
    storageConfig: {
        partitioner: (doc) => {
            const date = new Date(doc.committedAt);
            return `chunk-${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`;
        }
    }
});
```

> **Note:** Non-default partitioning breaks the per-stream consistency boundary. Use with care.

## Custom Serialization

Replace the default JSON serializer with any `serialize`/`deserialize` pair:

```javascript
const { encode, decode } = require('@msgpack/msgpack');

const eventstore = new EventStore('my-event-store', {
    storageConfig: {
        serializer: {
            serialize: (doc) => {
                const encoded = encode(doc);
                return Buffer.from(encoded.buffer, encoded.byteOffset, encoded.byteLength).toString('binary');
            },
            deserialize: (string) => decode(Buffer.from(string, 'binary'))
        }
    }
});
```

`@msgpack/msgpack` is often faster than `JSON.parse` for deserialization, while producing smaller files, but makes the storage files non-human-readable.

## Compression

Use the `serializer` option to wrap events in a compression codec.

### LZ4 Example

```javascript
const lz4 = require('lz4');

const eventstore = new EventStore('my-event-store', {
    storageConfig: {
        serializer: {
            serialize: (doc) =>
                lz4.encode(Buffer.from(JSON.stringify(doc))).toString('binary'),
            deserialize: (string) =>
                JSON.parse(lz4.decode(Buffer.from(string, 'binary')))
        }
    }
});
```

> **Note:** Compression operates per-event, which reduces compression efficiency compared to block compression. To improve ratios, provide the codec with a dictionary pre-filled with common field names such as:
>
> - `"metadata":{"commitId":`
> - `,"committedAt":`
> - `,"commitVersion":`
> - `,"commitSize":`
> - `,"streamVersion":`

## Security

Matcher functions passed to `createStream` are serialized into the index file and later `eval`'d on load for convenience (so you don't have to re-specify them every time). To prevent a tampered index file from executing arbitrary code, every persisted matcher is fingerprinted with an HMAC.

**Always set a strong, random `hmacSecret` in production:**

```javascript
const eventstore = new EventStore('my-event-store', {
    storageConfig: {
        hmacSecret: process.env.EVENT_STORE_HMAC_SECRET
    }
});
```

Alternatively, always pass the matcher explicitly when opening a stream — the store will verify the supplied matcher matches the one stored in the index:

```javascript
// Explicitly re-supply the matcher — the store verifies it matches
const stream = eventstore.getEventStream('my-projection-stream');
```

## Access Control Hooks

Each stream (partition) can carry an arbitrary metadata object that is written once into the partition file header at creation time. Two hooks let you intercept every read and write:

- **`preCommit`** — called with `(event, partitionMetadata)` *before* a write. Throw to abort.
- **`preRead`** — called with `(position, partitionMetadata)` *before* a read. Throw to abort.

Both hooks run synchronously on every operation. Keep handler logic cheap — avoid I/O and async work.

### EventStore Level

```javascript
const EventStore = require('event-storage');

const globalContext = { authorizedRoles: ['user'] };

const eventstore = new EventStore('my-event-store', {
    storageDirectory: './data',
    // Called once per stream at creation; result is persisted in the file header
    streamMetadata: (streamName) => ({
        allowedRoles: streamName === 'admin-stream' ? ['admin'] : ['user']
    })
});

eventstore.on('ready', () => {
    eventstore.on('preCommit', (event, meta) => {
        if (!meta.allowedRoles.some(r => globalContext.authorizedRoles.includes(r))) {
            throw new Error('Not authorized to write to this stream');
        }
    });

    eventstore.on('preRead', (position, meta) => {
        if (!meta.allowedRoles.some(r => globalContext.authorizedRoles.includes(r))) {
            throw new Error('Not authorized to read from this stream');
        }
    });

    // Succeeds — 'user' role is allowed
    eventstore.commit('user-stream', [{ type: 'UserCreated', id: 1 }], 0);

    // Throws — caller does not have 'admin' role
    eventstore.commit('admin-stream', [{ type: 'AdminAction' }], 0);
});
```

Use `eventstore.preCommit(handler)` / `eventstore.preRead(handler)` as convenient shorthand for `eventstore.on('preCommit', handler)` / `eventstore.on('preRead', handler)`.

### Storage Level

If you use the `Storage` class directly:

```javascript
const Storage = require('event-storage').Storage;

const storage = new Storage('events', {
    partitioner: (doc) => doc.stream,
    metadata: (partitionName) => ({
        allowedRoles: partitionName === 'admin' ? ['admin'] : ['user']
    })
});

storage.on('preCommit', (document, meta) => { /* ... */ });
storage.on('preRead',   (position, meta)  => { /* ... */ });
```
