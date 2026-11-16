# Customization

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
import { encode, decode } from '@msgpack/msgpack';

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
import lz4 from 'lz4';

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

## Access Control Hooks

Each stream (partition) can carry an arbitrary metadata object that is written once into the partition file header at creation time. Two hooks let you intercept every read and write:

- **`preCommit`** — called with `(event, partitionMetadata)` *before* a write. Throw to abort.
- **`preRead`** — called with `(position, partitionMetadata)` *before* a read. Throw to abort.

Both hooks run synchronously on every operation. Keep handler logic cheap — avoid I/O and async work.

### EventStore Level

```javascript
import { EventStore } from 'event-storage';

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
import { Storage } from 'event-storage';

const storage = new Storage('events', {
    partitioner: (doc) => doc.stream,
    metadata: (partitionName) => ({
        allowedRoles: partitionName === 'admin' ? ['admin'] : ['user']
    })
});

storage.on('preCommit', (document, meta) => { /* ... */ });
storage.on('preRead',   (position, meta)  => { /* ... */ });
```

## Security

Matcher functions passed to `createEventStream` are serialized into the index file and later `eval`'d on load for convenience (so you don't have to re-specify them every time). To prevent a tampered index file from executing arbitrary code, every persisted matcher is fingerprinted with an HMAC.

**Always set a strong, random `hmacSecret` in production:**

```javascript
const eventstore = new EventStore('my-event-store', {
    storageConfig: {
        hmacSecret: process.env.EVENT_STORE_HMAC_SECRET
    }
});
```

Object matchers (e.g. `{ payload: { type: ['FooHappened', 'BarHappened'] } }`) are stored as JSON and never evaluated as code, so prefer them over function matchers for persisted streams.

When working with the `Storage` class directly, you can also re-supply the matcher when opening an index; the storage then verifies it against the stored HMAC instead of evaluating the persisted source:

```javascript
// Explicitly re-supply the matcher — the storage verifies it matches
const index = storage.openIndex(
    'stream-my-projection-stream',
    (document) => ['FooHappened', 'BarHappened'].includes(document.payload.type)
);
```
