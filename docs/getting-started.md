# Getting Started

## Installation

Install the package from npm:

```bash
npm install event-storage
```

!!! note "CommonJS users"
    Version 1.0 is ESM-only (`import` syntax). If your project uses `require()` and migrating to ESM is not currently an option, the **0.x series** is functionally equivalent and retains full CJS support:

    ```bash
    npm install event-storage@0
    ```



```javascript
import { EventStore } from 'event-storage';

const eventstore = new EventStore('my-event-store', { storageDirectory: './data' });

eventstore.on('ready', () => {
    // Write events
    eventstore.commit('my-stream', [{ type: 'UserRegistered', userId: 1, email: 'user@example.com' }], 0, () => {
        console.log('Events written!');
    });

    // Read events
    const stream = eventstore.getEventStream('my-stream');
    for (const event of stream) {
        console.log(event);
    }
});
```

The `EventStore` constructor takes an optional store name and a configuration object.
Emits `'ready'` once the store has opened and all indexes are consistent — you should always wait for this event before performing any reads or writes.

## Constructor Options

```javascript
new EventStore(storeName, config)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `storeName` | `string` | `'eventstore'` | Name of the store, used as a prefix for storage files. |
| `config.storageDirectory` | `string` | `'./data'` | Directory where all data files are stored. |
| `config.streamsDirectory` | `string` | `'{storageDirectory}/streams'` | Directory for stream index files. |
| `config.storageConfig` | `object` | `{}` | Lower-level options forwarded to the `Storage` backend. See [Advanced Options](advanced.md#storage-configuration). |
| `config.readOnly` | `boolean` | `false` | Open the store in read-only mode. See [Read-Only Mode](consumers.md#read-only-mode). |
| `config.streamMetadata` | `object\|function` | — | Metadata attached to each stream at creation time. Accepts a plain `{ streamName: metadataObj }` map or a function `(streamName) => object`. See [Access Control](advanced.md#access-control-hooks). |

## Writing Events

Use `eventstore.commit(streamName, events, [expectedVersion], [callback])` to append events to a stream.

```javascript
// Append with no concurrency check (default)
eventstore.commit('orders', [{ type: 'OrderPlaced', orderId: 42 }]);

// Append only if the stream is currently at version 3
eventstore.commit('orders', [{ type: 'OrderShipped', orderId: 42 }], 3, (err) => {
    if (err) console.error('Commit failed:', err.message);
});
```

The `expectedVersion` can be:

- `ExpectedVersion.Any` (`-1`) — no check (the default).
- `ExpectedVersion.EmptyStream` (`0`) — only succeeds when the stream has no events yet.
- Any positive integer — the stream must currently be at exactly that version.

An `OptimisticConcurrencyError` is thrown when the stream version does not match. See [Optimistic Concurrency](streams.md#optimistic-concurrency) for more details.

## Reading Events

Use `eventstore.getEventStream(streamName, [minRevision], [maxRevision])` to get a stream and iterate it.

```javascript
const stream = eventstore.getEventStream('orders');
for (const event of stream) {
    console.log(event.type, event.orderId);
}
```

For details on range queries, reverse iteration, fluent API and more, see [Event Streams](streams.md).

## Running Tests

```bash
npm test
```
