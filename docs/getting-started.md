# Getting Started

This tutorial builds a tiny order system in one file: it writes events, reads them back, handles a concurrent change and keeps a projection up to date. It takes about ten minutes.

## Installation

Requires Node.js 20 or later.

```bash
npm install event-storage
```

!!! note "CommonJS users"
    Version 1.0 is ESM-only (`import` syntax). If your project uses `require()` and migrating to ESM is not currently an option, the **0.x series** (last release 0.9.1) still supports CommonJS, but lacks the features added in 1.x such as DCB queries, matchers and raw streaming:

    ```bash
    npm install event-storage@0
    ```

## The complete example

Save this as `app.mjs` and run `node app.mjs`:

```javascript
import { EventStore, ExpectedVersion, OptimisticConcurrencyError } from 'event-storage';

const eventstore = new EventStore('shop', { storageDirectory: './data' });

eventstore.on('ready', () => {
    // 1. Write: open a new order stream
    eventstore.commit('order-42', [
        { type: 'OrderPlaced', orderId: 42, customer: 'alice' },
        { type: 'ItemAdded', orderId: 42, sku: 'book', price: 12 }
    ], ExpectedVersion.EmptyStream);

    // 2. Read: rebuild the current state of the order
    const stream = eventstore.getEventStream('order-42');
    let total = 0;
    for (const event of stream) {
        if (event.type === 'ItemAdded') total += event.price;
    }
    console.log(`order-42 is at version ${stream.version}, total ${total}`);

    // 3. Decide with optimistic concurrency
    try {
        eventstore.commit('order-42', [{ type: 'ItemAdded', orderId: 42, sku: 'pen', price: 2 }], stream.version);
        eventstore.commit('order-42', [{ type: 'OrderShipped', orderId: 42 }], stream.version); // stale version
    } catch (e) {
        if (e instanceof OptimisticConcurrencyError) {
            console.log('Conflict: someone else changed order-42, reload and retry');
        }
    }

    // 4. Project: a durable consumer over all events
    const revenue = eventstore.getConsumer('_all', 'revenue', { total: 0 });
    revenue.on('data', ({ payload }) => {
        if (payload.type === 'ItemAdded') {
            revenue.setState((state) => ({ total: state.total + payload.price }));
        }
    });
    revenue.on('caught-up', () => {
        console.log('Revenue so far:', revenue.state.total);
        eventstore.close();
    });
});
```

Output:

```
order-42 is at version 2, total 12
Conflict: someone else changed order-42, reload and retry
Revenue so far: 14
```

Running it a second time throws an `OptimisticConcurrencyError` at step 1, because `order-42` is no longer empty. Delete the `data` directory to start over.

## What happens

**Opening the store.** `new EventStore('shop', { storageDirectory: './data' })` creates or opens the store. Always wait for `'ready'` before reading or writing. All options are listed in [Configuration](configuration.md).

**1. Writing.** `commit(streamName, events, expectedVersion)` appends events atomically. The stream is created on its first commit. `ExpectedVersion.EmptyStream` makes the commit fail if the stream already has events. Errors are thrown synchronously; an optional last argument `(commit) => {}` is called once the events are flushed to disk.

**2. Reading.** `getEventStream()` returns an iterable over the event payloads. `stream.version` is the number of events it covers, which is what you pass as `expectedVersion` for the next decision.

**3. Concurrency.** The second commit passes the same, now outdated version, so it is rejected with `OptimisticConcurrencyError`. In a real application you reload the state and retry or report the conflict. See [Writing and Reading Events](reading-writing.md#optimistic-concurrency).

**4. Projection.** A consumer delivers every event of a stream (here `_all`, i.e. all events) to its `'data'` handler as a document `{ stream, payload, metadata }`, remembers its position across restarts and persists its state together with the position via `setState()`. `'caught-up'` fires once it has processed everything that exists. See [Consumers](consumers.md).

## Next steps

- [Writing and Reading Events](reading-writing.md): ranges, reverse reads, metadata, raw NDJSON streaming.
- [Organizing Streams](organizing-streams.md): categories, joined and derived streams.
- [Dynamic Consistency Boundaries](dcb.md): consistency checks across streams.
- [Guarantees](guarantees.md): what exactly is guaranteed after a crash.
