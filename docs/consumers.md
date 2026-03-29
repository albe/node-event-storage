# Consumers

Consumers are durable, event-driven listeners attached to a stream. They implement the Node.js `stream.Readable` interface and provide **at-least-once delivery** guarantees: every event is delivered at least once. An event may be re-delivered if the process crashed during handling, because the consumer's position is only advanced *after* the handler returns.

## Basic Usage

```javascript
const consumer = eventstore.getConsumer('my-stream', 'my-consumer-id');

consumer.on('data', (event) => {
    console.log('Received event:', event.type);
    // position is advanced automatically after this handler returns
});
```

- Consuming starts as soon as the first `'data'` listener is attached and pauses when the last `'data'` listener is removed.
- Once the consumer has caught up to the end of the stream, it emits a `'caught-up'` event.
- The consumer will continue to receive new events as they are appended.

## Exactly-Once Semantics

Since version 0.6, consumers can persist their state alongside their position. Because state and position are updated in the same transaction, the state always reflects exactly the events processed — even across restarts.

```javascript
const consumer = eventstore.getConsumer('orders', 'orders-projection');

consumer.on('data', (event) => {
    // setState atomically advances position + persists state
    consumer.setState((state) => ({
        ...state,
        totalRevenue: state.totalRevenue + (event.amount || 0)
    }));
});
```

Whenever state is persisted the consumer emits a `'persisted'` event.

> **Important:** Never mutate `consumer.state` directly — always use `setState()`. Since version 0.8 the state object is frozen to enforce this.

> **Limitation:** Exactly-once applies only to operations you wrap together with the state update. For example, sending an email *and* persisting position cannot be made exactly-once without an external transaction coordinator.

## Initial State

Pass an initial state as the third argument to `getConsumer`:

```javascript
const consumer = eventstore.getConsumer('my-stream', 'my-consumer', {
    count: 0,
    lastSeen: null
});

consumer.on('data', (event) => {
    consumer.setState((state) => ({
        count: state.count + 1,
        lastSeen: event.type
    }));
});
```

## Updating State with a Function

Since version 0.8 `setState` accepts either a new state object or a function that receives the current state:

```javascript
// Pass a plain object
consumer.setState({ count: 0 });

// Pass a reducer function (preferred — avoids stale-closure bugs)
consumer.setState((state) => ({ ...state, count: state.count + 1 }));
```

## Resetting a Consumer

Force the consumer to reprocess events from a given position:

```javascript
// Reprocess from the beginning with a fresh initial state
consumer.reset();

// Reprocess from position 10, starting with a custom state
consumer.reset({ count: 0 }, 10);
```

## Consistency Guards (Aggregates)

A *consistency guard* (what DDD calls an *Aggregate*) is a consumer of its own stream that uses `setState` for exactly-once state tracking and raises an `OptimisticConcurrencyError` when two concurrent commands conflict.

```javascript
const guard = eventstore.getConsumer('order-42', 'order-42-guard');

// Apply events to keep the guard's state up-to-date
guard.on('data', (event) => {
    guard.setState((state) => applyEvent(state, event));
});

// Validate a command against the current state
function handlePlaceOrder(command) {
    // Throws if the business rule is violated
    validatePlaceOrder(command, guard.state);

    // Commit with the guard's current position as expectedVersion
    eventstore.commit(
        'order-42',
        [{ type: 'OrderPlaced', ...command }],
        command.expectedVersion ?? guard.position
    );
}
```

How this works:

1. The guard tracks state with `setState`, so it always reflects exactly the persisted events.
2. `validatePlaceOrder` checks business rules against that state.
3. If two concurrent requests both try to commit, the second `commit` will fail with `OptimisticConcurrencyError` because the stream version has already advanced.

> **Note:** Snapshotting is built-in. Restarting the process does not require replaying all events from scratch. To control snapshot frequency, pass a boolean as the second argument to `setState`:
>
> ```javascript
> consumer.setState(newState, this.position % 20 === 0); // snapshot every 20 events
> ```

## Read-Only Mode

Open the store in read-only mode to create consumers that run in a **separate process** from the writer:

```javascript
const EventStore = require('event-storage');

const eventstore = new EventStore('my-event-store', {
    storageDirectory: './data',
    readOnly: true
});

eventstore.on('ready', () => {
    const consumer = eventstore.getConsumer('my-stream', 'my-consumer');
    consumer.on('data', (event) => {
        // build a read model, serve it via HTTP, etc.
    });
});
```

- Writes are prevented; all reads and consumers work normally.
- The read-only store watches the underlying files and automatically picks up changes written by the writer process.
- Multiple read-only instances can run simultaneously.
- This enables a multi-process projection pattern: one writer process + N reader processes building different projections.

> In principle this also works across machines sharing a common filesystem (e.g. NFS), as long as the Node.js file watcher functions correctly on that filesystem.
