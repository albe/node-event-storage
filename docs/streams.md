# Event Streams

An event stream is an ordered sequence of events that can be iterated. Every stream is backed by a lightweight index file and implements the JavaScript [Iterable](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols) protocol as well as the Node.js `ReadableStream` interface.

## Writing Events (Commits)

Events are appended to a named stream with `commit`. The stream is created automatically the first time you write to it.

```javascript
eventstore.commit('user-123', [
    { type: 'UserRegistered', email: 'alice@example.com' },
    { type: 'ProfileCompleted', name: 'Alice' }
], EventStore.ExpectedVersion.EmptyStream, (err) => {
    if (err) throw err;
    console.log('Committed!');
});
```

A single `commit` call is the unit of atomicity: either all events in the array are persisted or none of them are.

### Optimistic Concurrency

When multiple sources produce events concurrently (e.g. multiple HTTP requests for the same aggregate), you need optimistic concurrency control to prevent conflicting writes.

The pattern is:

1. Read the stream and note its current `version`.
2. Build new events based on the current state.
3. Commit with `expectedVersion` set to the version noted in step 1.

If another writer has committed in the meantime the version will have advanced and an `OptimisticConcurrencyError` will be thrown. You then replay state and retry.

```javascript
const model = new MyAggregateModel();
const stream = eventstore.getEventStream('order-42');
stream.forEach((event) => model.apply(event));
const expectedVersion = stream.version;

// ... later, handling an incoming command:
const newEvents = model.handle(command);
try {
    eventstore.commit('order-42', newEvents, expectedVersion, () => {
        // success
    });
} catch (e) {
    if (e instanceof EventStore.OptimisticConcurrencyError) {
        // Replay and retry, or return a conflict error to the caller
    }
}
```

`expectedVersion` values:

| Value | Meaning |
|-------|---------|
| `EventStore.ExpectedVersion.Any` (`-1`) | No check — always succeeds (default). |
| `EventStore.ExpectedVersion.EmptyStream` (`0`) | Stream must not exist yet. |
| Positive integer | Stream must be at exactly that version. |

## Reading Streams

### Basic Iteration

```javascript
const stream = eventstore.getEventStream('user-123');
for (const event of stream) {
    console.log(event.type);
}

// Or with forEach, which also gives you metadata:
stream.forEach((event, metadata, streamName) => {
    console.log(event.type, metadata.streamVersion);
});
```

### Revision Ranges

Retrieve only a slice of the stream by passing `minRevision` and `maxRevision`:

```javascript
const all       = eventstore.getEventStream('user-123', 1, -1);  // all events
const first50   = eventstore.getEventStream('user-123', 1, 50);  // events 1–50
const last10    = eventstore.getEventStream('user-123', -10, -1); // last 10 events
const reversed  = eventstore.getEventStream('user-123', -1, -10); // last 10, newest first
```

Passing a negative `maxRevision` that is less than `minRevision` causes the stream to iterate in **reverse order**.

### Fluent API

Since version 0.9 there is a more readable fluent builder:

```javascript
// All events, newest first
eventstore.getEventStream('user-123').backwards();

// First 10 events
eventstore.getEventStream('user-123').first(10);
// equivalent:
eventstore.getEventStream('user-123').fromStart().forwards(10);

// Last 10 events (oldest first)
eventstore.getEventStream('user-123').last(10);

// Last 10 events, newest first
eventstore.getEventStream('user-123').last(10).backwards();

// Events from revision 16 onwards
eventstore.getEventStream('user-123').from(16).toEnd();

// Events from the start up to revision 10 (inclusive), newest first
eventstore.getEventStream('user-123').from(10).toStart();

// 10 events starting at revision 5
eventstore.getEventStream('user-123').from(5).forwards(10);

// Events between revisions 9 and 5 (reverse)
eventstore.getEventStream('user-123').from(9).until(5);
```

> **Note:** The revision boundary is fixed when you call `getEventStream()`. Events appended after that call will not appear in the iterator. If you need live updates, use [Consumers](consumers.md).

## Creating Additional Streams

You can create derived read streams that contain only a filtered subset of another stream, or even events from multiple streams combined.

```javascript
// Only events whose type is 'FooHappened' or 'BarHappened'
const projectionStream = eventstore.createStream(
    'my-projection-stream',
    (event) => ['FooHappened', 'BarHappened'].includes(event.type)
);

for (const event of projectionStream) {
    // ...
}
```

The matcher function is persisted in the index file so you do not need to re-specify it when reopening the store. See [Security](advanced.md#security) for how the matcher is protected against tampering.

## Joining Streams

Iterate events from multiple streams in their **global insertion order** using `fromStreams`:

```javascript
const joined = eventstore.fromStreams(
    'transient-join',       // A temporary name for this join — not persisted
    ['order-42', 'order-99']
);

for (const event of joined) {
    // Events from both streams, ordered by when they were written globally
}
```

The result is not persisted and cannot be used with consumers. For frequently-needed joins, create a permanent derived stream with `createStream` instead.

## Stream Categories

Name your streams as `<category>-<identity>` (e.g. `user-123`, `user-456`) to take advantage of category-level queries:

```javascript
eventstore.commit('user-' + user.id, [new UserRegistered(user.id, user.email)]);

// ...later, iterate all user events across all user instances:
const allUsersStream = eventstore.getEventStreamForCategory('user');
for (const event of allUsersStream) {
    // events from user-1, user-2, user-42, ... in global order
}
```

If you already created a dedicated stream for the category (e.g. via `createStream`), that stream is returned directly.

## Event Metadata

Every event carries storage-level metadata alongside the event payload. Access it through `forEach`:

```javascript
stream.forEach((event, metadata, streamName) => {
    console.log(metadata);
    // {
    //   commitId: '…',         unique ID for the whole commit
    //   committedAt: 1700000000000,  ms timestamp
    //   commitVersion: 1,      sequence number within the commit (1-based)
    //   commitSize: 2,         total events in the commit
    //   streamVersion: 5       version of this event within the stream
    // }
});
```

You can also pass extra metadata through when committing, and it will be merged into this object:

```javascript
eventstore.commit('my-stream', [{ type: 'Foo' }], EventStore.ExpectedVersion.Any, () => {}, {
    causationId: 'cmd-123',
    correlationId: 'req-456'
});
```
