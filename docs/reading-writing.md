# Writing and Reading Events

An event stream is an ordered sequence of events that can be iterated. Every stream is backed by a lightweight index file and implements the JavaScript [Iterable](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols) protocol as well as the Node.js `ReadableStream` interface.

## Writing Events (Commits)

Events are appended to a named stream with `commit`. The stream is created automatically the first time you write to it.

```javascript
import { ExpectedVersion } from 'event-storage';

eventstore.commit('user-123', [
    { type: 'UserRegistered', email: 'alice@example.com' },
    { type: 'ProfileCompleted', name: 'Alice' }
], ExpectedVersion.EmptyStream, (commit) => {
    console.log('Committed and flushed:', commit.commitId);
});
```

A single `commit` call is the unit of atomicity: either all events in the array are persisted or none of them are. Validation and concurrency errors are thrown synchronously from `commit()`; the callback only runs after a successful flush and receives the `commit` object.

### Optimistic Concurrency

When multiple sources produce events concurrently (e.g. multiple HTTP requests for the same aggregate), you need optimistic concurrency control to prevent conflicting writes.

The pattern is:

1. Read the stream and note its current `version`.
2. Build new events based on the current state.
3. Commit with `expectedVersion` set to the version noted in step 1.

If another writer has committed in the meantime the version will have advanced and an `OptimisticConcurrencyError` will be thrown. You then replay state and retry.

```javascript
import { OptimisticConcurrencyError } from 'event-storage';

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
    if (e instanceof OptimisticConcurrencyError) {
        // Replay and retry, or return a conflict error to the caller
    }
}
```

`expectedVersion` values:

| Value | Meaning |
|-------|---------|
| `ExpectedVersion.Any` (`-1`) | No check — always succeeds (default). |
| `ExpectedVersion.EmptyStream` (`0`) | Stream must not exist yet. |
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

### Raw Mode (NDJSON for Network Streaming)

Use raw mode when you want to stream events directly over HTTP/TCP without deserializing and re-serializing each document in userland.

```javascript
const stream = eventstore.getEventStream(
    'user-123',
    1,
    -1,
    { payload: { type: 'UserRegistered' } },
    true
);

// Each chunk is one compact JSON document plus "\n"
stream.pipe(httpResponse);
```

Raw mode is available on all stream-reading APIs (`getEventStream`, `getAllEvents`, `fromStreams`, `getEventStreamForCategory`) and also on `query()`.

The optional predicate argument accepts the same [matchers](matchers.md) as everywhere else; in raw mode they are applied to the compact JSON bytes.

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

The fluent builder expresses the same ranges more readably:

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

You can also pass extra metadata through when committing, and it will be merged into this object. The metadata argument comes before the callback:

```javascript
eventstore.commit('my-stream', [{ type: 'Foo' }], ExpectedVersion.Any, {
    causationId: 'cmd-123',
    correlationId: 'req-456'
}, (commit) => {});
```
