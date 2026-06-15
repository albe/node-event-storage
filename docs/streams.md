# Event Streams

An event stream is an ordered sequence of events that can be iterated. Every stream is backed by a lightweight index file and implements the JavaScript [Iterable](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Iteration_protocols) protocol as well as the Node.js `ReadableStream` interface.

## Writing Events (Commits)

Events are appended to a named stream with `commit`. The stream is created automatically the first time you write to it.

```javascript
eventstore.commit('user-123', [
  {type: 'UserRegistered', email: 'alice@example.com'},
  {type: 'ProfileCompleted', name: 'Alice'}
], ExpectStream.Empty(), (err) => {
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
| `ExpectStream.Any()` | No check — always succeeds (default). |
| `ExpectStream.Empty()` | Stream must not exist yet. |
| `ExpectStream.AtVersion(n)` | Stream must be at exactly version `n`. |
| `ExpectStream.AtGlobalSequence(n)` | Stream must have the last event at global sequence number `n`. |
| `ExpectStream.MatchCondition(condition)` | The condition must match, where `condition` is the `CommitCondition` returned by `query()`. |

Legacy numeric values remain supported for backward compatibility: `EventStore.ExpectedVersion.Any` (`-1`), `EventStore.ExpectedVersion.EmptyStream` (`0`), and positive integers.

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

Matcher semantics differ by mode:

| Mode | Function matcher | Object matcher |
|------|------------------|----------------|
| `raw=false` (default) | `(payload, metadata) => boolean` | Matched against `{ stream, payload, metadata }` |
| `raw=true` | `(buffer) => boolean` | Byte-level matcher against compact JSON bytes |

### Object Matcher Syntax

Object matchers use the same shape in both modes. In object mode they are evaluated against
`{ stream, payload, metadata }`. In raw mode the same matcher is compiled into byte-level checks.

Supported forms:

- **Scalar equality**

  ```javascript
  { payload: { type: 'OrderPlaced' } }
  ```

- **Nested object matching**

  ```javascript
  { metadata: { tenantId: 'acme' } }
  ```

- **Array values with OR semantics**

  ```javascript
  { payload: { type: ['OrderPlaced', 'OrderCancelled'] } }
  ```

- **Scalar comparison operators**

  ```javascript
  { payload: { amount: { $gte: 100, $lt: 1000 } } }
  ```

Supported operators are `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, and `$ne`.
Multiple operators on the same field are combined with AND semantics.

`$eq` is equivalent to plain equality, so prefer the simpler form when possible:

```javascript
{ payload: { type: 'OrderPlaced' } }
// equivalent to:
{ payload: { type: { $eq: 'OrderPlaced' } } }
```

Operator matching is intended for scalar values (`string`, `number`, `boolean`, `null`).
For arrays, objects, or custom raw encodings, use plain equality or a function matcher.

Important: raw object matchers require the default compact JSON serializer format. If you use a custom serializer, use a raw function matcher instead.

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

If `streamNames` is an empty array, `fromStreams()` returns an empty stream.

`fromStreams()` throws only when at least one named stream in `streamNames` does not exist.

## Stream Categories

### Flat category streams (`category-id`)

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

If no streams match the category prefix, `getEventStreamForCategory()` returns an empty stream.

This layout stores every stream as a flat file in the data directory:

```
data/
  eventstore.user-1
  eventstore.user-2
  eventstore.user-42
  …
streams/
  eventstore.stream-user-1.index
  eventstore.stream-user-2.index
  eventstore.stream-user-42.index
  …
```

This works well for small-to-medium numbers of entity instances. When the number of streams grows into the hundreds of thousands or millions (think users on a large platform), the flat layout can degrade directory-listing performance, because most operating systems slow down significantly when a single directory contains very large numbers of files.

### Hierarchical category streams (`category/id`)

For large-scale deployments, use a **slash-separated stream name** to organize streams into a directory hierarchy on disk:

```javascript
eventstore.commit('user/' + user.id, [new UserRegistered(user.id, user.email)]);
```

This maps to:

```
data/
  eventstore.user/
    1
    2
    42
    …
  streams/
    eventstore.stream-user/
      1.index
      2.index
      42.index
      …
```

Category queries work the same way — pass any prefix up to (but not including) the last separator. The query returns every stream whose name starts with that prefix followed by either `-` or `/`:

```javascript
// All user streams, regardless of depth
const allUsersStream = eventstore.getEventStreamForCategory('user');

// Narrowed to a sub-category (streams starting with 'user/a3/')
const shardStream = eventstore.getEventStreamForCategory('user/a3');

// Narrowed further to a two-level sub-category (streams starting with 'user/a3/f7/')
const leafStream = eventstore.getEventStreamForCategory('user/a3/f7');
```

`getEventStreamForCategory` unions **both layouts**: it returns events from all matching streams in global insertion order, regardless of whether they use the dash or slash convention.

For the unified API, `getStream('user/*')` requires a separator and a wildcard '*' as signal to return all streams within that category.

#### Hash-based sharding for very large entity populations

When millions of entity instances exist (e.g., users on a large platform), even a single subdirectory can become oversized. A two-level hash prefix distributes streams across thousands of small, balanced directories:

```javascript
function streamName(entityType, id) {
    // Two hex chars → 256 × 256 = 65 536 shards maximum
    const hex = id.toString(16).padStart(8, '0');
    const shard1 = hex.slice(0, 2);  // e.g. "a3"
    const shard2 = hex.slice(2, 4);  // e.g. "f7"
    return `${entityType}/${shard1}/${shard2}/${id}`;
}

// Stream for user 12345678 → "user/00/bc/614e" (actually padded hex of 12345678)
eventstore.commit(streamName('user', user.id), [
    new UserRegistered(user.id, user.email)
]);
```

On disk this looks like:

```
data/
  eventstore.user/
    00/
      00/
        1
        2
      01/
        257
      …
    a3/
      f7/
        12345678
      …
```

To read a specific user's stream you compose the same name:

```javascript
const userStream = eventstore.getEventStream(streamName('user', user.id));
```

Sub-category queries work at any depth — pass the shared prefix to scope the result set:

```javascript
// All users whose ID hashes to shard a3/f7
const leafStream = eventstore.getEventStreamForCategory('user/a3/f7');

// All users in the a3 top-level shard
const shardStream = eventstore.getEventStreamForCategory('user/a3');

// All user events across every shard
const allUsersStream = eventstore.getEventStreamForCategory('user');
```

#### Choosing a hash depth

| Estimated entity count | Recommended depth | Max streams per leaf dir |
|------------------------|-------------------|--------------------------|
| Up to ~100 k           | 1 level (`type/XX/id`) | ~390 |
| Up to ~25 M            | 2 levels (`type/XX/YY/id`) | ~1 526 |

Two hex characters per level gives 256 shards per level. Adjust by using more or fewer hex characters (e.g. 3 chars = 4 096 shards/level) to match the expected entity population. Additional nesting levels are structurally supported but have not been benchmarked at very large entity counts.

#### UUID-keyed entities

For entities identified by UUID v4, the first characters already have high entropy and can serve directly as the shard prefix:

```javascript
function userStream(uuid) {
    // "a3f7c2d1-…" → "user/a3/f7/a3f7c2d1-…"
    return `user/${uuid.slice(0, 2)}/${uuid.slice(2, 4)}/${uuid}`;
}
```

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
eventstore.commit('my-stream', [{type: 'Foo'}], ExpectStream.Any(), () => {
}, {
  causationId: 'cmd-123',
  correlationId: 'req-456'
});
```
