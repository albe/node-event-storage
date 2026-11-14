# Organizing Streams

Every stream is a lightweight index over the shared event log, so one event can belong to many streams. This page covers the ways to create and combine them.

## Derived Streams

You can create derived read streams that contain only a filtered subset of another stream, or even events from multiple streams combined.

```javascript
// Only events whose type is 'FooHappened' or 'BarHappened'
const projectionStream = eventstore.createEventStream(
    'my-projection-stream',
    (document) => ['FooHappened', 'BarHappened'].includes(document.payload.type)
);

// The same with an object matcher (preferred: routed in O(1) on writes and not eval'd on load)
const sameStream = eventstore.createEventStream(
    'my-projection-stream-2',
    { payload: { type: ['FooHappened', 'BarHappened'] } }
);

for (const event of projectionStream) {
    // ...
}
```

A function matcher receives the stored document `{ stream, payload, metadata }`, not just the event payload. The matcher is persisted in the index file so you do not need to re-specify it when reopening the store. See [Security](customization.md#security) for how the matcher is protected against tampering.

## Automatically Maintained Streams

On every `commit()`, EventStore can automatically maintain dedicated streams for any payload property. The high-level options `typeAccessor` and `tagsAccessor` are convenient shorthands; `streamSources` is the generic mechanism they build on.

Each entry in `streamSources` specifies a dot-notation `path` into the event payload and a `nameBuilder(value) => streamName` function. Scalar property values are passed to `nameBuilder` directly; array values are iterated so each element produces its own stream. Non-string and empty values are skipped silently.

```javascript
const store = new EventStore('my-store', {
    storageDirectory: './data',
    streamSources: [
        // one stream per tenant — 'acme' → stream 'tenant/acme'
        { path: 'tenantId', nameBuilder: (v) => `tenant/${v}` },
        // one stream per tag value (equivalent to tagsAccessor: 'tags')
        { path: 'tags',     nameBuilder: (v) => `tags/${v}` }
    ]
});
```

`typeAccessor` and `tagsAccessor` are shorthand for:

```javascript
streamSources: [
    { path: 'type', nameBuilder: (v) => v },           // typeAccessor: 'type'
    { path: 'tags', nameBuilder: (v) => `tags/${v}` }  // tagsAccessor: 'tags'
]
```

with the additional benefit that `typeAccessor` and `tagsAccessor` also wire into `DcbQuery` resolution for the `types` and `tags` fields.

Each source's `payload.<path>` is automatically added to `matcherProperties` so the IndexMatcher routes index writes in O(1) rather than scanning all registered matchers.

---

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

`fromStreams` accepts a selector tree with alternating operators by depth:

- depth 0 (top-level array): `OR` — union of all listed streams
- depth 1 (second-level arrays): `AND` — intersection of all listed streams
- depth 2: `OR` again, and so on

```javascript
// OR: all events from either stream
eventstore.fromStreams('or-stream', ['order-42', 'order-99']);

// AND: only events indexed in both streams (intersection)
eventstore.fromStreams('and-stream', [['order-42', 'payment-42']]);

// (tag-a AND tag-b AND (type-x OR type-y)) OR (tag-c AND tag-d)
eventstore.fromStreams('selector-stream', [
    ['tag-a', 'tag-b', ['type-x', 'type-y']],
    ['tag-c', 'tag-d']
]);
```

The union and intersection are computed over index entry ranges before any document is read — no events are deserialized that do not belong to the result set.

The result is not persisted and cannot be used with consumers. For frequently-needed joins, create a permanent derived stream with `createEventStream` instead.

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

If you already created a dedicated stream for the category (e.g. via `createEventStream`), that stream is returned directly.

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
