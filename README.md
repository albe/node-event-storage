![event-storage](logo/color.png)

[![build](https://github.com/albe/node-event-storage/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/albe/node-event-storage/actions/workflows/build.yml)
[![npm version](https://badge.fury.io/js/event-storage.svg)](https://badge.fury.io/js/event-storage)
[![Code Climate](https://codeclimate.com/github/albe/node-event-storage/badges/gpa.svg)](https://codeclimate.com/github/albe/node-event-storage)
[![Coverage Status](https://coveralls.io/repos/github/albe/node-event-storage/badge.svg?branch=main)](https://coveralls.io/github/albe/node-event-storage?branch=main)
[![Code documentation](https://inch-ci.org/github/albe/node-event-storage.svg?branch=main)](https://inch-ci.org/github/albe/node-event-storage)

# node-event-storage

An optimized embedded event store for modern node.js, written in ES6.

📖 **[Full documentation on readthedocs.io](https://node-event-storage.readthedocs.io/en/latest/)**

---

## Why?

There is currently only a single other embedded event store for node/javascript: [node-eventstore](https://github.com/adrai/node-eventstore). It has a few drawbacks:

- Its API requires loading a full Event Stream before committing, making it unfit for frequently-restarting client applications.
- Its embeddable backends (TingoDB, NeDB) do not persist indexes and are slow on initial load.
- Events are fixed to one stream — creating overlapping projection streams is not possible.

**node-event-storage** is built from first principles for append-only workloads, giving you near-optimal write speed with no unnecessary overhead.

---

## Installation

```bash
npm install event-storage
```

## Quick Start

```javascript
const EventStore = require('event-storage');

const eventstore = new EventStore('my-event-store', { storageDirectory: './data' });

eventstore.on('ready', () => {
    // Write events
    eventstore.commit('my-stream', [{ type: 'SomethingHappened', value: 42 }], 0, () => {
        console.log('Written!');
    });

    // Read events
    const stream = eventstore.getEventStream('my-stream');
    for (const event of stream) {
        console.log(event);
    }
});
```

## Key Features

| Feature | Summary |
|---------|---------|
| **Optimistic concurrency** | Pass `expectedVersion` to `commit()` to guarantee conflict-free writes. |
| **Flexible stream reading** | Range queries, reverse iteration, and a fluent builder API. |
| **Derived streams** | Filter or combine events into new read-only streams. |
| **Stream categories** | Name streams `<category>-<id>` and query the whole category at once. |
| **Durable consumers** | At-least-once (and exactly-once with `setState`) event delivery with automatic position tracking. |
| **Consistency guards** | Build aggregates that enforce business invariants with built-in snapshotting. |
| **Read-only mode** | Open the store from a second process to build projections without touching the writer. |
| **Crash safety** | Torn writes detected and truncated on startup; automatic index repair via `LOCK_RECLAIM`; bounded, predictable data loss validated by a dedicated stress test. |
| **Custom serialization** | Plug in msgpack, protobuf, or any other codec. |
| **Compression** | Apply LZ4, zstd, or any other compression via the `serializer` option. |
| **Access control hooks** | `preCommit` / `preRead` hooks with per-stream metadata for authorization. |

---

## Documentation

The full documentation is hosted at **<https://node-event-storage.readthedocs.io/en/latest/>** and covers:

- [Getting Started](https://node-event-storage.readthedocs.io/en/latest/getting-started/) — installation, constructor options, basic usage.
- [Event Streams](https://node-event-storage.readthedocs.io/en/latest/streams/) — writing, reading, optimistic concurrency, fluent API, joining streams, categories, and event metadata.
- [Consumers](https://node-event-storage.readthedocs.io/en/latest/consumers/) — at-least-once and exactly-once delivery, consumer state, consistency guards, and read-only mode.
- [Advanced Topics](https://node-event-storage.readthedocs.io/en/latest/advanced/) — ACID properties, reliability and crash-safety guarantees, storage configuration, partitioning, custom serialization, compression, security, and access control hooks.

---

## Run Tests

```bash
npm test
```
