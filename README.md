![event-storage](logo/color.svg)

[![build](https://github.com/albe/node-event-storage/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/albe/node-event-storage/actions/workflows/build.yml)
[![npm version](https://badge.fury.io/js/event-storage.svg)](https://badge.fury.io/js/event-storage)
[![Code Climate](https://codeclimate.com/github/albe/node-event-storage/badges/gpa.svg)](https://codeclimate.com/github/albe/node-event-storage)
[![Coverage Status](https://coveralls.io/repos/github/albe/node-event-storage/badge.svg?branch=main)](https://coveralls.io/github/albe/node-event-storage?branch=main)
[![Code documentation](https://inch-ci.org/github/albe/node-event-storage.svg?branch=main)](https://inch-ci.org/github/albe/node-event-storage)

# node-event-storage

An optimized embedded event store for modern node.js, written in ES6.

📖 **[Full documentation on readthedocs.io](https://node-event-storage.readthedocs.io/en/stable/)**

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

> **CommonJS / `require()` users:** version 1.0 is ESM-only. If your project uses `require()` and migrating to ESM is not an option, install the 0.x series (`npm install event-storage@0`), which supports CommonJS but lacks the features added in 1.x.


## Quick Start

```javascript
import { EventStore } from 'event-storage';

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
| **Joined and derived streams** | Combine any number of streams with a nested OR/AND selector tree into one globally-ordered virtual stream; filter events into reusable derived projection streams. |
| **Object matchers** | Support nested equality, array values (OR semantics), scalar operators (`$gt` / `$gte` / `$lt` / `$lte` / `$eq` / `$ne`), string prefixes (`$startsWith`), plus array-containment operators (`$has` / `$hasAny`), while still benefiting from O(1) discriminant routing on writes. |
| **DCB** | Configure `typeAccessor` to have per-type stream indexes maintained automatically, and use `query()` / `Condition` for fine-grained, query-scoped optimistic concurrency (Dynamic Consistency Boundaries). |
| **Stream categories** | Name streams `<category>-<id>` and query the whole category at once. |
| **Durable consumers** | At-least-once (and exactly-once with `setState`) event delivery with automatic position tracking. |
| **Consistency guards** | Build aggregates that enforce business invariants with built-in snapshotting. |
| **Read-only mode** | Open the store from a second process to build projections without touching the writer. |
| **Crash safety** | Torn writes detected and truncated on startup; automatic lock reclaim and index repair after a crashed writer; bounded, predictable data loss validated by a dedicated stress test. |
| **Custom serialization** | Plug in msgpack, protobuf, or any other codec. |
| **Compression** | Apply LZ4, zstd, or any other compression via the `serializer` option. |
| **Access control hooks** | `preCommit` / `preRead` hooks with per-stream metadata for authorization. |

---

## Companion Packages

- **[event-storage-http](https://github.com/albe/node-event-storage-http)** exposes a store over HTTP (NDJSON streams, consumer management, `HttpEventStream` client).
- **[event-storage-ui](https://github.com/albe/node-event-storage-ui)** is an admin dashboard for inspecting a running store on the same machine.

---

## Documentation

The full documentation is hosted at **<https://node-event-storage.readthedocs.io/en/stable/>**:

- **New to event sourcing?** [Event Sourcing in 5 Minutes](https://node-event-storage.readthedocs.io/en/stable/event-sourcing/), [When to Use](https://node-event-storage.readthedocs.io/en/stable/when-to-use/), [Getting Started](https://node-event-storage.readthedocs.io/en/stable/getting-started/).
- **Guides:** [Writing and Reading Events](https://node-event-storage.readthedocs.io/en/stable/reading-writing/), [Organizing Streams](https://node-event-storage.readthedocs.io/en/stable/organizing-streams/), [Matchers](https://node-event-storage.readthedocs.io/en/stable/matchers/), [DCB](https://node-event-storage.readthedocs.io/en/stable/dcb/), [Consumers](https://node-event-storage.readthedocs.io/en/stable/consumers/), [Multiple Processes](https://node-event-storage.readthedocs.io/en/stable/multi-process/), [Customization](https://node-event-storage.readthedocs.io/en/stable/customization/), [Operations](https://node-event-storage.readthedocs.io/en/stable/operations/).
- **Evaluating the implementation?** [Guarantees](https://node-event-storage.readthedocs.io/en/stable/guarantees/), [Implementation Architecture](https://node-event-storage.readthedocs.io/en/stable/architecture/), [Performance](https://node-event-storage.readthedocs.io/en/stable/performance/).
- **Reference:** [API](https://node-event-storage.readthedocs.io/en/stable/api/), [Configuration](https://node-event-storage.readthedocs.io/en/stable/configuration/), [Changelog](https://node-event-storage.readthedocs.io/en/stable/changelog/), [Upgrading](https://node-event-storage.readthedocs.io/en/stable/upgrading/).

---

## Run Tests

```bash
npm test
```
