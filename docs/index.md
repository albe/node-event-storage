![node-event-storage](../logo/color.png)

[![build](https://github.com/albe/node-event-storage/workflows/build/badge.svg?branch=main)](https://github.com/albe/node-event-storage/actions)
[![npm version](https://badge.fury.io/js/event-storage.svg)](https://badge.fury.io/js/event-storage)
[![Code Climate](https://codeclimate.com/github/albe/node-event-storage/badges/gpa.svg)](https://codeclimate.com/github/albe/node-event-storage)
[![Coverage Status](https://coveralls.io/repos/github/albe/node-event-storage/badge.svg?branch=main)](https://coveralls.io/github/albe/node-event-storage?branch=main)

# node-event-storage

An optimized embedded event store for modern Node.js, written in ES6.

> **Disclaimer:** This project is under active development and not yet production-ready.
> See [issue #29](https://github.com/albe/node-event-storage/issues/29) for the current status.

---

## Why node-event-storage?

The only other embedded event store for Node.js is [node-eventstore](https://github.com/adrai/node-eventstore). It is a solid project, but has several limitations:

- Its API is built entirely around Event Streams, requiring you to load a full stream before committing a new event — impractical for client applications that restart frequently.
- It supports many existing databases as backends (MongoDB, NeDB, …), but none of them are optimized for event storage workloads.
- The embeddable backends (TingoDB, NeDB) do not persist indexes, making initial load very slow at scale.
- It stores publishing metadata inside events, which causes data mutations.
- Events are fixed to one stream; there is no way to create overlapping streams for projections.

**node-event-storage** addresses all of these by building an event store from first principles, optimized exclusively for append-only workloads.

---

## Use Cases

- Event-sourced desktop applications built with Electron or NW.js.
- Small event-sourced single-server applications that need near-optimal write performance.
- Queryable log storage.

---

## Design Goals

### Single-node scalability

- Opening or writing to an existing store with millions of events must be as fast as an empty store.
- Write performance must not be constrained by locking or distributed-transaction costs (single writer per stream).
- Read performance is optimized for sequential forward reads from arbitrary positions.
- Any number of concurrent readers are supported (typically one per projection).
- Thousands of streams can be created without significant memory or CPU overhead.
- Replaying a stream costs no more than visiting every event in it — no full table scan.

### Consistency

- Writes to a single stream guarantee consistency: every write sees the state immediately before it.
- Reads from a stream are always consistent: repeatable-read isolation with read-committed for read-only stores and read-uncommitted (read-your-own-writes) for writers.

### Simplicity

- The architecture is intentionally minimal.
- Creating new derived streams from existing data is achievable with standard language constructs.

### Non-Goals

- Distributed storage or distributed transactions.
- Network API.
- Cross-stream transactions.
- Arbitrary query capabilities — only sequential range scans per stream.

---

## How Append-Only Storage Changes Everything

Traditional databases do a great deal of work that event stores simply do not need:

| Property | Traditional DB | node-event-storage |
|----------|---------------|--------------------|
| Write-ahead log | Required | Not needed — the store **is** the log |
| Write speed | Constrained by WAL + locks | As fast as sequential disk I/O |
| Single writer | Optional | Required (per stream) |
| Durability overhead | High | Configurable (trade off for performance) |
| Read isolation | Complex MVCC | Natural — append-only means reads never see partial writes |
| Indexes | B+-Tree / fractal trees | Simple file-position lists — cheap to create in high numbers |
| Backups | Database-specific tooling | `rsync` or file copy |

---

## Documentation Structure

| Page | Contents |
|------|----------|
| [Getting Started](getting-started.md) | Installation, constructor options, first commit & read |
| [Event Streams](streams.md) | Writing, reading, revision ranges, fluent API, joining, categories, metadata |
| [Consumers](consumers.md) | At-least-once & exactly-once delivery, state, consistency guards, read-only mode |
| [Advanced Topics](advanced.md) | ACID details, storage config, partitioning, serialization, compression, security, access control |
