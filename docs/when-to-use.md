# When to Use node-event-storage

## Why a dedicated embedded event store?

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
- Write performance must not be constrained by locking or distributed-transaction costs (a single writer process per store).
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
- Atomic writes to several streams in one commit (a commit always targets one write stream; consistency checks across streams are possible with [DCB conditions](dcb.md)).
- An ad-hoc query language — reads are range scans over stream indexes, optionally combined with AND/OR selectors and filtered by matchers.

---

## How Append-Only Storage Changes Everything

Traditional databases do a great deal of work that event stores simply do not need:

| Property | Traditional DB | node-event-storage |
|----------|---------------|--------------------|
| Write-ahead log | Required | Not needed — the store **is** the log |
| Write speed | Constrained by WAL + locks | As fast as sequential disk I/O |
| Single writer | Optional | Required (one writer process per store) |
| Durability overhead | High | Configurable (trade off for performance) |
| Read isolation | Complex MVCC | Natural — append-only means reads never see partial writes |
| Indexes | B+-Tree / fractal trees | Simple file-position lists — cheap to create in high numbers |
| Backups | Database-specific tooling | `rsync` or file copy |

---

## Compared with other options

This is a qualitative comparison of approaches, not a benchmark.

| Option | Typical strengths | Choose node-event-storage instead when |
|--------|-------------------|----------------------------------------|
| A relational database (e.g. PostgreSQL or SQLite) with an events table | Familiar tooling, ad-hoc SQL queries, transactions across tables, replication | You want no database server or schema, cheap streams by the thousands, and a store tuned only for appends and sequential reads |
| A dedicated event store server (e.g. KurrentDB, formerly EventStoreDB) | Clustering, network access from many languages, built-in subscriptions and projections at scale | Your application runs as one Node.js process (or one writer plus readers) and you want the store embedded in it |
| node-eventstore with an embedded backend | Pluggable backends, long history | You need persisted indexes, fast startup on large stores, and events that belong to several streams |

Everything node-event-storage guarantees, and how it achieves that without a known database engine underneath, is documented under [Guarantees](guarantees.md) and [Implementation Architecture](architecture.md).
