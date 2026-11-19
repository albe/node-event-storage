# Guarantees

This page states what node-event-storage guarantees and under which conditions. All guarantees apply to a single store with a single writer process.

## ACID Properties

> All of the following applies to a single transaction boundary, which is a single write stream (storage partition).

### Atomicity

A single event write is atomic at the JSON level — an incomplete write cannot be deserialized. For custom serialization formats (msgpack, protobuf, …) you should add a checksum to detect partial writes.

A multi-event commit is atomic end-to-end: on startup the store checks whether the last `commitId` matches the `commitSize`. If not, the incomplete commit is rolled back by truncating the storage to the position before it started.

Write-buffer batching also means logical atomicity can span multiple events. Control this with:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxWriteBufferDocuments` | `number` | unlimited | Maximum number of events in the write buffer before an automatic flush. |
| `writeBufferSize` | `number` | `16384` (16 KB) | Maximum byte size of the write buffer. |

For optimal I/O, avoid limiting `maxWriteBufferDocuments` so that the buffer fills to the page size (typically 4 KB) before being flushed.

### Consistency

The append-only nature guarantees consistency for all successful writes. Torn writes (partial flushes caused by a crash) are detected on startup and truncated when the lock is reclaimed:

```javascript
import { EventStore, LOCK_RECLAIM } from 'event-storage';

const eventstore = new EventStore('my-event-store', {
    storageConfig: { lock: LOCK_RECLAIM }
});
```

> **Warning:** `LOCK_RECLAIM` bypasses the single-writer lock. Do not use it when two processes might write concurrently.

### Isolation

- Only a single writer is permitted per store (enforced by a lock directory).
- Reads are always isolated: a read only sees writes that have already completed at the moment the stream is retrieved (MVCC).
- *Dirty reads* (reading events from the write buffer before they are flushed) are enabled by default for writers. To disable:

```javascript
const eventstore = new EventStore('my-event-store', {
    storageConfig: { dirtyReads: false }
});
```

Disabling dirty reads is only recommended for in-memory models that maintain their own uncommitted state.

### Durability

Strict durability is intentionally traded for write performance:

- Events are buffered in memory before being flushed to disk.
- Flush does **not** call `fsync` by default, so OS/disk write-cache can still absorb and potentially lose recent writes on a crash.

Three levels are available:

| Setting | Commit callback fires when | Survives process crash | Survives OS crash / power loss | Write throughput |
|---------|----------------------------|------------------------|--------------------------------|------------------|
| default | the data was handed to the OS | yes | no | highest |
| `syncInterval: <ms>` | the data was `fsync`ed, at most `<ms>` later | yes | yes, for acknowledged commits | close to the default |
| `syncOnFlush: true` | the data was `fsync`ed on every flush | yes | yes, for acknowledged commits | lowest |

`syncInterval` is a group commit: all flushes of a partition within the interval share one `fsync`, and their callbacks fire after it. A commit is therefore acknowledged up to `syncInterval` milliseconds later, but every acknowledged commit is on disk. Commits that are not yet acknowledged can still be lost.

```javascript
// Acknowledge commits only once they are on disk, with at most one fsync per partition every 50 ms
const eventstore = new EventStore('my-event-store', {
    storageConfig: { syncInterval: 50 }
});

// fsync on every flush
const strict = new EventStore('my-strict-store', {
    storageConfig: { syncOnFlush: true }
});
```

With either option, the directory is also synced after a new partition or index file is created, so a new stream does not disappear after a power loss.

## Global Order

The storage maintains a global primary index that gives every event a monotonically increasing sequence number. This guarantees a consistent order when reading across multiple write streams (e.g. in joined streams or category streams).

Each event additionally carries a monotonic clock stamp and its global sequence number in its binary header, so a consistent global order can be reconstructed without the global index if needed.

## Reliability and Crash-Safety Guarantees

node-event-storage is designed to survive hard process crashes (e.g. `SIGKILL`, power failure) and recover with a bounded, predictable amount of data loss.

### What is guaranteed

| Guarantee | Details |
|-----------|---------|
| **No silent corruption** | Torn writes (partial page writes caused by a crash) are detected on startup and automatically truncated. The store will never surface a partially-written event. |
| **Atomic commits** | A multi-event commit is all-or-nothing. If the process crashes mid-commit, the incomplete commit is rolled back on the next open. |
| **Consistent reads after recovery** | Once `'ready'` fires after a `LOCK_RECLAIM` open, all indexes are fully consistent with the data files. |
| **Bounded data loss** | Events that were in the in-memory write buffer at the time of the crash are lost. The maximum loss is deterministic and can be calculated from the buffer settings (see below). |

### Quantifying the data-loss window

Two factors bound how many events can be lost in a crash:

1. **Partition write buffer** — each stream buffers up to `writeBufferSize` bytes or `maxWriteBufferDocuments` events before flushing. With a 16 KB buffer and ~100-byte events, that is at most ~160 events per stream.
2. **In-flight commit** — at most one commit's worth of events (the number of events passed to that `commit()` call) may be torn.

Index entries that were buffered but not yet flushed at the time of the crash are **not** lost: `LOCK_RECLAIM` automatically reindexes from the data files on the next open, making all on-disk events visible again.

With the default settings and a single stream the worst-case data loss is roughly:

```
max_loss ≈ (writeBufferSize / avg_event_size) + events_per_commit
         ≈ 160 + events_per_commit   (for 100-byte events and a 16 KB write buffer)
```

### The stress test

The repository ships a crash-safety stress test in `stress-test/` that validates these guarantees empirically:

1. **Writer** (`stress-test/writer.js`) — commits events to multiple streams in a tight loop with a small write buffer, persisting a snapshot of the commit counts after every successful flush.
2. **Orchestrator** (`stress-test/run.sh`) — starts the writer, lets it run for a configurable number of seconds, then kills it with `SIGKILL` to simulate a hard crash.
3. **Recovery** (`stress-test/recovery.js`) — reopens the store with `LOCK_RECLAIM`, verifies all streams are readable and writable, calculates the actual data loss, and asserts it is within the theoretical upper bound.

Run it locally with:

```bash
cd stress-test
npm install
WRITE_DURATION=10 bash run.sh
```

The test exits with code `0` if data loss is within bounds and code `1` otherwise, making it suitable for CI. The GitHub Actions workflow (`.github/workflows/stress-test.yml`) runs it on every push.

## Size Limits

Index entries store sequence numbers and file positions as unsigned 32-bit integers. A storage therefore holds at most 4,294,967,295 documents, and every document has to start within the first 4 GiB of its partition. A write beyond either limit is rejected with an error before anything is written, so partitions and index stay consistent. To store more data per storage, use a partitioner that spreads the documents over more partitions.
