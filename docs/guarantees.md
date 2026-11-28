# Guarantees

This page states what node-event-storage guarantees, under which conditions, and where you can check it. All statements apply to one store with one writer process. node-event-storage does not build on a database engine; the mechanisms below are implemented directly on top of the file system, and the on-disk layout is documented in [Storage Format](storage-format.md).

## At a glance

| Guarantee | Holds when | Evidence |
|-----------|------------|----------|
| A commit is all-or-nothing | Always, after the store was reopened by a writer | `test/EventStore.spec.js` ("repairs unfinished commits", "repairs torn writes"), `stress-test/` |
| Only one process writes | Always; a second writer gets `StorageLockedError` while the first one is running | `StorageLock`, `test/Storage.spec.js` ("allows only a single writer") |
| `expectedVersion` and DCB conditions cannot be raced by another commit | Always (check and append happen in one synchronous call) | `EventStore.commit()`, `test/EventStore.spec.js` ("query" tests) |
| Events have one total order | Always, via the global sequence number | primary index, `JoinEventStream` |
| The writer reads its own writes | `dirtyReads: true` (default) | `test/Partition.spec.js` |
| Acknowledged commits survive an OS crash or power loss | With `syncInterval` or `syncOnFlush` | `SyncScheduler`, `WritablePartition.flush()` |
| Acknowledged commits survive a process crash | Always (data was handed to the OS) | `stress-test/` |
| After a crash, the store repairs itself | When the next writer reclaims the lock: automatically if the crashed process is gone on the same machine, or forced with `LOCK_RECLAIM` | `test/Storage.spec.js` ("auto-repairs …"), `stress-test/recovery.js` |
| No event is ever modified | Always; files are opened in append mode, and repair only truncates the tail after a crash, keeping a copy of the removed bytes in a `.branch` file | `WritablePartition.truncate()` |
| Partitions and index never diverge because of size limits | Always; a document beyond the 32-bit limits is rejected before it is written | `WritableStorage.write()` |

"Acknowledged" means the `commit()` callback was called.

## Atomicity

A `commit()` call appends one or more events to a single stream. Every event carries `commitId`, `commitVersion` (0-based position within the commit) and `commitSize` in its metadata.

When a writer opens the store, it reads the last event of the global log. If its `commitVersion` is not `commitSize - 1`, the commit was interrupted: the store emits `'unfinished-commit'` and truncates the log back to where that commit started. This check runs on every writer open. Truncated bytes are not discarded: they are copied to a `<partition>-truncated-<timestamp>-<position>.branch` file next to the partition for manual inspection.

On a lower level, every document is framed by a header with its size and a footer that repeats it (see [Storage Format](storage-format.md#document-envelope)). A document whose end lies beyond the end of the file is a *torn write*; reading it throws instead of returning partial data, and the repair after a crash truncates it.

A commit always targets one write stream. There is no atomic write across several streams; use one stream per consistency boundary, or [DCB](dcb.md) for consistency checks across streams.

## Concurrency control

**Single writer.** The writer creates a lock directory `<storeName>.lock` next to the data files with `mkdir`, which fails atomically if it already exists, and writes its owner (pid, host name and, on Linux, pid namespace) into it. A second writer receives `StorageLockedError`, which names the holder. The lock is removed once `close()` has synced everything.

If a writer crashed, the lock remains. With the default `lock: LOCK_AUTO`, the next writer reclaims it only when the owner was recorded on the same host and in the same pid namespace and that process is no longer running. Two writers that find the same stale lock at the same time are serialized by a second `mkdir` inside the lock directory, so only one of them removes it. In every other case (owner still running, other host or container, no owner information) the lock is not touched; `LOCK_RECLAIM` takes it over unconditionally and `LOCK_THROW` never reclaims it.

**Optimistic concurrency per stream.** `commit(stream, events, expectedVersion)` compares `expectedVersion` with the current length of the stream index and throws `OptimisticConcurrencyError` on mismatch. Because `commit()` runs synchronously from the check to the point where all events are in the write buffer, no other commit can interleave in between.

**Dynamic Consistency Boundaries.** `query()` returns a `CommitCondition` that records the selector, the optional matcher and the global log length at query time. `commit(…, condition)` re-evaluates the selector over all events appended after that position and throws `OptimisticConcurrencyError` if at least one matches. Streams that did not exist at query time are included. Events appended before the query never cause a conflict. Like the version check, this happens synchronously inside `commit()`. `condition.without(...streams)` narrows a condition by treating the given streams as empty; it can only make the condition narrower, never wider.

Read-only instances cannot commit, so all concurrency control is local to the writer process.

## Isolation and visibility

- An `EventStream` fixes its upper bound when it is created. Events appended later do not appear in it; use a [consumer](consumers.md) to follow new events.
- The writer sees its own commits immediately, including events still in the write buffer (`dirtyReads: true`). With `dirtyReads: false` it only reads flushed data.
- Read-only processes see events once the writer has flushed them to the data file and the index. Flushes happen independently per file, so a reader can observe a large multi-event commit partially while it is being written. Use `metadata.commitVersion` / `commitSize` if a reader must only act on complete commits.

## Ordering

Every event receives a global, gap-free, 1-based sequence number from the primary index. Joined streams, categories and `getAllEvents()` return events in this order. Each event header additionally stores a microsecond timestamp from a per-partition monotonic clock (`process.hrtime` based, it never decreases within a process) and the sequence number, so the global order can be reconstructed from the data files alone, which is what `reindex()` does.

## Durability

Writes go through two buffers:

1. **Write buffer (per partition).** `commit()` serializes events into an in-memory buffer (`writeBufferSize`, default 16 KB). The buffer is written to the file when it is full, when `maxWriteBufferDocuments` is reached, or at the latest on the next tick of the event loop.
2. **OS page cache.** Data written to the file survives a crash of the Node.js process, but not necessarily an OS crash or power loss, until it was `fsync`ed.

When the `commit()` callback fires depends on the durability setting:

| Setting | Commit callback fires when | Survives process crash | Survives OS crash / power loss | Write throughput |
|---------|----------------------------|------------------------|--------------------------------|------------------|
| default | the data was handed to the OS | yes | no | highest |
| `syncInterval: <ms>` | the next sync run `fsync`ed it, at most `<ms>` after the write | yes | yes, for acknowledged commits | close to the default for pipelined commits; an awaited commit takes about the interval ([details](performance.md#syncinterval-and-commit-latency)) |
| `syncOnFlush: true` | the next sync run, which starts right after the current tick, `fsync`ed it | yes | yes, for acknowledged commits | lowest |

Every flush of a partition is handed to the storage's sync scheduler, which syncs it with the next run, after the interval (`syncInterval`) or right after the current tick (`syncOnFlush`), or not at all (default). A newly created partition or index file is synced together with its directory by the same rule, so a new stream does not disappear after a power loss.

`syncInterval` is a group commit: one run per interval flushes all partitions written since the last run, `fsync`s each of them and every new file and directory once, and only then fires the callbacks. Commits to different streams therefore share the same run. A run syncs its files in parallel in the background, so the process goes on writing meanwhile; the next run starts only after it finished, so the callbacks fire in the order of the writes. If an `fsync` fails, the callbacks of that run never fire, because the data may be lost, and the error rejects the close of the storage, or crashes the process as an unhandled rejection if no close waits for it. Closing the store starts a pending run right away and waits for all runs before it releases the lock. Evicting a file handle because of `maxOpenPartitions` or `maxOpenIndexes` only flushes the file.

Index files are flushed with a short delay (100 ms by default) or when their buffer is full, but their entries are not synced while the store is open: an index can always be rebuilt from the partitions. With `syncInterval` or `syncOnFlush`, closing the store syncs every changed index together with the last sync run. While it is open, the writer records a **checkpoint** in the lock directory every 5 minutes: it syncs every index changed since the previous checkpoint in the background, and only then records the length the primary index had before. The repair after a crash rebuilds all index entries after the last checkpoint from the partitions, so it scans at most the events written since then. The repair does not sync the entries it rebuilt either: it records the previous checkpoint again in the new lock directory, so a crash before the next checkpoint rebuilds the same entries. Truncating or reindexing the store lowers the checkpoint to where it cut the index.

Consumer state files are replaced atomically (write a temporary file, then rename) but are not `fsync`ed. After an OS crash a consumer may resume from an earlier position, which means at-least-once redelivery, never a state that does not match its position.

### What can be lost in a crash

| Failure | Default settings | `syncInterval` or `syncOnFlush` |
|---------|------------------|---------------------------------|
| Process crash (e.g. `SIGKILL`) | Events committed in the current event-loop tick that were not yet written; at most one incomplete commit is rolled back | Same, but none of them were acknowledged |
| OS crash or power loss | Additionally, anything in the OS page cache that was not yet written to disk | Nothing that was acknowledged |

An upper bound for the default case per stream is `writeBufferSize / average event size` events plus the size of the interrupted commit, e.g. about 160 events plus one commit for 100-byte events and a 16 KB buffer. In practice the window is much smaller, because the buffer is flushed on the next tick. The [stress test](verify.md#crash-safety) checks this bound by killing a writer with `SIGKILL` and counting lost commits.

## Crash recovery

Opening a writer runs these steps:

1. If a lock is left behind: reclaim it when its owner process is gone on this machine (`LOCK_AUTO`, the default) or unconditionally (`LOCK_RECLAIM`), otherwise throw `StorageLockedError`.
2. If a lock was reclaimed: scan all partitions for torn writes, truncate every partition to the last complete document, and rebuild all index entries after the last checkpoint from the data, including those of secondary indexes that were not loaded (`checkTornWrites()`, `reindex()`).
3. Always: roll back an unfinished last commit (see [Atomicity](#atomicity)).
4. Emit `'ready'`.

See [Operations](operations.md#crash-recovery) for how to use this in practice.

## What is not guaranteed

The design limits (single machine, file sizes, number ranges, file system requirements) are listed in [Limitations](limitations.md).
