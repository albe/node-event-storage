# Operations

## Backups

All data lives in plain files below `storageDirectory`: partition files, index files, consumer state and the lock directory. The simplest consistent backup is a file copy (or `rsync`) while no writer is running. A copy taken while a writer is active may end in an incomplete commit; opening it with `LOCK_RECLAIM` truncates that commit and rebuilds lagging indexes, as described below.

## Crash Recovery

After a crash the writer's lock directory is left behind, and the last commit may be incomplete. The lock records which process holds it (pid, host name and, on Linux, pid namespace). When the store is opened again on the same machine and that process is no longer running, the lock is reclaimed and the store repaired automatically: an incomplete commit is truncated and lagging indexes are rebuilt before `'ready'` fires. Nothing needs to be configured for this (`lock: LOCK_AUTO` is the default).

The lock is not reclaimed automatically, and `StorageLockedError` names the holder instead, when:

- the holder is still running,
- the lock was taken on another host or in another container (another pid namespace), e.g. on a shared network volume,
- the lock has no owner information because it was created by an earlier version.

Set `lock: LOCK_THROW` to never reclaim a lock automatically. If you are sure no other process is writing, open the store with `LOCK_RECLAIM` to take over any lock and repair it:

```javascript
import { EventStore, LOCK_RECLAIM } from 'event-storage';

const eventstore = new EventStore('my-event-store', {
    storageDirectory: './data',
    storageConfig: { lock: LOCK_RECLAIM }
});

eventstore.on('ready', () => {
    // Store is fully repaired and consistent — safe to read and write
});
```

On open, the store truncates an incomplete (torn) commit and, if the primary index lags behind the data files, calls `reindex()` automatically. `'ready'` fires only after the repair is complete. What can be lost in a crash is described in [Guarantees](guarantees.md#quantifying-the-data-loss-window).

> **Warning:** `LOCK_RECLAIM` bypasses the single-writer lock. Only use it when you are sure no other process is writing to the store.

> **Note:** Automatic reclaiming assumes that a process with the same host name and pid namespace shares this machine's process table. Do not let several machines with the same host name write to one storage directory over a network file system.

## Durability

By default, commits are acknowledged once they are handed to the operating system, which protects against process crashes but not against an OS crash or power loss. For strict durability (no loss of acknowledged commits, at the cost of write throughput), enable `syncOnFlush`:

```javascript
const eventstore = new EventStore('my-event-store', {
    storageConfig: { syncOnFlush: true }
});
```

See [Guarantees](guarantees.md#durability) for what each setting protects against.

## Reindexing

If an index becomes inconsistent with the data files (e.g. after manual recovery), rebuild it:

```javascript
eventstore.on('ready', () => {
    // Full rebuild
    eventstore.storage.reindex(0);

    // Partial rebuild from a known-good checkpoint
    eventstore.storage.reindex(1000);
});
```

`reindex()` scans the partition files. For large stores prefer `reindex(knownGoodPosition)` over `reindex(0)`.

## Tuning

Write and read latency, startup time and the relevant options (`matcherProperties`, `maxOpenPartitions`, write buffer settings) are covered in [Performance](performance.md).
