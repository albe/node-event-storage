# Operations

## Backups

All data lives in plain files below `storageDirectory`: partition files, index files, consumer state and the lock directory. The simplest consistent backup is a file copy (or `rsync`) while no writer is running. A copy taken while a writer is active may end in an incomplete commit; opening it with `LOCK_RECLAIM` truncates that commit and rebuilds lagging indexes, as described below.

## Crash Recovery

After a crash the writer's lock directory is left behind, and the last commit may be incomplete. Open the store with `LOCK_RECLAIM` to take over the lock and repair it:

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

On open, the store truncates an incomplete (torn) commit and, if the primary index lags behind the data files, calls `reindex()` automatically. `'ready'` fires only after the repair is complete. What can be lost in a crash is described in [Guarantees](guarantees.md#what-can-be-lost-in-a-crash).

> **Warning:** `LOCK_RECLAIM` bypasses the single-writer lock. Only use it when you are sure no other process is writing to the store.

For strict durability (no loss of acknowledged commits, at the cost of write throughput), combine it with `syncOnFlush`:

```javascript
const eventstore = new EventStore('my-event-store', {
    storageConfig: {
        lock: LOCK_RECLAIM,
        syncOnFlush: true
    }
});
```

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
