# Multiple Processes

A store has exactly one writer process at a time. The writer holds a lock directory (`<storeName>.lock`); a second writer fails with `StorageLockedError` instead of silently interleaving writes. Any number of read-only processes can open the same store concurrently.

## Read-Only Mode

Open the store in read-only mode to create consumers that run in a **separate process** from the writer:

```javascript
import { EventStore } from 'event-storage';

const eventstore = new EventStore('my-event-store', {
    storageDirectory: './data',
    readOnly: true
});

eventstore.on('ready', () => {
    const consumer = eventstore.getConsumer('my-stream', 'my-consumer');
    consumer.on('data', (event) => {
        // build a read model, serve it via HTTP, etc.
    });
});
```

- Writes are prevented; all reads and consumers work normally.
- The read-only store watches the underlying files and automatically picks up changes written by the writer process.
- Multiple read-only instances can run simultaneously.
- This enables a multi-process projection pattern: one writer process + N reader processes building different projections.

If you need to hand the *current* process over from writing to reading during a deployment, use `EventStore.makeReadOnly([callback])` instead of starting a new read-only instance.

> In principle this also works across machines sharing a common filesystem (e.g. NFS), as long as the Node.js file watcher functions correctly on that filesystem.

## Requirements for live updates

Read-only instances discover new events through recursive `fs.watch`. This is supported by Node.js on Windows, macOS and Linux. On network file systems, watch events may be delayed or missing, so verify the behaviour on your setup before relying on it.

## Stale locks

If the writer process crashed, its lock directory remains. The next writer on the same machine reclaims it automatically and repairs the store, because the lock records which process held it. A lock from another host or container is not reclaimed automatically; see [Operations](operations.md#crash-recovery).
