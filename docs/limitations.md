# Limitations

These limits follow from the design and the [storage format](storage-format.md). Knowing them up front is part of deciding whether node-event-storage fits a project.

## Deployment

- **One machine, one writer process.** There is no replication, clustering or failover. High availability has to come from outside, e.g. replicated block storage or regular [backups](operations.md#backups).
- **No network access built in.** Other processes on the same machine can open the store read-only; remote access needs a layer such as [event-storage-http](https://github.com/albe/node-event-storage-http).
- **Read-only live updates depend on `fs.watch`.** Recursive watching works on Windows, macOS and Linux. On network file systems (NFS, SMB) and some container volume drivers, change events can be delayed or missing.
- **Stale locks are not detected automatically.** After a crash the next writer has to be opened with `LOCK_RECLAIM`, and you have to make sure the previous writer is really gone.

## Runtime behaviour

- **Synchronous file I/O.** Commits and reads use synchronous file system calls and block the event loop while they run. This is fast for small sequential appends, but long reads (e.g. replaying millions of events) in a web server process delay other requests. Run large replays in a worker or a separate read-only process.
- **Index entries are cached.** Index entries that have been read are kept in memory until the index is closed, so a full scan of a very large index holds all its entries in memory.
- **Durability is opt-in.** Without `syncOnFlush`, acknowledged commits can be lost on an OS crash or power loss; see [Guarantees](guarantees.md#durability).

## Sizes and number ranges

| Limit | Value | Reason |
|-------|-------|--------|
| Events per store | 4,294,967,295 | Sequence numbers are uint32 |
| Size of one partition file | 4 GiB | Positions in index entries are uint32. With one partition per stream this limits a single stream; use a custom `partitioner` (e.g. time-based) for larger volumes |
| Size of one serialized event | 64 MB | Checked on write |
| Partition metadata | 4 KB | Header limit, relevant for `streamMetadata` |
| Partition names | Must not end in `~<number>` | Reserved for partition id collisions, see below |

**Partition id collisions.** Index entries reference a partition by a 32-bit hash of its file name. With randomly distributed names, two partitions with the same hash become likely somewhere above 50,000 partitions. When the writer creates a partition whose id is already taken by another file, it stores it in `<name>~1` instead (then `~2`, …), so every partition keeps its own file and metadata. Because of this, new partition names ending in `~<number>` are rejected.

## Data management

- **Events cannot be deleted or changed.** `deleteEventStream()` removes an index, not the events. For personal data that may have to be erased, store it encrypted with a per-subject key and delete the key ("crypto shredding"), or keep it outside the event store.
- **No built-in schema evolution.** Upcasting old event versions is up to the application, for example in a custom serializer or when applying events.
- **No ad-hoc queries.** Reads are range scans over stream indexes, combined with selectors and [matchers](matchers.md). Anything else is a projection you build yourself.
- **Many files.** Every stream is at least one index file and, by default, one partition file. Use [hierarchical stream names](organizing-streams.md#hierarchical-category-streams-categoryid) to keep directories small, and check the open file limits of your OS together with `maxOpenPartitions` / `maxOpenIndexes`.
