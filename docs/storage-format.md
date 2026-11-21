# Storage Format

Everything node-event-storage persists is in plain files that you can inspect with standard tools. This page describes their layout, so you can judge the crash-safety mechanisms in [Guarantees](guarantees.md) or write your own reader.

All multi-byte integers in data files are big-endian; index entries are little-endian.

## Files on disk

For a store named `eventstore` with default settings (`storageDirectory: './data'`):

```
data/
  eventstore.lock/                      lock directory, exists while a writer is open
    owner.lock                          JSON: pid, hostname, pidNamespace, lockedAt of the writer
  eventstore.order-42                   partition: all events of stream "order-42"
  eventstore.user/                      partition directory for hierarchical names ("user/1", "user/2")
    1
    2
  eventstore.account-8444~1             partition whose id collided with another one (see Index entries)
  eventstore.order-42-truncated-<time>-<pos>.branch   bytes removed by crash repair (only after a repair)
  streams/
    eventstore.index                    primary index: every event, in global order
    eventstore.stream-order-42.index    stream index
    eventstore.stream-done.closed.index index of a closed stream
    eventstore.stream-user/1.index      stream index for a hierarchical name
    consumers/
      eventstore.stream-order-42.my-projection   consumer position and state
```

- **Partitions** hold the events. `EventStore` writes each stream into its own partition, named `<storeName>.<streamName>`. A custom `partitioner` changes this mapping.
- **Indexes** hold positions only. Every stream, derived stream, type and tag stream is one index file. The primary index covers all events.
- **The lock** is a directory, because `mkdir` either succeeds or fails atomically on all supported platforms. The `owner.lock` file inside lets the next writer decide whether the lock belongs to a process that is gone. While a writer reclaims a stale lock, it creates a `reclaim.lock` directory inside it, so that only one writer removes it.

## File header

Partition and index files start with the same header:

| Offset | Size | Content |
|--------|------|---------|
| 0 | 8 | Magic and format version: `nesprt03` for partitions, `nesidx01` for indexes |
| 8 | 4 | `n` = length of the metadata block (uint32 BE) |
| 12 | `n` | Metadata as JSON, padded with spaces and ending in `\n`, so the header size is a multiple of 16 |

Partition metadata contains `epoch` (ms timestamp the partition's clock counts from) plus any user metadata from `streamMetadata`. Index metadata contains the entry class and size, and for derived streams the `matcher` (an object, or the source of a function plus its `hmac`). Partition metadata is limited to 4 KB.

A reader refuses files whose magic does not match its format version, so an older library never misreads a newer format.

## Document envelope

After the header, a partition is a sequence of documents. Positions are byte offsets relative to the end of the header and are always multiples of 4.

| Size | Content |
|------|---------|
| 4 | `dataSize`: byte length of the serialized document (uint32 BE) |
| 4 | Global sequence number, 0-based (uint32 BE) |
| 8 | Timestamp in microseconds since the partition epoch (float64 BE) |
| `dataSize` | Serialized document (JSON by default), UTF-8 |
| 0–3 | Spaces, so the whole document is 4-byte aligned |
| 4 | `dataSize` again (uint32 BE) |
| 4 | Separator `00 00 1E 0A` |

The size footer allows reading backwards from the end of a file and detecting a torn write: if the header announces more bytes than the file contains, the document is incomplete. The trailing `\n` puts every JSON document on its own line, so partitions can be searched with text tools such as `grep -a`.

The serialized document is `{ "stream": …, "payload": …, "metadata": { … } }`. The metadata fields written by `commit()` are:

| Field | Meaning |
|-------|---------|
| `commitId` | Global position (0-based) at which the commit started |
| `committedAt` | Commit time in ms (`Date.now()`) |
| `commitVersion` | 0-based position of the event within its commit |
| `commitSize` | Number of events in the commit |
| `streamVersion` | 0-based position of the event within its stream |

## Index entries

After the header, an index file is an array of fixed 16-byte entries:

| Offset | Size | Content |
|--------|------|---------|
| 0 | 4 | Global sequence number, 1-based (uint32 LE) |
| 4 | 4 | Byte position of the document in its partition (uint32 LE) |
| 8 | 4 | `dataSize` of the document, used to verify the read (uint32 LE) |
| 12 | 4 | Partition id: a 32-bit hash of the partition file name (uint32 LE). If two names have the same hash, the writer stores the second partition as `<name>~1`, so ids stay unique within a store |

Entries are sorted by sequence number, so a lookup by revision is an array access and a lookup by global position is a binary search. Because entries have a fixed size, an index can be truncated to any entry and rebuilt from the partitions by scanning their document headers (`reindex()`).

## Consumer files

A consumer file contains the position (uint32 LE, 4 bytes) followed by the consumer state as UTF-8 JSON. It is replaced atomically by writing a temporary file `<name>.<position>.tmp` and renaming it.

## Inspecting a store

```bash
# Header and first document of a partition
od -A d -c data/eventstore.order-42 | head -20

# All events of a partition as JSON lines (JSON serializer only)
grep -a -o '{"stream".*}' data/eventstore.order-42

# Number of events in the store: (file size - header size) / 16
stat -c %s data/streams/eventstore.index
```
