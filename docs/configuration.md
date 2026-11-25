# Configuration

All options in one place. `EventStore` options are passed as the second constructor argument; the storage options go into `config.storageConfig` (or directly into `new Storage(name, config)` when using the low-level class).

```javascript
import { EventStore } from 'event-storage';

const eventstore = new EventStore('my-store', {
    storageDirectory: './data',
    typeAccessor: 'type',
    storageConfig: { syncOnFlush: true }
});
```

## EventStore options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `storeName` (1st argument) | `string` | `'eventstore'` | Prefix for all storage files. |
| `storageDirectory` | `string` | `'./data'` | Directory for data files. |
| `streamsDirectory` | `string` | `'{storageDirectory}/streams'` | Directory for stream index files. |
| `readOnly` | `boolean` | `false` | Open the store in read-only mode. See [Multiple Processes](multi-process.md). |
| `typeAccessor` | `string` | — | Dot-notation path to the event type. Maintains one stream per type value and enables `types` in `DcbQuery`. See [DCB](dcb.md). |
| `tagsAccessor` | `string` | — | Dot-notation path to an array of tag strings. Maintains one stream `tags/<tag>` per tag value and enables `tags` in `DcbQuery`. |
| `streamSources` | `Array<{ path, nameBuilder }>` | `[]` | Generic automatically maintained streams. See [Organizing Streams](organizing-streams.md#automatically-maintained-streams). |
| `streamMetadata` | `object\|function` | — | Metadata stored once per stream at creation: a `{ streamName: metadata }` map or `(streamName) => object`. Used by [access control hooks](customization.md#access-control-hooks). Ignored when `storageConfig.metadata` is set. |
| `storageConfig` | `object` | `{}` | Storage options, see below. |

## Storage options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `dataDirectory` | `string` | `storageDirectory` | Directory for partition (event) files. |
| `indexDirectory` | `string` | `streamsDirectory` | Directory for index files. |
| `indexFile` | `string` | `'{storeName}.index'` | File name of the primary index. |
| `partitioner` | `function(document, sequenceNumber)` | one partition per stream | Returns the partition (file) name for a document. `EventStore` uses `(document) => document.stream`; the bare `Storage` class writes everything to one partition. See [Partitioning](customization.md#partitioning). |
| `serializer` | `{ serialize, deserialize }` | JSON | Custom serialization or compression. See [Customization](customization.md#custom-serialization). |
| `readBufferSize` | `number` | `4096` | Read buffer size in bytes. |
| `writeBufferSize` | `number` | `16384` | Write buffer size in bytes. |
| `maxWriteBufferDocuments` | `number` | `0` (unlimited) | Flush after this many buffered documents. |
| `syncOnFlush` | `boolean` | `false` | Call `fsync` on every flush for strict durability. |
| `syncInterval` | `number` | `0` (off) | Group commit: call `fsync` on every partition written at most this many milliseconds later, in one shared run, and only then invoke the commit callbacks. Ignored when `syncOnFlush` is set. See [Durability](guarantees.md#durability). |
| `dirtyReads` | `boolean` | `true` | Let the writer read events that are still in the write buffer. |
| `lock` | `LOCK_AUTO\|LOCK_THROW\|LOCK_RECLAIM` | `LOCK_AUTO` | How to handle an existing lock on open: reclaim it only if its owner process is gone, never, or always. See [Operations](operations.md#crash-recovery). |
| `hmacSecret` | `string` | `''` | Secret used to verify persisted function matchers. See [Security](customization.md#security). |
| `metadata` | `object\|function(partitionName)` | — | Metadata written to each partition header (low-level alternative to `streamMetadata`). |
| `matcherProperties` | `string[]` | `['stream', 'payload.type']` | Document paths used for O(1) routing of writes to indexes. See [Performance](performance.md#matcherproperties). |
| `maxOpenPartitions` | `number` | `1024` | Maximum open partition file descriptors (`0` = unlimited). See [Performance](performance.md#maxopenpartitions). |
| `maxOpenIndexes` | `number` | `1024` | Maximum open index file descriptors (`0` = unlimited). |
| `indexOptions` | `object` | `{}` | Options passed to every index on construction. |

Read-only storages ignore the write-related options (`writeBufferSize`, `maxWriteBufferDocuments`, `syncOnFlush`, `syncInterval`, `dirtyReads`, `partitioner`, `lock`).
