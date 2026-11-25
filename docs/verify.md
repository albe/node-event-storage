# Verify It Yourself

Every guarantee on [Guarantees](guarantees.md) can be checked against the code, the tests and the stress test in this repository. This page tells you where to look and how to run them.

```bash
git clone https://github.com/albe/node-event-storage.git
cd node-event-storage
npm ci
```

## Where each claim is backed

| Claim | Code | Tests or tools |
|-------|------|----------------|
| Single writer | `StorageLock` | `test/Storage.spec.js`: "allows only a single writer" |
| Stale locks are reclaimed only when their owner is gone | `StorageLock.reclaimOrphaned()`, `StorageLock.isOwnerGone()` | `test/StorageLock.spec.js`: "does not reclaim the lock of a running process", "does not reclaim the lock of a process on another host or in another pid namespace", "does not reclaim a stale lock that another writer is already reclaiming"; `test/Storage.spec.js`: "reclaims the lock of a process that is gone and repairs the storage" |
| Optimistic concurrency | `EventStore.commit()` | `test/EventStore.spec.js`: "throws an optimistic concurrency error if stream version does not match" |
| DCB conditions | `EventStore.query()`, `EventStore.checkCondition()` | `test/EventStore.spec.js`: tests under `query`, e.g. "causes an OCC error when a matched event appeared since the condition" |
| Unfinished commits are rolled back | `EventStore.checkUnfinishedCommits()` | `test/EventStore.spec.js`: "repairs unfinished commits", "does not lose previously committed data after repairing an unfinished commit" |
| Torn writes are detected and repaired | `ReadablePartition.readFrom()`, `WritableStorage.checkTornWrites()`, `WritablePartition.truncateAfterSequence()` | `test/Partition.spec.js`: "throws when an unfinished write is found", "correctly truncates torn writes"; `test/EventStore.spec.js`: "repairs torn writes"; `test/Storage.spec.js`: "auto-repairs primary index when both torn write and index lag are present" |
| Dirty reads and their opt-out | `WritablePartition.prepareReadBuffer()` | `test/Partition.spec.js`: "can read unflushed documents", "can disable dirty reads" |
| Group commit with `syncInterval` | `SyncScheduler`, `WritablePartition.flush()` | `test/Partition.spec.js`: "invokes write callbacks only after one shared fsync", "syncs the directory when creating a new file in the next run"; `test/Storage.spec.js`: "syncs all partitions written within the interval in one background run"; `test/SyncScheduler.spec.js` |
| Bounded loss after a hard crash | write buffers in `WritablePartition` | `stress-test/` (below) |
| Writes beyond the 32-bit limits are rejected | `WritableStorage.write()` | `test/Storage.spec.js`: "rejects a document that would start beyond 4 GiB of its partition without writing it" |
| Storage format | `ReadablePartition`, `WritablePartition`, `IndexEntry`, `utils/metadataUtil.js` (`buildMetadataHeader`) | [Storage Format](storage-format.md), inspect files with `od` |
| Performance numbers | `bench/` | [Performance](performance.md) (below) |

## Unit tests

```bash
npm test                         # all tests with coverage (c8 + mocha)
npm run test:grep -- "torn"      # only tests whose name matches
```

Tests write to `test/data/` and clean it before each test.

## Crash safety

The stress test starts a writer that commits to several streams in a tight loop, kills it with `SIGKILL` after a configurable time, reopens the store, which reclaims the lock of the killed writer and repairs the store, and compares what is in the store with what the writer had acknowledged:

```bash
cd stress-test
WRITE_DURATION=10 bash run.sh
```

It prints the number of acknowledged events, the number found after repair, and checks the loss against the bound derived from the writer's buffer settings. The exit code is `0` when the loss is within bounds. In CI the test runs on demand via the `stress-test` workflow (`.github/workflows/stress-test.yml`).

`SIGKILL` simulates a process crash. It does not simulate power loss; for that, the relevant settings are `syncInterval` and `syncOnFlush` (see [Guarantees](guarantees.md#durability)).

## Benchmarks

```bash
cd bench
npm install
npm run bench
```

The benchmarks compare the current working copy with the latest published npm release. `bench-scalability.js` produces the partition and index scaling numbers on the [Performance](performance.md) page and `bench-matcher.js` the matcher numbers. The script behind the DCB scenario numbers is not part of the repository yet. Results depend on hardware and file system, so compare relative numbers rather than absolute ones.

## Inspecting a store

Data files are readable with standard tools; see [Storage Format](storage-format.md#inspecting-a-store) for commands.
