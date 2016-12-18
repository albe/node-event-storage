[![Build Status](https://travis-ci.org/albe/node-event-store.svg?branch=master)](https://travis-ci.org/albe/node-event-store)

# node-event-store

An optimized event store for node.js

Disclaimer: This is currently under heavy development and not production ready.

## Why?

There is currently only a single event store implementation for node/javascript, namely https://github.com/adrai/node-eventstore

It is a nice project, but has a few drawbacks though:

  - its API is fully based around Event Streams, so in order to commit a new event the full existing Event Stream needs to be
      retrieved first. This makes it unfit for client application scenarios that frequently restart the application.
  - it has backends for quite a few existing databases, but none of them are optimized for event storage needs
  - it stores event publishing meta information in the events, so it does updates to event data
  - events are fixed onto one stream and it's not possible to create multiple streams that partially contain
      the same events. This makes creating projections hard and/or slow.

## Event-Storage and it's specifics

The thing that makes event storages stand out (and also makes them simpler and more performant), is that they
have no concept of overwriting or deleting data. They are purely append-only storages and the only querying is
sequential reading (possibly with some filtering applied): 

This means a couple of things:

  - no write-ahead log or transaction log required - the storage itself is the transaction log!
  - therefore writes are as fast as they can get, but you only can have a single writer
  - durability comes for free if write caches are avoided
  - reads and writes can happen lock-free, reads don't block writes and are always consistent (natural MVCC)
  - indexes are append-only and hence gain the same benefits
  - since only sequential reading is needed, indexes are simple file position lists - no fancy B+-Tree/fractal tree required
  - indexes are therefore pretty cheap and can be created in high numbers

Using any SQL/NoSQL database for storing events therefore is sub-optimal, as those databases do a lot of work on
top which is simply not needed. Write and read performance suffer.

## Use Cases

Event sourced client applications running on node.js (electron, node-webkit, etc.).
Small event sourced single-server applications that want to get near-optimal write performance.

## Installation

`npm install node-event-store`

## Run Tests

`npm test`

## Usage

```
const EventStore = require('node-event-store');

var eventstore = new EventStore('my-event-store', { storageDirectory: './data' });
eventstore.on('ready', () => {
    ...
    eventstore.commit('my-stream', [{ foo: 'bar' }], () => {
        ...
    });

    let stream = eventstore.getEventStream('my-stream');
    for (let event of stream) {
        ...
    }
});
```

### Creating additional streams

```
...
let myProjectionStream = eventstore.createStream('my-projection-stream', (event) => ['FooHappened', 'BarHappened'].includes(event.type));

for (let event of myProjectionStream) {
    ...
}
```

### Optimistic concurrency

```
    try {
        eventstore.commit('my-stream', [{ foo: 'bar' }], expectedVersion, () => {
            ...
        });
    } catch (e) {
        if (e instanceof EventStore.OptimisticConcurrencyError) {
            ...
            // Reattempt command / resolve conflict
        }
    }
```

Where `expectedVersion` is either `EventStore.ExpectedVersion.Any` (no optimistic concurrency check),
`EventStore.ExpectedVersion.EmptyStream` or any version number > 0 that the 'my-stream' is expected to be at.
It will throw an OptimisticConcurrencyError if the given stream version does not match the expected.

## Implementation details

### ACID

The storage engine is not strictly designed to follow ACID semantics. However, it has following properties:

#### Atomicity

A single document write is guaranteed to be atomic. Unless specifically configured, atomicity spreads to all subsequent
writes until the write buffer is flushed, which happens either if the current document doesn't fully fit into the write
buffer or on the next node event loop.
This can be (ab)used to create a reduced form of transactional behaviour: All writes that happen within a single event loop
and still fit into the write buffer will all happen together or not at all.
If strict atomicity for single documents is required, you can configure the option `maxWriteBufferDocuments` to 1, which
leads to every single document being flushed directly.

#### Consistency

Since the storage is append-only, consistency is automatically guaranteed.

#### Isolation

The storage is supposed to only work with a single writer, therefore writes do not influence each other obviously. The single
writer is not yet guaranteed by the storage itself however.
Reads are guaranteed to be isolated due to the append-only nature and a read only ever seeing writes that have finished
(not necessarily flushed - i.e. Dirty Reads) at the point of the read. Multiple reads can happen without blocking writes.

t.b.d. Dirty Reads, Lost Updates, Non-Repeatable Reads, Phantom Read

#### Durability

Durability is not strictly guaranteed due to the used write buffering and flushes not being synced to disk by default.
All writes happening within a single node event loop and fitting into the write buffer can be lost on application crash.
Even after flush, the OS and/or disk write buffers can still limit durability guarantees.
This is a trade-off made for increased write performance and can be more finely configured to needs.
The write buffer behaviour can be configured with the already mentioned `maxWriteBufferDocuments` and `writeBufferSize`
options. For strict durability, you can set the option `syncOnFlush` which will sync all flushes to disk before finishing,
but comes at a very high performance penalty of course.

Note: If there are any misconceptions on my side to the ACID semantics, let me know.

### Event Streams

There are two slightly different concepts of Event Streams:

  - A write stream is a single identifier that an event/document is assigned to on write (see Partitioning). It is therefore
    a physical separation of the events that happens on write. An event written to a specific write stream can not be removed
    from it, it can only be linked to from other additional (read) streams.

  - A read stream is an ordered sequence in which specific events are iterated when reading. Every write stream automatically
    creates a read stream that will iterate the events in the order they were written to that stream. Additional read streams
    can be created that possibly even sequence events from multiple write streams. Such read streams can be deleted without
    problem, since they will not actually delete the events, but just the specific iteration sequence.

An Event Stream is implemented as an iterator over an storage index. It is therefore limited to iterating the events at
the point the Event Stream was retrieved, but can be limited to a specific range of events, denoted by min/max revision.
It implements the node `ReadableStream` interface.

### Partitioning

By default, the Event Store is partitioned on (write) streams, so every unique stream name is written to a separate file.
This has several consequences:

  - subsequent reads from a single write stream are faster, because the events share more locality
  - every write stream has it's own write and read buffer, hence interleaved writes/reads will not trash the buffers
  - since writes are buffered, only writes within a single write stream will be flushed together, hence transactionality is not spread over streams
  - the amount of write streams is limited by the amount of files the filesystem can handle inside a single folder
  - if hard disk is configured for file based RAID, this will most likely lead to unbalanced load

If required, the partitioning behaviour can be configured with the `partitioner` option, which is a method with following signature:
`(string:document, number:sequenceNumber) -> string:partitionName`
i.e. it maps a document and it's sequence number to a partition name. That way you could for example easily distribute all writes
equally among a fixed number of arbitrary partitions by doing `(document, sequenceNumber) => 'partition-' + (sequenceNumber % maxPartitions)`.
This is not recommended in the generic case though, since it contradicts the consistency boundary that a single stream should give.
Many databases partition the data into Chunks (striding) of a fixed size, which helps with disk performance especially in RAID setups.
However, since SSDs become more the standard, the benefit of chunking data is becoming more limited.
