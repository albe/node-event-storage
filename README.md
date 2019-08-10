[![Build Status](https://travis-ci.org/albe/node-event-storage.svg?branch=master)](https://travis-ci.org/albe/node-event-storage) [![npm version](https://badge.fury.io/js/event-storage.svg)](https://badge.fury.io/js/event-storage)
[![Code Climate](https://codeclimate.com/github/albe/node-event-storage/badges/gpa.svg)](https://codeclimate.com/github/albe/node-event-storage)
[![Coverage Status](https://coveralls.io/repos/github/albe/node-event-storage/badge.svg?branch=master)](https://coveralls.io/github/albe/node-event-storage?branch=master)
[![dependencies Status](https://david-dm.org/albe/node-event-storage/status.svg)](https://david-dm.org/albe/node-event-storage)
[![Code documentation](https://inch-ci.org/github/albe/node-event-storage.svg?branch=master)](https://inch-ci.org/github/albe/node-event-storage)

# node-event-storage

An optimized embedded event store for modern node.js, written in ES6.

> **Disclaimer:** This is currently under heavy development and not production ready. See [issues/29](https://github.com/albe/node-event-storage/issues/29) for more information.

## Why?

There is currently only a single embedded event store implementation for node/javascript, namely https://github.com/adrai/node-eventstore

It is a nice project, but has a few drawbacks though:

  - its API is fully based around Event Streams, so in order to commit a new event the full existing Event Stream needs to be
      retrieved first. This makes it unfit for client application scenarios that frequently restart the application.
  - it has backends for quite a few existing databases (TingoDB, NeDB, MongoDB, ...), but none of them are optimized for event storage needs
  - the embeddable storage backends (TingoDB, NeDB) do not persist indexes and hence are very slow on initial load
  - it stores event publishing meta information in the events, so it does updates to event data
  - events are fixed onto one stream and it's not possible to create multiple streams that partially contain
      the same events. This makes creating projections hard and/or slow.

## Use Cases

Event sourced client applications running on node.js (electron, node-webkit, etc.).
Small event sourced single-server applications that want to get near-optimal write performance.
Using it as queryable log storage.

## Event-Storage and it's specifics

The thing that makes event storages stand out (and also makes them simpler and more performant), is that they
have no concept of overwriting or deleting data. They are purely append-only storages and the only querying is
sequential (range) reading (possibly with some filtering applied): 

This means a couple of things:

  - no write-ahead log or transaction log required - the storage itself is the transaction log!
  - therefore writes are as fast as they can get, but you only can have a single writer
  - durability comes for free if write caches are avoided
  - reads and writes can happen lock-free, reads don't block writes and are always consistent (natural MVCC)
  - indexes are append-only and hence gain the same benefits
  - since only sequential reading is needed, indexes are simple file position lists - no fancy B+-Tree/fractal tree required
  - indexes are therefore pretty cheap and can be created in high numbers
  - creating backups is easily doable with rsync or by creating file copies on the fly

Using any SQL/NoSQL database for storing events therefore is sub-optimal, as those databases do a lot of work on
top which is simply not needed. Write and read performance suffer.

## Installation

`npm install event-storage`

## Run Tests

`npm test`

## Usage

```javascript
const EventStore = require('event-storage');

const eventstore = new EventStore('my-event-store', { storageDirectory: './data' });
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

```javascript
...
let myProjectionStream = eventstore.createStream('my-projection-stream', (event) => ['FooHappened', 'BarHappened'].includes(event.type));

for (let event of myProjectionStream) {
    ...
}
```

### Optimistic concurrency

```javascript
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

### Consumers

Consumers are durable event-driven listeners on event streams. They provide at-least-once delivery guarantees,
meaning that they receive each event in the stream at least once. An event can possibly be delivered twice if
the program crashed during the handling of an event, since the current position will only be persisted *afterwards*.

```javascript
let myConsumer = eventstore.getConsumer('my-stream', 'my-stream-consumer1');
myConsumer.on('data', event => {
    // do something with event, but be sure to de-duplicate or have idempotent handling
});
```

**Note**
> The consuming of events will start as soon as a handler for the `data` event is registered and suspended
> when the last listener is removed.

As soon as the consumer has caught up the stream, it will emit a `caught-up` event.

#### Exactly-Once semantics

Since version 0.6 the consumers can persist their state (a simple JSON object), which allows for achieving
exactly-once processing semantics relatively easy. What this means is, that the state of the consumer will
always reflect the state of having each event processed exactly once, because if persisting the state fails,
the position is also not updated and vice versa.

```javascript
let myConsumer = eventstore.getConsumer('my-stream', 'my-stream-consumer1');
myConsumer.on('data', event => {
    const newState = { ...myConsumer.state, projectedValue: myConsumer.state.projectedValue + event.someValue };
    myConsumer.setState(newState);
});
```

This is very useful for projecting some data out of a stream with exactly-once processing without a lot of effort.
Whenever the state is persisted, the consumer will also emit a `persisted` event.

**Note**
> Never mutate the consumers `state` property directly and only use the `setState` method **inside** the `data` handler.

The reason this is works is, that conceptually the state update and the position update happens within a single
transaction. So anything you can wrap inside a transaction with storing the position yields exactly-once semantics.
However, for example sending an email exactly once for every event is not achievable with this, because you can't
wrap a transaction around sending an e-mail and persisting the consumer position in a local file easily.

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

If Dirty Reads are not wanted, they can be disabled with the storage configuration option `dirtyReads` set to false. That
way you will only ever be able to read back documents that where flushed to disk.

t.b.d. Lost Updates, Non-Repeatable Reads, Phantom Read

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

### Compression

To apply compression on the storage level, the `serializer` option of the Storage can be used.

For example to use LZ4:

```javascript
const lz4 = require('lz4');
const eventstore = new EventStore('my-event-store', {
	storageDirectory: './data',
	storageConfig: {
		serializer: {
			serialize: (doc) => {
				return lz4.encode(Buffer.from(JSON.stringify(doc))).toString('binary');
			},
			deserialize: (string) => {
				return JSON.parse(lz4.decode(Buffer.from(string, 'binary')));
			}
		}
	}
});
```

Since compression works on a per document level, compression efficiency is reduced. This is currently necessary
to allow fully random access of single documents without having to read a large block before.
If available, use a dictionary for the compression library and fill it with common words that describe
your event/document schema and the following terms:

- "metadata":{"committedAt":
- ,"commitId":
- ,"commitVersion":
- ,"streamVersion":

### Security

When specifying a matcher function for streams/indexes those matcher functions will be serialized into the index
file and be `eval`'d on later loading for convenience to not having to specify the matcher when reopening.
In order to prevent some malicious attacker from executing arbitrary code in your application by altering an index
file, the matcher function gets fingerprinted with an HMAC.
This HMAC is calculated with a secret that you should specify with the `hmacSecret` option of the storage
configuration.

Currently the `hmacSecret` is an optional parameter defaulting to an empty string, which is unsecure, so always
specify an own unique random secret for this in production.

Alternatively you should always explicitly specify your matchers when opening an existing index, since that will
check that the specified matcher matches the one in the index file.
