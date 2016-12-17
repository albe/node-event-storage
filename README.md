https://travis-ci.org/albe/node-event-store.svg?branch=master

# node-event-store

An optimized event store for node.js

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

An Event Stream is implemented as an iterator over an storage index. It is therefore limited to iterating the events at
the point the Event Stream was retrieved, but can be limited to a specific range of events, denoted by min/max revision.

