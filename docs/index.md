![node-event-storage](logo.png)

[![build](https://github.com/albe/node-event-storage/workflows/build/badge.svg?branch=main)](https://github.com/albe/node-event-storage/actions)
[![npm version](https://badge.fury.io/js/event-storage.svg)](https://badge.fury.io/js/event-storage)
[![Code Climate](https://codeclimate.com/github/albe/node-event-storage/badges/gpa.svg)](https://codeclimate.com/github/albe/node-event-storage)
[![Coverage Status](https://coveralls.io/repos/github/albe/node-event-storage/badge.svg?branch=main)](https://coveralls.io/github/albe/node-event-storage?branch=main)

# node-event-storage

An embedded event store for Node.js: events are appended to plain files in your application's data directory, with no database server to run.

```bash
npm install event-storage
```

---

## Where do you want to start?

**New to event sourcing?** Read [Event Sourcing in 5 Minutes](event-sourcing.md) for the core ideas and good references, then [When to Use node-event-storage](when-to-use.md) and the [Getting Started](getting-started.md) tutorial.

**Evaluating the implementation?** Start with [Guarantees](guarantees.md) (atomicity, concurrency control, isolation, durability, crash recovery), then [Storage Format](storage-format.md), [Limitations](limitations.md) and [Verify It Yourself](verify.md). Every guarantee names the code and tests that back it.

---

## A good fit

- Event-sourced desktop applications (Electron, NW.js) and single-server applications.
- One writer process, optionally with several reader processes for projections.
- Many small streams (thousands to millions) and fast sequential reads.
- Queryable append-only log storage.

## Not a good fit

- Several application servers writing to the same store, or replication across machines.
- Access from other languages or over the network (see [event-storage-http](https://github.com/albe/node-event-storage-http) for an HTTP layer).
- Ad-hoc queries over event contents beyond stream selection and matchers.

---

## Documentation map

| Section | Pages |
|---------|-------|
| Getting started | [Event Sourcing in 5 Minutes](event-sourcing.md), [When to Use](when-to-use.md), [Getting Started](getting-started.md) |
| Guides | [Writing and Reading Events](reading-writing.md), [Organizing Streams](organizing-streams.md), [Matchers](matchers.md), [DCB](dcb.md), [Consumers](consumers.md), [Multiple Processes](multi-process.md), [Customization](customization.md), [Operations](operations.md) |
| Guarantees and internals | [Guarantees](guarantees.md), [Storage Format](storage-format.md), [Implementation Architecture](architecture.md), [Performance](performance.md), [Limitations](limitations.md), [Verify It Yourself](verify.md) |
| Reference | [API](api.md), [Configuration](configuration.md), [Changelog](changelog.md), [Upgrading](upgrading/index.md) |
