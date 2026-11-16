# Event Sourcing in 5 Minutes

Event sourcing stores **what happened** instead of **what is**. Rather than updating a row with the current state of an order, an application appends facts like `OrderPlaced`, `ItemAdded` and `OrderShipped`. The current state is derived by replaying those facts. Nothing is ever updated or deleted, so the full history is always available.

## Core ideas

**Event** — an immutable fact in past tense, e.g. `{ type: 'OrderPlaced', orderId: 42, total: 99 }`. Events are the source of truth.

**Stream** — an ordered sequence of events that belong together, usually all events of one entity such as `order-42`. Each event has a version (its position) within its stream.

**Commit** — appending one or more events to a stream as one atomic unit.

**Projection (read model)** — state computed by processing events, for example "all open orders" or "revenue per day". Projections can be rebuilt at any time from the events, and you can add new ones later over the full history.

**Optimistic concurrency** — to decide whether a command is allowed, an application reads a stream, checks its business rules and appends new events with the version it read. If another writer appended in the meantime, the commit is rejected and the command is retried with fresh state. This replaces locks.

**Aggregate / consistency boundary** — the set of events a decision must be consistent with. Classic event sourcing uses one stream per aggregate. [Dynamic Consistency Boundaries](dcb.md) instead define that set per command with a query, which helps when a decision spans several entities.

**CQRS** — event sourcing is often combined with Command Query Responsibility Segregation: commands write events, queries read from projections that are optimized for each use case.

## Why do this?

- **Audit and debugging:** you know not only the current state but how it came about.
- **New views on old data:** a new projection can be built from all past events.
- **Simple writes:** appending to a log is fast and needs no complex locking.
- **Integration:** other parts of a system can react to events as they happen.

The cost is a different way of thinking: you design events instead of tables, think about how event schemas evolve, and accept that projections are updated after the write (eventual consistency between write and read side).

## How it maps to node-event-storage

| Concept | In node-event-storage |
|---------|-----------------------|
| Event | Any JSON-serializable object passed to `commit()` |
| Stream | A named stream; created on first commit. See [Writing and Reading Events](reading-writing.md) |
| Commit | `eventstore.commit(stream, events, expectedVersion)` |
| Optimistic concurrency | `expectedVersion`, or a `condition` from `query()` for DCB |
| Projection | A [consumer](consumers.md) that processes events and persists its state, or a [derived stream](organizing-streams.md#derived-streams) |

## Further reading

- Martin Fowler: [Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) — the classic short introduction.
- Greg Young: [CQRS Documents](https://cqrs.files.wordpress.com/2010/11/cqrs_documents.pdf) — the background of CQRS and event sourcing.
- Greg Young: [Versioning in an Event Sourced System](https://leanpub.com/esversioning) — how to evolve event schemas.
- Oskar Dudycz: [event-driven.io](https://event-driven.io/en/) — many practical articles on event sourcing.
- [Event Modeling](https://eventmodeling.org) — a method for designing systems around events.
- [dcb.events](https://dcb.events) — the Dynamic Consistency Boundary specification.

Next: [When to Use node-event-storage](when-to-use.md) or straight to [Getting Started](getting-started.md).
