# Dynamic Consistency Boundaries (DCB)

## What is DCB?

**Dynamic Consistency Boundary** (DCB) is a pattern for event-sourced systems where the unit of consistency — the "transaction boundary" — is defined at command-handling time rather than fixed to a single aggregate stream.

In traditional event-sourcing each command targets one aggregate (e.g. `order-42`) and optimistic concurrency is enforced by checking that the aggregate's stream is at the expected version.

DCB generalises this:

- The consistency boundary is expressed as a **query**: _a set of event types_ (and an optional additional filter function) that the command handler cares about.
- A **condition** is obtained before reading, capturing the current global position in the event log.
- After building the transaction context from those events the handler commits its new events together with the condition.
- The commit engine checks: did any new events appear that satisfy the original query? If yes, a conflict is raised; if no, the commit succeeds.

This removes the need to route every command through a fixed aggregate, enabling commands that span multiple entities while retaining strong consistency guarantees without distributed locks.

Consider the classic DCB example from Sara Pellegrini's talk: students subscribing to courses. A course raises `CourseCreated`, `CourseRenamed`, and `CourseCapacityChanged`; a student raises `StudentCreated`. The event `StudentSubscribedToCourse` is ambiguous — it concerns both a Student and a Course — so in a traditional aggregate model it is unclear which aggregate owns it. Meanwhile the Course aggregate keeps growing with new event types:

![Traditional aggregate model — the Course aggregate grows with CourseCreated, CourseRenamed and CourseCapacityChanged while StudentSubscribedToCourse is ambiguous between the Course and Student aggregates](diagram-dcb-aggregate-ambiguity.svg)

DCB resolves this by letting each command handler declare exactly which events it needs to be consistent with — a **dynamic** boundary defined at execution time, not at design time. Different decisions (commands) define different boundaries, and those boundaries can freely overlap. The `StudentSubscribedToCourse` event ends up inside a cross-aggregate boundary that also shares events with the course-only boundaries:

![Multiple overlapping Dynamic Consistency Boundaries per decision — Rename Course, Change Course Capacity, and the cross-aggregate Subscribe Student to Course boundary](diagram-dcb-boundary.svg)

---

## Enabling DCB-style Queries with `typeAccessor`

To use `query()` the store needs per-type stream indexes.  Configure `typeAccessor` to have those indexes created automatically on every commit:

```javascript
import { EventStore } from 'event-storage';

const store = new EventStore('my-store', {
    storageDirectory: './data',
    typeAccessor: (event) => event.type   // tell the store how to read the event type
});
```

`typeAccessor` is a function `(event) => string` that returns the event type for a given event payload.  When configured:

1. **Type streams are created automatically on commit.**  Before writing each event to its entity stream (e.g. `order-42`), the store ensures a dedicated type-stream index exists for the value returned by `typeAccessor`.  Because the index is created just before the write, it is always complete — no historical scan is needed (`reindex=false`).

2. **`query()` treats missing type streams as empty (0-length)** rather than throwing.  A missing stream simply means no event of that type has been committed yet, which is a valid and expected state when the store is new or the type has never been used.

3. **Any stream name can be used for entity partitioning.**  You are free to commit events to whatever stream names make sense for your domain (e.g. `order-42`, `customer-7`).  The type index is maintained as a separate secondary index independently of the entity stream.

When `typeAccessor` returns a falsy value for an event (e.g. the event has no `type` field), no type stream is created for that event — it is treated as "untyped" and skipped silently.

---

## The DCB Workflow

### Step 1 — Query for the Transaction Context

```javascript
const { stream, condition } = store.query(
    ['OrderPlaced', 'OrderShipped'],  // event types to watch
    (event, metadata) => event.orderId === 'order-42'  // optional additional filter
);
```

- `stream` — an `EventStream` pre-filtered to events of the given types that also satisfy the optional matcher.  Iterate it to build the transaction context.
- `condition` — an accept condition capturing the current global event-log position.  Pass it to `commit()` to enforce the concurrency check.

The optional `matcher` is a function `(payload, metadata) => boolean` that qualifies **which events are in scope**.  Only events for which the matcher returns truthy appear in `stream`, and only those events can cause a conflict at commit time.

Domain identifiers and tags should be encoded in the `matcher` for now.  A future release may introduce a native tag-based query syntax.

### Step 2 — Build the Transaction Context

```javascript
const model = new OrderModel();

stream.forEach((event, metadata) => {
    model.apply(event);
});
```

Because `stream` is already filtered by both the type list and the matcher, you can iterate it directly to build your decision model.

### Step 3 — Commit with the Condition

```javascript
try {
    store.commit('order-42', [{ type: 'OrderShipped', orderId: 'order-42', ... }], condition, () => {
        console.log('Committed successfully');
    });
} catch (e) {
    if (e instanceof EventStore.OptimisticConcurrencyError) {
        // A conflicting event appeared — replay and retry
    }
}
```

The stream name passed to `commit()` is the entity stream (e.g. `'order-42'`).  The `typeAccessor` takes care of maintaining the type index so that subsequent `query()` calls remain O(1).

---

## Conflict Semantics

The commit engine guarantees that under the scope of the query condition, no event matching the query has appeared since the condition was captured:

| Scenario | Result |
|----------|--------|
| No new events of any listed type appeared | ✅ No conflict |
| New events appeared, but none match the `matcher` | ✅ No conflict |
| New events appeared and at least one matches the `matcher` | ❌ `OptimisticConcurrencyError` |
| New events appeared and no `matcher` was provided | ❌ `OptimisticConcurrencyError` |

Unrelated concurrent writes (same event type but different business entity) never cause spurious conflicts — the optional `matcher` lets you narrow the boundary to exactly the events that would actually affect the decision.

---

## Full Example

```javascript
import { EventStore } from 'event-storage';

const store = new EventStore('accounts', {
    storageDirectory: './data',
    typeAccessor: (event) => event.type
});

store.on('ready', () => {

    function handleRegisterCustomer(command) {
        // Query for any prior registration with the same email
        const { stream, condition } = store.query(
            ['CustomerRegistered'],
            (event, metadata) => event.email === command.email
        );

        // Build state: is this email already taken?
        let emailTaken = false;
        stream.forEach((event) => {
            if (event.email === command.email) emailTaken = true;
        });

        if (emailTaken) {
            throw new Error(`Email ${command.email} is already registered`);
        }

        // Commit to the customer entity stream.
        // typeAccessor ensures the 'CustomerRegistered' type index is updated automatically.
        store.commit(`customer-${command.customerId}`, [
            { type: 'CustomerRegistered', customerId: command.customerId, email: command.email }
        ], condition);
    }

    handleRegisterCustomer({ customerId: 'cust-1', email: 'alice@example.com' });
});
```

---

## The DCB Specification: Types and Tags

The formal DCB specification (as described by Pellegrini) expresses a query as a list of **query items**, each pairing an array of event types with an array of **domain-identifier tags**:

```
queryItems = [
  { types: ['CourseCreated', 'CourseCapacityChanged', 'StudentSubscribedToCourse'],
    tags:  ['course:jdsj4'] },
  { types: ['StudentCreated', 'StudentSubscribedToCourse'],
    tags:  ['student:gfh3j'] }
]
```

An event matches the query when **any** query item matches it: the event's type must be in that item's `types` list **and** the event must carry **all** of that item's tags:

```
event matches query  ⟺  ∃ q ∈ queryItems :  event.type ∈ q.types  ∧  q.tags ⊆ event.tags
```

In node-event-storage this pattern is expressed today using the `matcher` function parameter of `query()`. The union of all types from every query item is passed as the `types` array, and the multi-item logic is encoded in the matcher:

```javascript
const courseId  = 'course:jdsj4';
const studentId = 'student:gfh3j';

const { stream, condition } = store.query(
    // Union of all types across both query items
    ['CourseCreated', 'CourseCapacityChanged', 'StudentCreated', 'StudentSubscribedToCourse'],
    // Matcher encodes the per-item (type, tags) logic
    (event, meta) =>
        (['CourseCreated', 'CourseCapacityChanged', 'StudentSubscribedToCourse'].includes(meta.stream)
            && meta.tags?.includes(courseId))
        ||
        (['StudentCreated', 'StudentSubscribedToCourse'].includes(meta.stream)
            && meta.tags?.includes(studentId))
);
```

Note that the type membership check in the matcher duplicates what is already expressed in the `types` array. This is a current limitation: node-event-storage does not yet support a native multi-item query API. Future versions may introduce tag-based secondary indexes and a structured query item format to avoid this duplication and to make tag-filtered queries as performant as type-filtered ones.

---

## `query()` Behaviour by Configuration

| Configuration | Missing type stream in `query()` | Type stream creation |
|---|---|---|
| Default (no `typeAccessor`) | **Throws** — create the stream first with `createEventStream()` or use type-named entity streams | Manual via `createEventStream()` |
| With `typeAccessor` | Treated as **empty** (0-length) | Automatic on each `commit()` |

In the default configuration (no `typeAccessor`) `query()` will throw if any of the listed type streams do not exist.  This prevents accidental full-store scans: if a stream was never created the store cannot know whether matching events exist.

When using type-named entity streams (stream name = event type, e.g. `commit('OrderPlaced', ...)`) the type stream will exist as soon as the first event of that type has been committed, so `query()` will work without `typeAccessor` as long as every type listed has had at least one event committed.

When `typeAccessor` is configured, type streams are maintained automatically and `query()` will never throw for a missing type — it simply returns an empty stream for types that have not yet seen any events.

---

## Important Caveats

### `typeAccessor` is for new stores

Type indexes created via `typeAccessor` are built with `reindex=false`.  This means they only track events written *after* the index was first created.  If you open a pre-existing store and add `typeAccessor` for the first time, the type indexes will be incomplete for events that were written before.

**Always configure `typeAccessor` from the beginning** if you intend to use `query()` with automatic type indexes.

