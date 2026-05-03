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

Consider the classic DCB example: students signing up for courses. In a traditional aggregate model, `StudentSignedUpForCourse` is ambiguous — it concerns both a Student and a Course, so it is unclear which aggregate owns it.

![Traditional aggregate model showing StudentSignedUpForCourse as ambiguous between the Student and Course aggregates](diagram-dcb-aggregate-ambiguity.svg)

DCB resolves this by letting the command handler declare exactly which events it needs to be consistent with — a **dynamic** boundary defined at execution time, not at design time. The relevant events are grouped by the query rather than by the aggregate. Note that the aggregate boundaries (dotted) still exist and can be used for their own consistency checks — but they intersect the DCB boundary, sharing some events:

![Dynamic Consistency Boundary for course enrolment spanning StudentEnrolled, StudentSignedUpForCourse and CourseCreated, with StudentNameChanged outside the boundary](diagram-dcb-boundary.svg)

---

## DCB Mode

DCB mode is an opt-in store configuration that optimises for query-centric (rather than aggregate-centric) workloads.

```javascript
import { EventStore } from 'event-storage';

const store = new EventStore('my-store', {
    storageDirectory: './data',
    dcbMode: true          // <-- enable DCB mode
});
```

In DCB mode:

1. **The stream name is the event type**.  Users commit events to type-named streams (e.g. `'OrderPlaced'`, `'OrderShipped'`) rather than entity-scoped streams.  This keeps events of each type in their own partition file.

2. **Type-stream indexes are created without a historical scan** (`reindex=false`).  For a store created in DCB mode from day one this is correct: every event has always been written to its type stream, so no backfill is needed.

3. **`query()` requires no store scan** because the type streams are always up to date.

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
    store.commit('OrderShipped', [{ orderId: 'order-42', ... }], condition, () => {
        console.log('Committed successfully');
    });
} catch (e) {
    if (e instanceof EventStore.OptimisticConcurrencyError) {
        // A conflicting event appeared — replay and retry
    }
}
```

The stream name passed to `commit()` in DCB mode should be the event type name (e.g. `'OrderShipped'`).

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
    dcbMode: true
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

        // Commit — the condition ensures no concurrent registration with the same email slips through
        store.commit('CustomerRegistered', [
            { customerId: command.customerId, email: command.email }
        ], condition);
    }

    handleRegisterCustomer({ customerId: 'cust-1', email: 'alice@example.com' });
});
```

---

## Standard Mode vs DCB Mode

`query()` works in both standard and DCB mode.

| Feature | Standard mode | DCB mode |
|---------|---------------|----------|
| Stream naming convention | Entity streams (e.g. `order-42`) | Type-named streams (e.g. `OrderPlaced`) |
| Type stream matcher | `{ payload: { type } }` (scans all streams) | `{ stream: type }` (matches stream name) |
| Type stream creation | `reindex=true` on first use (one-time scan) | `reindex=false` (instant) |
| `query()` first call | May scan existing events | O(1) always |
| `query()` subsequent calls | O(1) | O(1) |

In **standard mode** the first call to `query()` for a given event type creates a secondary index by scanning all existing events (`reindex=true`). This is a one-time cost proportional to the store size; all subsequent calls are O(1).

If this one-time scan is undesirable you can pre-build a persistent multi-type index yourself before calling `query()`. Object matchers support arrays as OR conditions, and each value is routed in O(1) on writes via the discriminant optimisation:

```javascript
// Pre-build a multi-type index so the first query() call skips the scan.
eventstore.createEventStream('order-events', {
    payload: { type: ['OrderPlaced', 'OrderShipped', 'OrderCancelled'] }
});
```

---

## Important Caveats

### DCB mode is for new stores

Type indexes in DCB mode are created with `reindex=false`.  This means they only track events written *after* the index was first created.  If you open a pre-existing store in DCB mode, the type indexes will be incomplete and the condition will not reflect the full history.

**Always create a store with `dcbMode: true` from the beginning** if you intend to use DCB mode.

### Stream imbalance in DCB mode

In DCB mode every event of a given type shares a single partition file. High-volume event types accumulate large files and flush the write buffer more frequently, while rare types accumulate very slowly. This imbalance is an expected trade-off of the type-stream design: it enables O(1) queries at the cost of uneven write amplification across streams.
