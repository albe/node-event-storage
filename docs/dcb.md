# Dynamic Consistency Boundaries (DCB)

## What is DCB?

**Dynamic Consistency Boundary** (DCB) is a pattern for event-sourced systems where the unit of consistency is defined at command-handling time rather than fixed to a single aggregate stream.

In traditional event-sourcing each command targets one aggregate (e.g. `order-42`) and optimistic concurrency is enforced by checking the aggregate stream's version.

DCB generalises this: the consistency boundary is expressed as a **query** over a set of event types (and an optional filter). Before reading, a **condition** captures the current global position in the event log. After building state from those events, the handler commits its new events together with the condition. The commit engine then checks whether any new matching events appeared — if so, a conflict is raised.

This removes the need to route every command through a fixed aggregate, enabling commands that naturally span multiple entities while retaining strong consistency guarantees.

Consider the classic example from Sara Pellegrini's talk: students subscribing to courses. The event `StudentSubscribedToCourse` concerns both a Student and a Course — in a traditional aggregate model it is unclear which aggregate owns it, and the Course aggregate keeps growing with every new event type:

![Traditional aggregate model — the Course aggregate grows while StudentSubscribedToCourse is ambiguous between Course and Student](diagram-dcb-aggregate-ambiguity.svg)

DCB resolves this by letting each command handler declare exactly which events it needs to be consistent with. Different commands define different boundaries that can freely overlap:

![Multiple overlapping Dynamic Consistency Boundaries — Rename Course, Change Course Capacity, and the cross-aggregate Subscribe Student to Course boundary](diagram-dcb-boundary.svg)

---

## The DCB Workflow

### Step 1 — Query for the transaction context

```javascript
const { stream, condition } = store.query(
    ['OrderPlaced', 'OrderShipped'],                           // event types to watch
    (event, metadata) => event.orderId === 'order-42'          // optional filter
);
```

- `stream` — an `EventStream` filtered to events of the given types that also satisfy the optional matcher.
- `condition` — captures the current global event-log position; pass it to `commit()` to enforce the concurrency check.

The optional `matcher` narrows the boundary to exactly the events that would affect the decision — unrelated events of the same type (e.g. a different order) never cause spurious conflicts.

That `matcher` can be either a predicate function or the same object-matcher syntax used by streams: nested equality, array values with OR semantics, and scalar operators like `$gte` / `$lt`. See [Event Streams -> Object Matcher Syntax](streams.md#object-matcher-syntax).

The first argument to `query()` accepts the full selector algebra — the same nested array structure as `fromStreams()`. A flat `['TypeA', 'TypeB']` is equivalent to a top-level OR join over those streams. Nested arrays express AND (intersection) at odd depths and OR at even depths. See [The DCB Specification: Types and Tags](#the-dcb-specification-types-and-tags) for complete examples.

`query()` also supports raw mode (`query(selector, matcher, minRevision, true)`), but raw streaming itself is a general stream-reading feature, not DCB-specific. See [Event Streams -> Reading Streams](streams.md#reading-streams) for the full raw-mode semantics and matcher behavior.

### Implementation detail: selector algebra when tags are indexed as streams

The important DCB concept is `types` + `tags` selection semantics. How this is executed is an implementation detail.

If tags are materialized as dedicated streams, DCB items naturally compile to nested selector algebra — which `query()` accepts directly, returning both the filtered stream and the concurrency condition in a single call.

If tags are not materialized as streams, the same semantics can be expressed entirely through matcher logic.

### Step 2 — Build the decision model

```javascript
const model = new OrderModel();
stream.forEach((event, metadata) => {
    model.apply(event);
});
```

### Step 3 — Commit with the condition

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

---

## Conflict Semantics

| Scenario | Result |
|----------|--------|
| No new events of any listed type appeared | ✅ No conflict |
| New events appeared, but none match the `matcher` | ✅ No conflict |
| New events appeared and at least one matches the `matcher` | ❌ `OptimisticConcurrencyError` |
| New events appeared and no `matcher` was provided | ❌ `OptimisticConcurrencyError` |

---

## Enabling DCB-style Queries with `typeAccessor`

`query()` requires a named stream per event type. Configure `typeAccessor` to have those streams created and maintained automatically on every `commit()`:

```javascript
const store = new EventStore('my-store', {
    storageDirectory: './data',
    typeAccessor: 'type'   // dot-notation path to the type field in the event payload
});
```

`typeAccessor` accepts a dot-notation path string (e.g. `'type'`, `'meta.kind'`) pointing to the event type field, which also enables faster index routing. For non-standard event layouts a function `(event) => string` can be used instead.

Type stream names currently map directly to event types (for example `CourseCreated`).

`query()` treats missing referenced streams as empty sets. This applies both to type-stream lists and nested selector algebra.

In selector terms:

- missing stream in an `OR` group: no effect,
- missing stream in an `AND` group: that branch becomes empty.

> **New stores only**: type indexes are built with `reindex=false` — they only cover events committed *after* the index was first created. Always configure `typeAccessor` from the beginning if you intend to use `query()`.

> **CommitCondition timing**: the condition captures the global store length at the moment `query()` is called. At commit time, the conflict check includes all streams referenced by the selector that exist *at that point* — including streams created between the `query()` and `commit()` calls. This means a type stream or tag stream created by another writer during that window is automatically included in the conflict check, with no race gap.

Tag streams are optional. DCB queries can be implemented without any tag streams by using matcher logic only.

---

## Full Example

```javascript
import { EventStore } from 'event-storage';

const store = new EventStore('accounts', {
    storageDirectory: './data',
    typeAccessor: 'type'
});

store.on('ready', () => {

    function handleRegisterCustomer(command) {
        const { stream, condition } = store.query(
            ['CustomerRegistered'],
            (event) => event.email === command.email
        );

        let emailTaken = false;
        stream.forEach((event) => {
            if (event.email === command.email) emailTaken = true;
        });

        if (emailTaken) {
            throw new Error(`Email ${command.email} is already registered`);
        }

        store.commit(`customer-${command.customerId}`, [
            { type: 'CustomerRegistered', customerId: command.customerId, email: command.email }
        ], condition);
    }

    handleRegisterCustomer({ customerId: 'cust-1', email: 'alice@example.com' });
});
```

---

## The DCB Specification: Types and Tags

The formal DCB specification expresses a query as a list of **query items**, each pairing an array of event types with an array of **domain-identifier tags**:

```
queryItems = [
  { types: ['CourseCreated', 'CourseCapacityChanged', 'StudentSubscribedToCourse'],
    tags:  ['course:jdsj4'] },
  { types: ['StudentCreated', 'StudentSubscribedToCourse'],
    tags:  ['student:gfh3j'] }
]
```

An event matches when **any** item matches it: the event's type must be in that item's `types` **and** the event must carry **all** of that item's tags.

Equivalent selector intent (when tags are represented as streams):

- query level (`items`): `OR`
- per-item `tags`: `AND`
- per-item `types`: `OR`

When tags are materialized as dedicated streams, pass the nested selector directly to `query()` — it returns both the filtered stream and the concurrency condition in one call, using the same alternating OR/AND depth semantics described in [Joining Streams](streams.md#joining-streams):

```javascript
const courseId  = 'course:jdsj4';
const studentId = 'student:gfh3j';

const { stream, condition } = store.query([
    [courseId, ['CourseCreated', 'CourseCapacityChanged', 'StudentSubscribedToCourse']],
    [studentId, ['StudentCreated', 'StudentSubscribedToCourse']]
]);
```

- depth 0 (top-level array): OR across query items
- depth 1 (second-level arrays): AND — tag stream intersected with the type-group stream
- depth 2 (third-level arrays): OR across event types

Selection happens at the index level: only events at the intersection of the tag stream and the relevant type streams are yielded, without deserializing unrelated documents. Missing streams are treated as empty — an `OR` branch with no matching stream is skipped; an `AND` branch containing a missing stream yields nothing.

```javascript
store.commit('enrollment-...', [{ type: 'StudentSubscribedToCourse', ... }], condition);
```

Without tag streams, the same intent can be expressed using the `matcher` function. Pass the union of all types, and encode per-item tag logic in the matcher:

```javascript
const courseId  = 'course:jdsj4';
const studentId = 'student:gfh3j';

const { stream, condition } = store.query(
    ['CourseCreated', 'CourseCapacityChanged', 'StudentCreated', 'StudentSubscribedToCourse'],
    (event, meta) =>
        (['CourseCreated', 'CourseCapacityChanged', 'StudentSubscribedToCourse'].includes(event.type)
            && meta.tags?.includes(courseId))
        ||
        (['StudentCreated', 'StudentSubscribedToCourse'].includes(event.type)
            && meta.tags?.includes(studentId))
);
```

## Choosing Between Matcher-Only and Tag Streams

Both approaches are valid and can coexist.

- **Matcher-only**: no additional tag indexes to maintain; lower write amplification.
- **Tag streams**: more index writes per commit (one extra index write per configured tag stream), but can reduce read-time misses and deserialization/evaluation overhead when tag cardinality is high.

Rule of thumb:

- higher write load / lower read selectivity pressure -> matcher-only is often better,
- lower write load / high tag diversity with many matcher misses -> tag streams are often better.

### Benchmark findings and practical recommendation

This benchmark section is inspired by the excellent pyeventsourcing speedrun style: a compact scenario that makes architectural trade-offs visible under sustained write-then-read pressure.

Benchmark setup in this repository:

- scenario: course subscriptions with DCB read model rebuilding,
- runtime: 3 seconds per mode,
- per iteration: 10 student registrations, 10 course registrations, 100 enrollments (120 operations total),
- each mode runs against a fresh data directory.

Recent measurements with that setup produced the following excerpt:

| Mode                    | Ops/s | us/op | Relative vs matcher-only |
|-------------------------|------:|------:|-------------------------:|
| Matcher-only            |   372 |  2690 |                    1.00x |
| Tag-streams             |  1241 |   806 |                    3.34x |
| Semester-bounded stream |  3606 |   277 |                    9.69x |

Mode definitions and how to apply them in practice:

- **Matcher-only**: query by event types and evaluate tags only via matcher logic at read time. Practical setup: keep type streams, avoid tag streams, call `query(types, matcher)`.
- **Tag-streams**: materialize tags as dedicated streams and build DCB context from stream selectors. Practical setup: create tag streams (manually or in pre-commit) and query through `query()` with nested selector algebra.
- **Semester-bounded stream**: write all decision-relevant events for a semester into `semesters/{id}` so the read context stays naturally bounded. Practical setup: choose business-bounded write streams first, then query only within that boundary.

This demonstrates three important points:

- pure matcher-only DCB can degrade when the scanned context grows without a hard domain boundary,
- DCB according to the full types+tags specification is usually fast enough with tag streams, but this can create many streams and extra index writes,
- the best long-term approach is often domain partitioning (bounded write streams), so reads are naturally selective even without additional tag/type streams.

Compared to generic SQL-backed event-store speedruns (including the pyeventsourcing benchmark context), this indicates that event-storage can outperform when stream boundaries reflect real domain limits, not only technical entity ownership.

For the semester case specifically, realistic systems typically need one more filter/intersection for enrollment-relevant types because a semester stream may contain many unrelated events. Two common options are:

- keep one semester stream and intersect with enrollment-related event types during context read,
- split into `semester/{id}/students`, `semester/{id}/courses`, and `semester/{id}/enrolments`, then build the DCB context from the join of those three streams.
