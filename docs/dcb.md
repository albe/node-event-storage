# Dynamic Consistency Boundaries (DCB)

## What is DCB?

**Dynamic Consistency Boundary** (DCB) is a pattern for event-sourced systems where the unit of consistency — the "transaction boundary" — is defined at command-handling time rather than fixed to a single aggregate stream.

In traditional event-sourcing each command targets one aggregate (e.g. `order-42`) and optimistic concurrency is enforced by checking that the aggregate's stream is at the expected version.

DCB generalises this:

- The consistency boundary is expressed as a **query**: _a set of event types_ (and an optional additional filter) that the command handler cares about.
- A **consistency token** is obtained before reading, capturing the current length of each relevant type stream.
- After building the transaction context from those events the handler commits its new events together with the token.
- The commit engine checks: did any new events appear that satisfy the original query? If yes, a conflict is raised; if no, the commit succeeds.

This removes the need to route every command through a fixed aggregate, enabling commands that span multiple entities while retaining strong consistency guarantees without distributed locks.

---

## Multi-Value Object Matchers

Before introducing full DCB mode it is worth knowing that **object matchers** support arrays for any property value.  An array is treated as an OR condition: a document matches when its property equals **any** value in the list.

```javascript
// Create a persistent stream that indexes two event types in one shot.
eventstore.createEventStream('order-events', {
    payload: { type: ['OrderPlaced', 'OrderShipped', 'OrderCancelled'] }
});
```

Crucially, array-valued properties still benefit from the **discriminant optimisation** in the write path: each new event is routed to the right index in O(1) without a full-store scan.  This makes multi-type persistent streams nearly free to maintain.

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

1. **Events are physically partitioned by `payload.type`** instead of by stream name.  Every distinct event type is stored in its own file, making type-scoped reads highly sequential.

2. **A lightweight type-stream index is automatically created** for each new event type the first time it appears.  This index is populated with `reindex=false`, meaning it tracks only events written while DCB mode is active (see the [caveats](#important-caveats) section).

3. **`getConsistencyToken` requires no store scan** because the type indexes are always up to date.

4. **Commits to arbitrary entity-based stream names still work** — the type stream and the entity stream are maintained concurrently as independent secondary indexes.

---

## The DCB Workflow

### Step 1 — Obtain a Consistency Token

```javascript
const { token, stream } = store.getConsistencyToken(
    ['OrderPlaced', 'OrderShipped'],  // event types to watch
    (e) => e.payload.orderId === 'order-42'  // optional additional filter
);
```

- `token` — a `ConsistencyToken` snapshot.  Pass it to `commit()` to enforce the concurrency check.
- `stream` — an `EventStream` over all events of the given types, ordered by global sequence number.  Iterate it to build the transaction context.

The optional `matcher` is an additional filter on the stored event document (`{ stream, payload, metadata }`).  It qualifies **which new events count as a conflict**: a new event of a listed type only causes a conflict when this matcher also returns `true` for it.

### Step 2 — Build the Transaction Context

```javascript
const model = new OrderModel();

stream.forEach((event) => {
    // The optional matcher lets you filter here too
    if (token.matcher && !token.matcher(event)) return;
    model.apply(event.payload);
});
```

### Step 3 — Commit with the Token

```javascript
try {
    store.commit('order-42', newEvents, token, () => {
        console.log('Committed successfully');
    });
} catch (e) {
    if (e instanceof EventStore.OptimisticConcurrencyError) {
        // A conflicting event appeared — replay and retry
    }
}
```

The stream name passed to `commit()` can be:

- **An event type** (`'OrderPlaced'`): works naturally in DCB mode since each type is already its own stream.
- **An entity-based name** (`'order-42'`): still works — the entity stream index is maintained alongside the type indexes.

---

## Conflict Semantics

The conflict check at commit time evaluates each event type listed in the token:

| Scenario | Result |
|----------|--------|
| No new events of any listed type appeared | ✅ No conflict |
| New events appeared, but none match the token `matcher` | ✅ No conflict |
| New events appeared and at least one matches the token `matcher` | ❌ `OptimisticConcurrencyError` |
| New events appeared and the token has no `matcher` | ❌ `OptimisticConcurrencyError` |

This means that unrelated concurrent writes (same event type but different business entity) never cause spurious conflicts — the optional `matcher` lets you narrow the boundary to exactly the events that would actually affect the decision.

---

## Full Example

```javascript
import { EventStore } from 'event-storage';

const store = new EventStore('orders', {
    storageDirectory: './data',
    dcbMode: true
});

store.on('ready', async () => {

    async function handleRegisterCustomer(command) {
        // Watch for any CustomerRegistered event with this email
        const { token, stream } = store.getConsistencyToken(
            ['CustomerRegistered'],
            (e) => e.payload.email === command.email
        );

        // Build state: is this email already taken?
        let emailTaken = false;
        stream.forEach((e) => {
            if (e.payload.email === command.email) emailTaken = true;
        });

        if (emailTaken) {
            throw new Error(`Email ${command.email} is already registered`);
        }

        // Commit — the token ensures no concurrent registration with the same email slips through
        store.commit('customer-' + command.customerId, [
            { type: 'CustomerRegistered', customerId: command.customerId, email: command.email }
        ], token);
    }

    await handleRegisterCustomer({ customerId: 'cust-1', email: 'alice@example.com' });
});
```

---

## Standard Mode vs DCB Mode

`getConsistencyToken` works in both standard and DCB mode.

| Feature | Standard mode | DCB mode |
|---------|---------------|----------|
| Physical partitioning | By stream name | By `payload.type` |
| Type stream creation | `reindex=true` on first use (one-time scan) | `reindex=false` (instant) |
| `getConsistencyToken` first call | May scan existing events | O(1) always |
| `getConsistencyToken` subsequent calls | O(1) | O(1) |
| Entity-based stream names | Default | Also supported |
| Type-based stream names | Manual | Automatic |

In **standard mode** the first call to `getConsistencyToken` for a given event type creates a secondary index by scanning all existing events (`reindex=true`).  This is a one-time cost proportional to the store size; all subsequent calls are O(1).  If your store is large and you want to avoid the initial scan, switch to DCB mode.

---

## Important Caveats

### DCB mode is for new stores

Type indexes in DCB mode are created with `reindex=false`.  This means they only track events written *after* the index was first created.  If you open a pre-existing, stream-partitioned store in DCB mode, the type indexes will be incomplete and the consistency token will not reflect the full history.

**Always create a store with `dcbMode: true` from the beginning** if you intend to use DCB mode.

### Single-writer constraint

Like the rest of event-storage, DCB mode enforces a single writer per store (via a lock file).  The consistency token check is not distributed — use this for single-server applications or a single coordinating write process.
