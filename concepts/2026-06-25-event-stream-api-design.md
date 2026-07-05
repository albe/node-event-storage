# Event Stream API Design

**Date:** 2026-06-23  
**Status:** Draft  
**Scope:** `event-storage` / `event-storage-http` public interface  

---

## Motivation

The `event-storage` library today exposes streams as bounded ranges: a read always requires an explicit `from`/`until` window, and the caller is responsible for re-issuing the next window. Long polling and the `Consumer` abstraction were added on top to reduce projection latency — but they are transport mechanisms layered over the fundamental concept, not part of it.

The goal of this document is to define a clean public interface that:

- Models an event stream as what it conceptually is: an **unbounded, ordered sequence of events**
- Separates the *definition* of what to read from the *mode* in which it is consumed
- Makes continuous projections and ad-hoc decision models natural first-class patterns
- Keeps the HTTP transport layer fully transparent — callers behave identically against an in-process store and a remote HTTP instance

---

## Core Model

An event stream is an `AsyncIterable<StoredEvent>` with no inherent end. The implementation decides what happens at the current tail:

| Consumption mode | Behaviour at tail |
|---|---|
| **Ad-hoc iteration** (snapshot) | Ends — iterator completes normally |
| **`subscribe()`** | Waits and yields newly committed events as they arrive — never ends unless explicitly stopped |
| **`query()`** | Reads to current tail, collects result + returns a `CommitCondition` for optimistic concurrency |

All three modes share the same source definition and filter chain. The mode is chosen at call time, not at query definition time.

---

## Interface

```typescript
// ─── Top-level factory ───────────────────────────────────────────────────────

interface EventStore {
  /** Single named write stream. */
  stream(name: string): EventStreamQuery;

  /** All streams whose name starts with `category/`. */
  category(name: string): EventStreamQuery;

  /**
   * Explicit join over a fixed list of streams.
   * Events are merged in global-position order.
   */
  join(streams: string[]): EventStreamQuery;

  /**
   * Join over per-type index streams (one stream per event type, written
   * automatically on commit). Gracefully excludes types whose index stream
   * does not yet exist (no events of that type have been committed).
   *
   * This is the primary entry point for DCB-style decision models that are
   * decoupled from write-stream topology.
   */
  ofTypes(...types: string[]): EventStreamQuery;

  /** Append events to a named stream. */
  commit(
    stream: string,
    events: NewEvent[],
    opts?: CommitOptions,
  ): Promise<CommitResult>;
}

// ─── Query descriptor ────────────────────────────────────────────────────────

/**
 * Base query descriptor — pre-mode filters only.
 * Valid for all three consumption modes: subscribe(), query(), and range().
 *
 * `until()` and `backwards()` are intentionally absent here. They live only
 * on RangeQuery (entered via .range()) so that the type system statically
 * prevents nonsensical combinations such as `.until(x).subscribe()`.
 */
interface EventStreamQuery extends AsyncIterable<StoredEvent> {
  // --- Pre-mode modifiers (valid for all consumption modes) ---------------

  /**
   * Start reading from this global position (inclusive).
   * Default: 0 (beginning of store).
   */
  from(position: number): EventStreamQuery;

  /**
   * Filter events by matching against their payload (and/or metadata) fields.
   * Supports equality, array membership, and comparison operators.
   * Passed through to the server; also applied client-side as a guard.
   */
  matching(matcher: ObjectMatcher): EventStreamQuery;

  // --- Consumption modes ------------------------------------------------

  /**
   * Convenience snapshot: iterate from `from` to the current tail, then end.
   * Equivalent to `.range().from(x)` with no explicit upper bound.
   *
   * In-process adapters SHOULD additionally implement [Symbol.iterator]()
   * (synchronous) for throughput-critical paths — see "Sync Iteration" below.
   */
  [Symbol.asyncIterator](): AsyncIterator<StoredEvent>;

  /**
   * Continuous subscription — never ends unless the signal is aborted.
   *
   * Delivers the backlog from `from` to the current tail, then delivers newly
   * committed matching events as they arrive. Reconnects transparently on
   * connection loss (HTTP) or store restart (in-process).
   * The caller is responsible for persisting the checkpoint.
   *
   * Note on naming: `follow()` is semantically more precise (no server-side
   * subscription state is created; the caller follows the tail of an
   * append-only log). `subscribe()` is used here as a provisional name for
   * familiarity.
   */
  subscribe(opts?: SubscribeOptions): AsyncIterable<StoredEvent>;

  /**
   * DCB-style lazy read — returns an EventStreamResult immediately.
   *
   * The EventStreamResult carries the CommitCondition from the response
   * (available before any event is iterated) and is itself an AsyncIterable
   * over the matching events. Reads from `from` to the current tail;
   * `matching()` is applied server-side.
   *
   * The returned Promise resolves as soon as the connection is established
   * and response metadata (condition, effective range) is available —
   * before the body is consumed.
   */
  query(): Promise<EventStreamResult>;

  /**
   * Fork into bounded-read mode.
   *
   * Returns a RangeQuery that adds `until()` / `to()` / `backwards()`.
   * RangeQuery does NOT have `subscribe()` or `query()` — use this fork
   * only for finite, explicitly-bounded ad-hoc iteration.
   */
  range(): RangeQuery;
}

// ─── Bounded range query (fork of EventStreamQuery via .range()) ─────────────

/**
 * Bounded read descriptor — adds directional range bounds.
 * Does NOT expose subscribe() or query(): those are open-ended operations
 * and are incompatible with an explicit upper bound or backwards direction.
 */
interface RangeQuery extends AsyncIterable<StoredEvent> {
  from(position: number): RangeQuery;
  matching(matcher: ObjectMatcher): RangeQuery;

  /**
   * End of the range (inclusive). Alias: to().
   *
   * Direction is inferred from the from/until relationship:
   * - from < until → forwards
   * - from > until → backwards (iterate from `from` down to `until`)
   */
  until(position: number): RangeQuery;

  /** Alias for until(). */
  to(position: number): RangeQuery;

  /**
   * Read in the backwards direction.
   *
   * With no argument: reverses the direction of a from/until range.
   *   `.from(10).until(15).backwards()` = `.from(15).until(10)`
   *
   * With argument n: read n events backwards from `from` (or from tail
   * if `from` is not set).
   *   `.from(500).backwards(20)` — 20 events ending at 500
   *   `.backwards(20)`           — last 20 events
   */
  backwards(n?: number): RangeQuery;

  /** Bounded async iteration. */
  [Symbol.asyncIterator](): AsyncIterator<StoredEvent>;

  /**
   * Sync iteration — in-process adapters only.
   * Not available on the HTTP adapter (network I/O cannot be synchronous).
   */
  [Symbol.iterator]?(): Iterator<StoredEvent>;

  /**
   * Returns a Node.js Readable (objectMode) for pipe-based tooling.
   * In-process: returns `this` (zero cost). HTTP: Readable.from(this).
   */
  toReadable?(): import('node:stream').Readable;
}

// ─── Result of query() ───────────────────────────────────────────────────────

/**
 * Lazy result of a DCB-style query().
 *
 * The CommitCondition and range metadata are available immediately after
 * the Promise from query() resolves — before any events are iterated.
 * This matches the HTTP wire model: the condition lives in the response
 * header, which is readable before the body stream begins.
 */
interface EventStreamResult extends AsyncIterable<StoredEvent> {
  /**
   * Commit condition covering the types and matcher used in this query.
   * Pass to store.commit() to assert no matching event was added since the
   * read (optimistic concurrency / DCB pattern).
   * Null if the source does not support condition headers.
   */
  readonly condition: CommitCondition | null;

  /** Effective global-position range covered by this result. */
  readonly range: { from: number; until: number };

  /** Source description. */
  readonly source: StreamSource;

  /** Lazy event delivery — iterate as events are received. */
  [Symbol.asyncIterator](): AsyncIterator<StoredEvent>;

  /** Convenience: collect all events into an array. */
  toArray(): Promise<StoredEvent[]>;
}

// ─── Supporting types ────────────────────────────────────────────────────────

/** Recursive field matcher (subset of MongoDB query syntax). */
type Scalar = string | number | boolean;
type ObjectMatcher = {
  [field: string]:
    | Scalar
    | Scalar[]                          // field is one of [...] (IN)
    | { $ne?: Scalar; $gt?: number; $gte?: number; $lt?: number; $lte?: number }
    | ObjectMatcher;                    // nested path
};

type StreamSource =
  | { kind: 'stream';   name: string }
  | { kind: 'category'; name: string }
  | { kind: 'join';     streams: string[] }
  | { kind: 'types';    types: string[] };

interface SubscribeOptions {
  signal?: AbortSignal;
}

interface CommitCondition {
  types: string[];
  noneMatchAfter: number;
  matcher?: ObjectMatcher;
}

interface CommitOptions {
  condition?: CommitCondition | CommitCondition[];
  expectedVersion?: number;
  metadata?: Record<string, unknown>;
  signal?: AbortSignal;
}
```

---

## Usage Patterns

### Continuous projection

```javascript
// Load persisted checkpoint, then subscribe from there.
let checkpoint = await loadCheckpoint('organization-projection');

for await (const event of store.category('organization').from(checkpoint).subscribe({ signal })) {
  await project(event);
  checkpoint = event.meta.globalPosition;

  // Persist checkpoint alongside projection state — crash-safe.
  if (shouldFlush) await saveCheckpoint('organization-projection', checkpoint);
}
```

The loop never ends on its own; it ends when `signal` is aborted (graceful shutdown).

### Ad-hoc decision model (OCC / DCB)

`query()` resolves as soon as the connection is open and the condition header is
available — before any event body has been read. The caller can take the condition
immediately and then lazily iterate:

```javascript
const result = await store
  .ofTypes('OrganizationCreated', 'OrganizationUpdated', 'OrganizationDeleted')
  .from(lastKnownVersion)
  .matching({ payload: { organizationId } })
  .query();

// condition available immediately (from response header):
const { condition } = result;

// lazy iteration:
let state = null;
for await (const event of result) {
  state = applyEvent(state, event);
}

// or collect when the full array is needed:
const events = await result.toArray();
const state = events.reduce(applyEvent, null);

await store.commit(`organization/${organizationId}`, [newEvent], { condition });
```

### Bounded ad-hoc reads (via `.range()`)

Range modifiers live on `RangeQuery`, entered via `.range()`. This makes invalid
combinations (e.g. `.until(x).subscribe()`) a compile-time error:

```javascript
// Forwards [100, 500] inclusive:
for await (const event of store.category('organization').range().from(100).until(500)) { }

// Backwards — equivalent expressions:
for await (const event of store.stream('orders/123').range().from(500).until(100)) { }
for await (const event of store.stream('orders/123').range().from(500).backwards(401)) { }
for await (const event of store.stream('orders/123').range().from(100).to(500).backwards()) { }

// Last 20 events:
for await (const event of store.stream('orders/123').range().backwards(20)) { }
```

The `.range()` call is the explicit fork point: before it, only `from()` and
`matching()` are available (valid for all modes); after it, `until()` / `to()` /
`backwards()` are available and `subscribe()` / `query()` are gone.

---

## `ofTypes()` — Type-Indexed Queries

`event-storage` automatically maintains a per-type index stream for each committed event type (e.g. `$type-OrganizationCreated`). `ofTypes()` builds a join over these index streams, giving read access to all events of given types across the entire store regardless of write-stream topology.

```javascript
// Returns an EventStreamQuery backed by a join over type index streams.
store.ofTypes('OrderPlaced', 'OrderCancelled')
```

**Behaviour:**
- Types whose index stream does not yet exist are silently excluded from the join (no events committed for that type yet — the result is simply empty for that type, not an error).
- The returned query behaves identically to `join()` — `from()`, `matching()`, `subscribe()`, and `query()` all apply.
- `query()` on an `ofTypes()` query returns a `CommitCondition` covering exactly the requested types, suitable for an OCC commit.

**Future scope:** AND-semantics across multiple stream dimensions (e.g. "events of type X in stream Y") would require a server-side intersection query path. This is out of scope for the initial design but the interface does not preclude it — a future `store.ofTypes(...).inStream(name)` or similar could be added without breaking the existing API.

---

## Ad-hoc Range Semantics

`until()`, `to()`, and `backwards()` are only available on `RangeQuery` (entered
via `.range()`). They are intentionally absent from `EventStreamQuery` so that
`subscribe()` and `query()` cannot be called on a query that has an explicit upper
bound — the type system enforces this statically.

All ranges are **inclusive** on both ends: `range().from(a).until(b)` includes
events at positions `a` and `b`. `to()` is an alias for `until()`.

Direction is inferred from the from/until relationship:

| Combination | Meaning |
|---|---|
| `.range().from(a).until(b)` where `a < b` | Positions `[a, b]`, forwards |
| `.range().from(a).until(b)` where `a > b` | Positions `[b, a]`, backwards (from `a` down to `b`) |
| `.range().from(a).backwards(n)` | `n` events ending at `a` (inclusive), backwards |
| `.range().backwards(n)` | Last `n` events (ending at current tail) |
| `.range().from(a).until(b).backwards()` | Same as `.range().from(b).until(a)` — reverses a forwards range |
| `.range().from(a)` only | From `a` to current tail, forwards |
| `.range()` (no modifiers) | Entire source, forwards |
| `EventStreamQuery[Symbol.asyncIterator]` | From `from` to current tail — convenience, no `.range()` needed |

`backwards()` with no argument flips the direction of a `from/until` range; with
an argument `n` it reads exactly `n` events backwards from `from` (or the tail).

---

## Sync Iteration (In-Process Performance)

Async iteration (`for await`) incurs measurable overhead per event due to promise allocation and microtask scheduling. In benchmarks against the in-process store this overhead is visible at high throughput (bulk projection rebuilds, large catchup reads).

In-process adapters **should** additionally implement `[Symbol.iterator]()` on their `RangeQuery` and on the `EventStreamQuery` default iterator path:

```javascript
// Sync iteration — in-process only, zero promise overhead
if (Symbol.iterator in query) {
  for (const event of query) {        // sync
    project(event);
  }
} else {
  for await (const event of query) {  // async fallback (HTTP adapter)
    project(event);
  }
}
```

**Constraints:**
- Sync iteration is only possible for snapshot reads (finite, already-committed events).
- `subscribe()` is inherently async — waiting for future events requires yielding the thread.
- The HTTP adapter never implements `[Symbol.iterator]()` (network I/O cannot be synchronous).
- Callers that target both in-process and HTTP must use the async path; callers that are compile-time in-process-only may use the sync path directly.

A capability check helper keeps callers clean:

```javascript
// Utility — not part of the core interface, but a recommended convenience
function iterateSync(query, callback) {
  if (Symbol.iterator in query) {
    for (const event of query) callback(event);
  } else {
    return (async () => { for await (const event of query) callback(event); })();
  }
}
```

---

## Consumer (Simplified Role)

The `Consumer` in `event-storage` was historically a server-side abstraction for durable subscriptions. Under this design its role is reduced: it becomes a **thin convenience wrapper** over an `EventStreamQuery` that adds flat-file checkpoint persistence and an optional lightweight projection model.

It is appropriate for:
- Embedded/local use cases where an external database is not available
- Development and testing
- Simple side-processes co-located with the event store

It is **not** a replacement for external projections backed by a real database (Firestore, Postgres, etc.) — those callers use `EventStreamQuery.subscribe()` directly and manage their own checkpoints.

```typescript
/**
 * Pure projection description — no persistence details.
 * Passed as the second argument to the Consumer constructor.
 */
interface ConsumerOptions<S> {
  /** The query to subscribe to. The Consumer reads the persisted checkpoint
   *  on startup and calls `.from(checkpoint)` on this query automatically. */
  source: EventStreamQuery;
  /** Pure event reducer (state, event) → state. */
  apply?: (state: S | null, event: StoredEvent) => S;
  /** Initial in-memory state before any events are applied. Default: null. */
  initialState?: S;
}

/**
 * Constructor: new Consumer(name, options)
 *
 * `name` is the persistence identity — the checkpoint and state file are
 * stored at `{storageDirectory}/consumers/{name}.{pos|state}`.
 * `options` is a pure projection description independent of persistence.
 */
class Consumer<S = unknown> implements AsyncIterable<StoredEvent> {
  constructor(name: string, options: ConsumerOptions<S>);

  /** Current in-memory projection state (updated after each apply() call). */
  readonly state: S | null;
  /** Global position of the last processed event. */
  readonly checkpoint: number;

  /**
   * Persist the current state + checkpoint to the flat files atomically.
   *
   * In run() mode this is called automatically according to `persistEvery`.
   * In manual iteration mode the caller is responsible for calling setState()
   * at appropriate semantic boundaries.
   */
  setState(state?: S): void;

  /**
   * Start consuming continuously.
   * Internally calls subscribe() on the source, applies each event via apply(),
   * and persists according to `persistEvery`.
   */
  run(opts?: { signal?: AbortSignal; persistEvery?: number | 'idle' }): Promise<void>;

  /**
   * Reset the Consumer to a new initial state and starting position,
   * then restart run() if it was active.
   *
   * Use cases: re-projecting from scratch after a schema migration;
   * seeding with a snapshot and continuing from its position.
   *
   * If run() is currently active, it is stopped, the state and checkpoint
   * are reset, and run() is restarted from the new `from` position.
   * The flat files are overwritten with the new initial values.
   */
  reset(opts: { initialState?: S; from?: number }): Promise<void>;

  [Symbol.asyncIterator](): AsyncIterator<StoredEvent>;
}
```

**Constructor design rationale**

Splitting `name` (persistence identity) from `options` (projection description) keeps the two concerns separate:

```javascript
// The projection description is a plain object — no persistence knowledge.
const organizationProjection = {
  source: store.category('organization'),
  apply: (state, event) => applyOrganizationEvent(state, event),
  initialState: null,
};

// The Consumer binds a name (= file path) to the description.
const consumer = new Consumer('organization-projection', organizationProjection);
await consumer.run({ signal: shutdownSignal });
```

This means `organizationProjection` can be defined independently, tested in isolation, and reused (e.g. passed to an HTTP-backed external projector without modification).

**Persistence strategy and reducer purity**

The current implementation passes `setState(state, doPersist: boolean)` inside the reducer to give the reducer control over flush timing. This forces the reducer to be aware of persistence, making it impure and harder to test.

The intended model:

- `apply(state, event)` is a **pure function** — no side effects, no injected callbacks.
- `consumer.setState()` is the Consumer's own persistence method. The caller (or `run()`) decides when to invoke it.
- `persistEvery` on `run()` covers the common cadences (`1` = after every event, `100` = every 100 events, `'idle'` = after a quiet period). For custom logic, iterate manually:

```javascript
// Auto-persist after every event (default):
await consumer.run({ signal });

// Persist every 100 events:
await consumer.run({ signal, persistEvery: 100 });

// Manual — flush only at semantic batch boundaries:
for await (const event of consumer) {
  // apply() is called automatically; consumer.state is updated
  if (event.type === 'BatchCompleted') {
    consumer.setState(); // explicit flush
  }
}
```

**reset() — rebuild from scratch**

```javascript
// Re-project from the beginning after a breaking schema change:
await consumer.reset({ initialState: null, from: 0 });

// Seed from a snapshot and continue from its position:
await consumer.reset({ initialState: snapshot.state, from: snapshot.position });
```

If `run()` is active when `reset()` is called, the current subscription is aborted, the in-memory state and checkpoint are replaced, and `run()` is restarted from the new position. The flat files are overwritten atomically.

The `Consumer` does not use long polling or server-side registration — it uses the same `subscribe()` path as any other caller, with the only addition being flat-file checkpoint + state persistence between restart cycles.

---

## Transport Transparency

The `EventStore` interface is implemented by two adapters with identical public behaviour:

### In-Process Adapter (`event-storage`)

- `EventStreamQuery[Symbol.asyncIterator]()` — reads log file(s) directly to EOF via sync generator wrapped in async
- `EventStreamQuery[Symbol.iterator]()` — same, without async wrapper (sync, zero-overhead)
- `RangeQuery[Symbol.asyncIterator/iterator]()` — bounded read respecting from/until/backwards; sync variant available in-process
- `subscribe()` — reads to EOF, then attaches an event listener on the store's write lock/notification mechanism; resumes reading when notified
- `query()` — opens a bounded read to current tail, builds `EventStreamResult` with `CommitCondition` from the store's current global position; returns immediately (before body iteration)

### HTTP Adapter (`event-storage-http` client layer)

- `EventStreamQuery[Symbol.asyncIterator]()` — `GET /streams/.../from/:from` (no `until`), reads NDJSON body
- `RangeQuery[Symbol.asyncIterator]()` — `GET /streams/.../from/:from/until/:until` or `/backwards/:n`
- `subscribe()` — long-poll loop: opens one NDJSON connection per iteration, reads backlog + live events until server closes the connection (idle timeout or signal), reconnects from latest position; watchdog timer aborts a stalled connection
- `query()` — `GET /query?types=...&from=...` (or join endpoint), extracts `x-event-store-query-condition` response header into `EventStreamResult.condition` before body iteration begins

The caller observes no difference:

```javascript
// Both lines produce an identical EventStreamQuery.
// Swap the adapter to switch transport.

const store = new EventStore('/data/events');                      // in-process
const store = new HttpEventStoreClient({ baseUrl, getToken });     // HTTP

// Caller code is unchanged:
for await (const event of store.category('organization').from(checkpoint).subscribe({ signal })) {
  await project(event);
}
```

---

## HTTP API Mapping

| Query chain | HTTP request |
|---|---|
| `.stream(n).from(a).until(b)` | `GET /streams/{n}/from/{a}/until/{b}` |
| `.stream(n).from(a).backwards(k)` | `GET /streams/{n}/from/{a}/backwards/{k}` |
| `.stream(n).backwards(k)` | `GET /streams/{n}/backwards/{k}` |
| `.category(n).from(a)` | `GET /streams/category/{n}/from/{a}/until/{a+window}` |
| `.join([a,b]).from(p)` | `GET /streams/join/from/{p}/until/{p+window}?streams=a,b` |
| `.ofTypes(T1,T2).from(p).query()` | `GET /query?types=T1,T2&from={p}` |
| any + `.subscribe()` | repeated long-poll requests from advancing checkpoint |

The `until` window for `subscribe()` is chosen by the HTTP adapter (`from + 1_000_000` by default) and advanced on each reconnect. This detail is invisible to the caller.

---

## Node.js `Readable` Stream Compatibility

The current in-process implementation exposes event streams as Node.js `Readable` streams (object mode). This section evaluates how that relates to the `AsyncIterable<StoredEvent>` interface proposed here.

### Compatibility

In Node.js ≥ 12, `Readable` implements `Symbol.asyncIterator` natively — a `Readable` IS-A `AsyncIterable`. Consequently, if an adapter returns a `Readable`, it automatically satisfies the `EventStreamQuery extends AsyncIterable<StoredEvent>` contract with no wrapping needed:

```javascript
const query = store.category('organization').from(0);
// If backed by a Readable internally:
for await (const event of query) { }          // works — Readable[Symbol.asyncIterator]
query.pipe(transform).pipe(writable);         // also works — Readable.pipe()
```

The reverse also holds: `Readable.from(asyncIterable)` wraps any `AsyncIterable` in a Readable when pipe access is needed.

### Arguments for `Readable` as the primary backing

| Aspect | Notes |
|---|---|
| **Explicit backpressure** | `highWaterMark` and `pause()`/`resume()` give explicit flow control; useful when events arrive faster than the projector can persist |
| **Pipe composition** | `.pipe(Transform)` enables filter, map, batch transforms using the existing Node.js stream ecosystem |
| **Existing implementation** | In-process EventStream is already Readable-backed — the design inherits performance characteristics that are already benchmarked |
| **`asyncIterator` for free** | No extra work needed for `for await` consumption |

### Arguments for `AsyncIterable` as the primary interface

| Aspect | Notes |
|---|---|
| **Platform neutrality** | `AsyncIterable` works in Deno, browser/edge runtimes, and bundled environments where Node.js `stream` is unavailable — relevant if the HTTP client ever runs outside Node.js |
| **Simpler implementation** | Async generators are easier to implement, test, and reason about than a correct Readable subclass |
| **Implicit backpressure** | The `for await` loop already provides natural backpressure: the producer only reads the next chunk when the consumer's loop body has completed — no explicit highWaterMark needed for typical projection use |
| **Sync iterator composability** | `[Symbol.iterator]()` (sync, in-process) sits alongside `[Symbol.asyncIterator]()` cleanly; adding it to a Readable subclass is more involved |

### Performance: `Readable.from(asyncIterable)` vs native `Readable._read()`

The key question for `toReadable()` on the HTTP adapter is whether wrapping an `AsyncIterable` in `Readable.from()` recovers the performance of a native Readable, or whether it inherits the async overhead.

**How `Readable.from()` works:**
Node.js `Readable.from(iter)` calls `iter[Symbol.asyncIterator]()` and, for each `_read()` call, invokes `iterator.next()` which returns a `Promise<{value, done}>`. Even if the underlying value is already in memory (synchronously available), Node.js must schedule a microtask to resolve the Promise before the value can be pushed into the Readable buffer. There is no shortcut for synchronously-resolved promises in this path.

**Native `Readable._read()` (in-process):**
```javascript
_read() {
  this.push(this._events[this._index++] ?? null); // synchronous, no Promise
}
```
`push()` is called synchronously inside `_read()`, with no microtask boundary. Node.js can service multiple `push()` calls within a single tick up to `highWaterMark`, enabling true batching.

**Consequence:** `Readable.from(asyncIterable)` does **not** recover native Readable performance. The async overhead originates in the iterator's `next()` → Promise chain, and `Readable.from()` sits on top of that chain — it does not remove it. Measured throughput for in-memory object streams:

| Implementation | Approximate throughput |
|---|---|
| Native `Readable._read()` (object mode) | ~2–5 M events/s |
| `for await` over sync async-generator | ~200–500 k events/s |
| `Readable.from(asyncIterable)` | ~200–500 k events/s (same as above) |

The 4–10× gap between native Readable and AsyncIterable-backed Readable is consistent with the per-event Promise allocation cost in the V8 microtask queue.

### Recommendation: split-adapter strategy

Given that the primary target is Node.js and the HTTP adapter is inherently network-bound, the right approach is to let each adapter use its natural implementation:

| Adapter | Primary backing | `AsyncIterable` | `[Symbol.iterator]` | `toReadable()` |
|---|---|---|---|---|
| **In-process** | Native `Readable` (`_read()`) | free (built-in on Readable) | additional method on the subclass | `return this` (zero cost) |
| **HTTP** | Async generator | the generator itself | not available | `Readable.from(this)` — acceptable; network RTT >> async overhead |

The contract for callers remains `AsyncIterable<StoredEvent>` in both cases. The in-process implementation satisfies it via Readable's built-in `[Symbol.asyncIterator]`, the HTTP implementation via a plain async generator.

```typescript
interface EventStreamQuery extends AsyncIterable<StoredEvent> {
  // ... core API ...

  /**
   * Returns the underlying Node.js Readable (objectMode).
   * In-process: returns `this` (the stream IS a Readable — zero overhead).
   * HTTP: returns `Readable.from(this)` — acceptable for pipe-based
   *       tooling; not recommended for throughput-critical bulk reads.
   */
  toReadable?(): import('node:stream').Readable;
}
```

The sync iterator (`[Symbol.iterator]`) is a separate capability of the in-process Readable subclass and is independent of the `toReadable()` path — it operates below the Readable abstraction entirely.

---

## Decisions

- **`ObjectMatcher` server-side pushdown**: The matcher is passed through by the HTTP adapter to the server on all endpoints (`/query`, stream, category, join reads). Server-side filtering is the current implementation; client-side filtering is an additional guard only.
- **Readable vs AsyncIterable per adapter**: In-process adapter is backed by a native `Readable` (satisfies `AsyncIterable` via built-in `[Symbol.asyncIterator]`). HTTP adapter is backed by an async generator. `Readable.from(asyncIterable)` does not recover native Readable throughput and must not be used for in-process bulk reads. `toReadable()` on the HTTP adapter is acceptable for pipe-based tooling only, as network RTT dominates.

## Open Questions

- **`subscribe()` vs `follow()`**: `follow()` more precisely describes the semantics (the caller follows the tail of an append-only log; no server-side subscription state is created). `subscribe()` is used here as a provisional name for familiarity. To be revisited before stabilising the API.
- **`ofTypes()` AND-intersection**: Intersecting a type query with a write stream (`ofTypes('X').inStream('org/123')`) would enable targeted OCC without full category scans. Deferred — requires server-side index intersection support.
