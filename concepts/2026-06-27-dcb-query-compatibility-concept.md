# DCB Query Compatibility Concept

This document builds on `concepts/2026-06-25-logical-streams-api.md` and defines DCB compatibility on top of the generic stream-selector model.

## Goal

Keep the EventStore core API stream-based while enabling DCB use cases through a dedicated translation layer.

- EventStore operates on stream selectors (`OR`/`AND` algebra over stream names).
- DCB query semantics (`types` + `tags`) are compiled to stream selectors.
- `query()` returns `{ stream, condition }` and stays the single entry point.
- `fromStreams` + `JoinEventStream` are the only stream-composition surface; no separate `CombinedEventStream` API remains.

## Non-goals

- No wire protocol format.
- No storage layout details.
- No requirement that DCB semantics become part of the generic stream API.

## Core Principle

The generic API does **not** know about `types`/`tags` as first-class concepts. It only evaluates selector algebra over stream names.

DCB support is an opt-in layer that:

1. validates/normalizes DCB query objects,
2. resolves `types`/`tags` to concrete stream names,
3. compiles them into selector algebra,
4. delegates execution to the generic selector path.

## Query Model (DCB Layer Input)

```ts
export type EventType = string;
export type EventTag = string;

export interface QueryItem {
  /** OR within one item */
  types?: readonly EventType[];
  /** AND within one item */
  tags?: readonly EventTag[];
}

export type QueryMatcher =
  | ((payload: any, metadata: any) => boolean)
  | object;

export interface Query {
  /** OR across items. Empty means match all. */
  items: readonly QueryItem[];
  matcher?: QueryMatcher | null;
}
```

## Unified API Surface

```ts
export interface QueryOptions {
  minRevision?: number; // default 1
  maxRevision?: number; // default -1
  raw?: boolean; // default false
  matcher?: QueryMatcher | null;
}

export interface DcbQueryResult {
  stream: EventStreamLike;
  condition: CommitCondition;
}

export interface EventStore {
  /** Generic selector execution API */
  fromStreams(selector: readonly unknown[], options?: QueryOptions): EventStreamLike;

  /** Unified query API: selector array OR DCB query object */
  query(selectorOrQuery: readonly unknown[] | Query, options?: QueryOptions): DcbQueryResult;
}
```

Notes:

- `queryByDefinition` / `queryItems` / `queryAll` are removed from the concept.
- Moving from multi-arg query signatures to `options` object is considered part of one breaking change.

## Selector Algebra

`fromStreams` evaluates nested selectors with alternating operators by depth:

- depth 0: `OR`
- depth 1: `AND`
- depth 2: `OR`
- ...

`all()` is a selector helper term with normalization:

- `OR(..., all(), ...)` -> `all()`
- `AND(..., all(), ...)` -> remove `all()` from that `AND` level

This allows `AND` queries without introducing an additional stream-composition method.

## Mapping DCB Query -> Selector

Per item:

- `types` become one `OR` term
- `tags` become `AND` terms

Example:

- `{ types: [A, B], tags: [T1, T2] }`
- compiles to `allOf(T1, T2, anyOf(A, B))`

Query-level:

- `items = [i1, i2, ...]` compiles to `anyOf(i1Selector, i2Selector, ...)`
- `items = []` compiles to `all()`

## anyOf/allOf Helper API

Helpers are syntax sugar over selector arrays and normalize accidental nesting.

```ts
type SelectorTerm = string | readonly SelectorTerm[];

export interface SelectorApi {
  anyOf(...terms: readonly SelectorTerm[]): SelectorTerm;
  allOf(...terms: readonly SelectorTerm[]): SelectorTerm;
  all(): SelectorTerm;
}
```

Canonical readable style:

- `anyOf(allOf(a, b), c)`

Normalization examples:

- `anyOf(anyOf(a, b), c)` -> `anyOf(a, b, c)`
- `allOf(allOf(a, b), c)` -> `allOf(a, b, c)`
- redundant single-child wrappers collapse

## CommitCondition Integration

`selector` remains the property name for now (rename can happen in a later breaking release).

```ts
export interface CommitCondition {
  readonly noneMatchAfter: number;
  readonly raw?: boolean;
  readonly selector?: {
    /** Already normalized selector in execution order. */
    readonly streams: readonly unknown[];
    readonly matcher?: QueryMatcher | null;
    readonly optimization?: {
      readonly plannedAtStoreLength?: number;
      readonly andOrderByEstimatedCost?: readonly number[];
    };
  };
}
```

Minimal payload guidance:

- Transport only what commit check needs: `noneMatchAfter`, `raw`, `selector.streams`, `matcher`, optional light `optimization`.
- Do not duplicate selector/matcher in nested plan objects.
- The caller already has the original `Query` object; it does not need to be embedded in condition metadata.

## Compiled Plan Shape (Internal/Optional)

A plan can exist internally, but is not required in serialized condition payload.

```ts
export interface CompiledItemPlan {
  readonly itemSelector: readonly unknown[];
}

export interface CompiledQueryPlan {
  readonly isAll: boolean;
  readonly selector: readonly unknown[];
  readonly optimization?: {
    readonly plannedAtStoreLength: number;
    readonly andOrderByEstimatedCost?: readonly number[];
  };
}
```

`itemOrderByEstimatedCost` is not part of the stable public transport contract.

## DCB Index Opt-In

DCB querying requires dedicated index streams and should be explicitly enabled.

- `typeAccessor`: enables type stream indexing (existing path).
- `tagsAccessor`: enables tag stream indexing.
- Supported configurations: either accessor alone, or both together.

Practical expectation:

- `tagsAccessor` alone is possible but usually weak for real DCB decision models.
- Most practical DCB usage is type-scoped with tags as additional narrowing.

## Execution Semantics

Given `condition.noneMatchAfter`:

- conflict check runs on `(noneMatchAfter + 1 .. latest)` only,
- conflict exists if any new event matches selector + matcher,
- for pure type-stream `OR` checks, conflict detection is existence-based with early exit (no global merge/sort required).

## Example

```js
const input = {
  items: [
    { types: ['EventType1', 'EventType2'], tags: ['tag1', 'tag2'] },
    { tags: ['tag3', 'tag4'] },
    { types: ['EventType3'] }
  ],
  matcher: { payload: { tenantId: 't-42' } }
};

const { stream, condition } = eventstore.query(input, { minRevision: 1 });
```

Equivalent selector intent:

```js
anyOf(
  allOf('tag1', 'tag2', anyOf('EventType1', 'EventType2')),
  allOf('tag3', 'tag4'),
  allOf(anyOf('EventType3'))
);
```

## Rollout Plan

1. Keep one selector algebra in `fromStreams`.
2. Move `query()` to unified signature: `query(selector|Query, options)`.
3. Add DCB compiler layer (`types`/`tags` -> selector) with accessor opt-in.
4. Add `anyOf`/`allOf`/`all()` normalization helpers.
5. Keep `CommitCondition` payload minimal and de-duplicated.
6. Add conformance tests:
   - OR over items
   - OR over item types
   - AND over item tags
   - helper normalization
   - `items = []` equals `all()`
   - commit conflict check equivalence vs query semantics

## Open Decisions

1. Exact tag stream naming (`tag:<value>` vs namespaced variants).
2. Strict vs forgiving normalization for malformed nested selector arrays.
3. Whether function matchers are serializable or only object matchers.
4. Whether to keep a temporary compatibility shim for old query signature during migration.
