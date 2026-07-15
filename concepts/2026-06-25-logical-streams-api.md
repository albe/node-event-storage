# Logical Stream Selector API

Provide logical `AND`/`OR` composition over streams without introducing a separate query language.

## Core Direction

Use **one** stream composition API:

- `fromStreams(selector, options)` is the canonical entry point.
- `AND` use cases are represented directly in selector algebra.
- `CombinedEventStream` is removed; `JoinEventStream` is the single implementation behind `fromStreams`.

## Selector Algebra

Nested arrays encode alternating operators by depth:

- depth 0: `OR`
- depth 1: `AND`
- depth 2: `OR`
- depth 3: `AND`
- ...

Examples:

```javascript
fromStreams(['a', 'b']);
// a OR b

fromStreams([['a', 'b']]);
// a AND b

fromStreams([
  ['tag1', 'tag2', ['typeA', 'typeB']],
  ['tag3', 'tag4']
]);
// (tag1 AND tag2 AND (typeA OR typeB)) OR (tag3 AND tag4)
```

## Convenience Helpers

`anyOf` / `allOf` / `all` helpers improve readability and normalize shape artifacts.

Canonical style:

```javascript
anyOf(allOf('a', 'b'), 'c');
```

Normalization examples:

- `anyOf(anyOf('a', 'b'), 'c')` -> `anyOf('a', 'b', 'c')`
- `allOf(allOf('a', 'b'), 'c')` -> `allOf('a', 'b', 'c')`
- `OR(..., all(), ...)` -> `all()`
- `AND(..., all(), ...)` -> remove `all()` from that `AND` level

## Implementation via Index Union/Intersect

Selector evaluation maps directly to index set operations.

- `OR` level -> `union(...)`
- `AND` level -> `intersect(...)`

Example:

```javascript
[
  ['a', 'b', ['c', 'd']],
  ['e', 'f']
]
// (a AND b AND (c OR d)) OR (e AND f)
```

Execution:

1. Evaluate depth 2 (`OR`) subtree:
   - `x = union(index(c), index(d))`
2. Evaluate each depth 1 (`AND`) group:
   - `g1 = intersect(index(a), index(b), x)`
   - `g2 = intersect(index(e), index(f))`
3. Evaluate depth 0 (`OR`) root:
   - `result = union(g1, g2)`

General recursion:

```text
evaluate(node, depth):
  if node is streamName:
    return index(streamName)

  children = node.map(child => evaluate(child, depth + 1))

  if depth is even:  // OR level
    return union(children)

  // depth is odd: AND level
  return intersect(children)
```

## Execution Notes

- `fromStreams(['a', 'b'])` maps to existing OR/union paths naturally.
- For commit conflict checks, existence checks can short-circuit; full ordering is not required.

## Join Replacement Status

The previous `JoinEventStream` k-way-merge logic is fully replaced by selector-algebra evaluation over indexes (`union`/`intersect`) in the canonical `JoinEventStream` implementation.

Benchmark excerpt (`bench/bench-join-vs-combined-or.js`, 2026-06-27, selected cases):

```text
Scenario: balanced-with-two-noise-streams, forward-full
JoinEventStream [a,b] 34 ops/s | Combined OR [a,b] 36 ops/s | relative Join/Combined: 0.94x

Scenario: b-sparse-with-noise, forward-full
JoinEventStream [a,b] 54 ops/s | Combined OR [a,b] 55 ops/s | relative Join/Combined: 0.98x

Scenario: bursty-b, reverse-max-capped-75pct
JoinEventStream [a,b] 75 ops/s | Combined OR [a,b] 73 ops/s | relative Join/Combined: 1.03x
```

Interpretation: results are in near-parity with small scenario-dependent variation. Together with the cleaner single-path architecture, this supports keeping the algebra-based implementation as the only stream-composition path.

## Migration Note

- Existing "all streams must match" use cases are represented with selector algebra (for example `allOf(...)` or equivalent normalized array form).
