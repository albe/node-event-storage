# DCB Query Compatibility Concept

This document defines a DCB compatibility layer on top of the generic stream-selector model.

## Goal

Enable DCB query semantics (`types` + `tags`) through a thin compilation layer that maps
`DcbQuery` objects to the existing selector algebra, without changing the core API,
`CommitCondition`, or `JoinEventStream`.

- `query()` continues to accept selector arrays (flat or nested) as before.
- When passed a `DcbQuery` object instead, the layer compiles it to a selector and delegates
  to the same execution path.
- `typeAccessor` and `tagsAccessor` configure which streams are available for compilation.

## Non-Goals

- No changes to `CommitCondition`, `JoinEventStream`, or `fromStreams`.
- No helper functions (`anyOf` / `allOf`) exposed as public API — those are internal compiler
  details only.
- No changes to `query()` parameter positions or shapes beyond accepting `DcbQuery` as the
  first argument.
- No unified `QueryOptions` object parameter — this belongs to a separate breaking-change
  branch.

## Core Principle

The generic API knows nothing about `types` / `tags` as first-class concepts. It only evaluates
selector algebra over stream names.

DCB support is an opt-in compilation step that:

1. detects whether the first argument to `query()` is a `DcbQuery` object,
2. validates and normalizes it,
3. resolves `types` and `tags` to concrete stream names via `typeAccessor` / `tagsAccessor`,
4. compiles them into the existing selector algebra,
5. delegates execution to the unchanged generic path.

---

## Input Shape (DcbQuery)

```js
// QueryItem — one row in the consistency boundary
{
  types: ['TypeA', 'TypeB'],   // OR within the item (optional)
  tags:  ['tag:1', 'tag:2']    // AND within the item (optional)
}

// DcbQuery — the full boundary
{
  items: [item1, item2, ...]   // OR across items
}
```

TypeScript-style for clarity:

```ts
interface QueryItem {
  types?: readonly string[];  // event-type names — OR semantics within the item
  tags?:  readonly string[];  // tag values — AND semantics within the item
}

interface DcbQuery {
  items: readonly QueryItem[]; // OR across items; empty = match all
}
```

**Detection in `query()`**: if the first argument is a plain object with an `items` property
(not an array), it is treated as a `DcbQuery`.

---

## API Surface

`query()` gains one overload. Existing call sites are unchanged:

```js
// Existing — selector array (flat or nested):
const { stream, condition } = store.query(['TypeA', 'TypeB'], matcher, minRevision, raw);
const { stream, condition } = store.query([['tag:1', 'TypeA'], ['tag:2', 'TypeB']]);

// New — DcbQuery object:
const { stream, condition } = store.query({
    items: [
        { types: ['CourseCreated', 'CourseCapacityChanged'], tags: ['course:jdsj4'] },
        { types: ['StudentCreated'],                         tags: ['student:gfh3j'] }
    ]
}, matcher, minRevision, raw);
```

`matcher`, `minRevision`, and `raw` behave identically in both forms. `CommitCondition` is
constructed and checked exactly as today — the compiled selector flows in where a plain selector
array would.

---

## Mapping DcbQuery → Selector

### Per item

| Item shape | Compiled sub-selector |
|---|---|
| `{ types: [A] }` | `'A'` (single string) |
| `{ types: [A, B] }` | `['A', 'B']` (OR group — inner array at depth +1) |
| `{ tags: [T1, T2] }` | `['tags/T1', 'tags/T2']` (AND, no type restriction) |
| `{ types: [A, B], tags: [T1, T2] }` | `['tags/T1', 'tags/T2', ['A', 'B']]` (AND of tags + type OR-group) |
| `{}` / `{ types: undefined }` / `{ types: [] }` | `'_all'` (no constraint → match all for that item) |
| `{ types: null }` or `{ tags: null }` | **throws** — explicit `null` is a programming error |

### Query level

The top-level OR array wraps all compiled items:

```
items = [i1, i2, i3]  →  [compiled(i1), compiled(i2), compiled(i3)]
items = []             →  throws — use getEventStream('_all') for an unconstrained read
```

### Full example

```
items = [
  { types: ['CourseCreated', 'CourseCapacityChanged', 'StudentSubscribedToCourse'],
    tags:  ['course:jdsj4'] },
  { types: ['StudentCreated', 'StudentSubscribedToCourse'],
    tags:  ['student:gfh3j'] }
]
```

Compiles to:

```js
[
    ['tags/course:jdsj4', ['CourseCreated', 'CourseCapacityChanged', 'StudentSubscribedToCourse']],
    ['tags/student:gfh3j', ['StudentCreated', 'StudentSubscribedToCourse']]
]
```

Which is equivalent to the handwritten nested selector from `dcb.md`, but with
tag streams resolved through the `tags/{tag}` naming convention.

---

## Opt-In via typeAccessor and tagsAccessor

The compiler must know how to turn type names and tag values into stream names. This requires
the corresponding accessor to be configured:

### typeAccessor

- Already exists. Maps event payloads to a type string and creates a stream per unique type.
- Stream naming: type value directly (e.g. `CourseCreated` → stream `CourseCreated`).
- Without `typeAccessor`: a `DcbQuery` that references `types` throws at `query()` time with a
  clear message — the system cannot guarantee type streams exist.

### tagsAccessor

- New config option, parallel to `typeAccessor`.
- Accepts a dot-notation string path (preferred, e.g. `'tags'` or `'meta.tags'`) or a function
  `(event) => string[]` that extracts the tag values from an event payload on write.
- On each `commit()`, `tagsAccessor` extracts tags and writes the event into one stream per tag
  (in addition to the entity stream).
- **Stream naming**: `tags/{tag}` — e.g. tag value `course:jdsj4` → stream `tags/course:jdsj4`.
  Tags themselves are recommended to use `/` as a hierarchy separator for selective subscriptions
  (e.g. `course/jdsj4` → stream `tags/course/jdsj4`).  
  _Note: in a future breaking change, `typeAccessor` may also migrate to `types/{type}` for
  consistency._
- Without `tagsAccessor`: a `DcbQuery` that references `tags` throws at `query()` time.

### Validation at query() time

```
DcbQuery has types  →  requires typeAccessor configured
DcbQuery has tags   →  requires tagsAccessor configured
```

Missing streams (type or tag stream not yet created) follow the existing selector semantics:
- In an OR group: skipped — no contribution from that branch.
- In an AND group: the entire AND branch yields nothing.

Supported configurations:

| typeAccessor | tagsAccessor | Usable for |
|:---:|:---:|---|
| ✓ | — | Type-scoped DCB queries (tags in QueryItem not allowed) |
| — | ✓ | Tag-only queries (unusual; no type restriction per item) |
| ✓ | ✓ | Full DCB: types + tags per item |

---

## Implementation Sketch

```js
function compileDcbQuery(query, resolveType, resolveTag) {
    if (!Array.isArray(query.items) || query.items.length === 0) {
        throw new Error('DcbQuery.items must be a non-empty array. Use getEventStream(\'_all\') for an unconstrained read.');
    }
    return query.items.map(item => compileItem(item, resolveType, resolveTag));
}

function compileItem(item, resolveType, resolveTag) {
    if (item === null || item === undefined) {
        throw new Error('DcbQuery item must be an object, got ' + item);
    }
    if (item.tags !== undefined && !Array.isArray(item.tags)) {
        throw new Error('DcbQuery item.tags must be an array or undefined, got ' + typeof item.tags);
    }
    if (item.types !== undefined && !Array.isArray(item.types)) {
        throw new Error('DcbQuery item.types must be an array or undefined, got ' + typeof item.types);
    }

    const terms = [];

    for (const tag of item.tags ?? []) {
        terms.push(resolveTag(tag));          // resolveTag: tag → 'tags/{tag}'
    }
    if (item.types?.length) {
        const typeStreams = item.types.map(resolveType);
        terms.push(typeStreams.length === 1 ? typeStreams[0] : typeStreams);
    }

    if (terms.length === 0) return '_all';    // {} or {types:[], tags:[]} → match all
    if (terms.length === 1) return terms[0];
    return terms;                             // inner array = AND level
}
```

`resolveType(type)` returns the stream name for a given event type.  
`resolveTag(tag)` returns `'tags/' + tag` (and throws if `tagsAccessor` is not configured).

Both throw with a descriptive message if the corresponding accessor is not configured.

The compiled selector is passed to the existing `fromStreams` / `JoinEventStream` path without
any further modification. The normalizer in `indexUtil.normalizeSelector` handles any redundant
nesting automatically.

---

## Usage Example

```js
const store = new EventStore('courses', {
    storageDirectory: './data',
    typeAccessor: 'type',
    tagsAccessor: 'tags'
});

store.on('ready', () => {

    function handleSubscribeStudent(command) {
        const { stream, condition } = store.query({
            items: [
                { types: ['CourseCreated', 'CourseCapacityChanged', 'StudentSubscribedToCourse'],
                  tags:  [`course:${command.courseId}`] },
                { types: ['StudentCreated', 'StudentSubscribedToCourse'],
                  tags:  [`student:${command.studentId}`] }
            ]
        });

        const model = buildModel(stream);
        if (!model.canSubscribe(command)) {
            throw new Error('Subscription not allowed');
        }

        store.commit(`enrollments`, [
            { type: 'StudentSubscribedToCourse',
              tags: [`course:${command.courseId}`, `student:${command.studentId}`],
              ...command }
        ], condition);
    }

});
```

---

## Rollout

1. Add `tagsAccessor` configuration option to `EventStore` constructor (parallel to
   `typeAccessor`): on each `commit()`, extract tags and write the event into one additional
   stream per tag.
2. Implement `compileDcbQuery` as a private module function.
3. In `query()`: detect `DcbQuery` (object with `items` property, not an array), validate
   accessor prerequisites, compile to selector, pass through to the existing execution path.
4. Emit clear errors when accessors are missing and the query requires them.
5. Add conformance tests:
   - `items = []` → throws
   - `items = null` → throws
   - item `= null` → throws
   - `{ types: null }` → throws
   - `{ tags: null }` → throws
   - `{}` / `{ types: [] }` / `{ tags: [] }` → compiles to `'_all'` for that item
   - type-only single item → plain string or flat OR array
   - tag+type item → AND array with `tags/` prefixed tag streams + inner OR group for types
   - multi-item → top-level OR
   - missing type stream in OR → branch skipped
   - missing tag stream in AND → branch yields nothing
   - no `typeAccessor` + `types` reference → throws at `query()` time
   - no `tagsAccessor` + `tags` reference → throws at `query()` time

---

## Decisions

1. **Tag stream naming**: `tags/{tag}` — hierarchical prefix avoids collisions with entity
   streams. Tag values themselves are recommended to use `/` as a separator for selective
   subscriptions (e.g. `course/jdsj4` → stream `tags/course/jdsj4`). A future breaking change
   may align `typeAccessor` to `types/{type}` for consistency.

2. **`tagsAccessor` dot-notation**: Supports both a dot-notation string path (e.g. `'tags'` or
   `'meta.tags'`) and a function `(event) => string[]`. The string path is preferred for the
   common case — it is consistent with `typeAccessor` and enables fast static analysis.

3. **Malformed items**: Explicit `null` on `item`, `item.types`, or `item.tags` → throw.
   Missing/undefined properties → treat as empty (no constraint from that dimension).
   An empty array (`[]`) for `types` or `tags` is also treated as missing.

4. **`items = []` semantics**: Throws — an empty item list is almost always a programming
   error. For an intentional unconstrained read, use `getEventStream('_all')` directly.
