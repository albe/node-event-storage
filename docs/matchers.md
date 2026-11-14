# Matchers

Object matchers use the same shape in both modes. In object mode they are evaluated against
`{ stream, payload, metadata }`. In raw mode the same matcher is compiled into byte-level checks.

Matchers select events by content. They are accepted by `getEventStream()`, `getAllEvents()`, `fromStreams()`, `getEventStreamForCategory()`, `query()`, `EventStream.where()` and `createEventStream()`.

| Mode | Function matcher | Object matcher |
|------|------------------|----------------|
| `raw=false` (default) | `(payload, metadata) => boolean`; for `createEventStream()` `(document) => boolean` | Matched against `{ stream, payload, metadata }` |
| `raw=true` | `(buffer) => boolean` | Compiled to byte-level checks against compact JSON |

Prefer object matchers: they work in both modes, can be routed in O(1) on writes (see [`matcherProperties`](performance.md#matcherproperties)) and are persisted as plain JSON. For hot paths, reuse the same matcher object reference across calls so its compiled form is cached.

## Supported forms

- **Scalar equality**

```javascript
  { payload: { type: 'OrderPlaced' } }
```

- **Nested object matching**

```javascript
  { metadata: { tenantId: 'acme' } }
```

- **Array values with OR semantics**

```javascript
  { payload: { type: ['OrderPlaced', 'OrderCancelled'] } }
```

- **Array containment with `$has` and `$hasAny`**

  When the document field is an array and you want to match documents that contain a
  specific scalar element, use the `$has` operator. For matching any of several values
  use `$hasAny`. A plain scalar matcher against an array-valued field is always
  exact-equality (i.e. never matches an array).

```javascript
  // matches events whose payload.tags array includes 'featured'
  { payload: { tags: { $has: 'featured' } } }
  // equivalent to: Array.isArray(event.payload.tags) && event.payload.tags.includes('featured')

  // matches events whose payload.tags array includes 'featured' OR 'new'
  { payload: { tags: { $hasAny: ['featured', 'new'] } } }
  // equivalent to: Array.isArray(event.payload.tags) && ['featured','new'].some(v => event.payload.tags.includes(v))
```

  Both operators compile to fast byte-level checks in raw mode: the compiled matcher
  locates the `"tags":[` opener and scans the array's element level for the serialized
  scalar(s) without parsing nested objects. Use `$has`/`$hasAny` instead of a function
  matcher wherever possible for tag-membership predicates.

- **Scalar comparison operators**

```javascript
  { payload: { amount: { $gte: 100, $lt: 1000 } } }
```

Supported operators are `$gt`, `$gte`, `$lt`, `$lte`, `$eq`, `$ne`, `$has`, and `$hasAny`.
Multiple operators on the same field are combined with AND semantics, but only when
they operate on the same value shape. `$gt`/`$gte`/`$lt`/`$lte`/`$eq`/`$ne` are scalar
operators and can be freely combined (e.g. `{ $gte: 100, $lt: 1000 }`). `$has` and
`$hasAny` are non-scalar (array-containment) operators and cannot be combined with scalar
operators — `{ $has: 'tag', $gt: 10 }` is nonsensical because a value cannot
simultaneously be an array (for `$has`) and a scalar comparable to `10`.

`$eq` is equivalent to plain equality, so prefer the simpler form when possible:

```javascript
{ payload: { type: 'OrderPlaced' } }
// equivalent to:
{ payload: { type: { $eq: 'OrderPlaced' } } }
```

Operator matching is intended for scalar values (`string`, `number`, `boolean`, `null`).
For arrays, objects, or custom raw encodings, use plain equality or a function matcher.

Important: raw object matchers require the default compact JSON serializer format. If you use a custom serializer, use a raw function matcher instead.
