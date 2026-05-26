# Upgrading from 0.x to 1.0

Version 1.0 migrates the entire library to native **ES Modules (ESM)**. If your application was using `require('event-storage')` you will need to update your import statements and a few API references.

---

## 1. Node.js version

ESM support requires **Node.js 18 or later**. Check your version:

```bash
node --version
```

---

## 2. Update your `package.json`

Your own project must opt in to ESM. Add `"type": "module"` to your `package.json`:

```json
{
  "type": "module"
}
```

If you cannot migrate your whole project to ESM, you can rename individual files from `.js` to `.mjs` — Node.js treats `.mjs` files as ESM regardless of the `"type"` field.

---

## 3. Replace `require()` with `import`

### Basic import

```js
// Before (0.x)
const EventStore = require('event-storage');

// After (1.0)
import { EventStore } from 'event-storage';
```

### Named exports that were previously properties

In 0.x, constants and sub-classes were attached as properties on the default export. Constants are now individual named exports, while sub-classes remain accessible as properties of the parent class:

| 0.x | 1.0 |
|-----|-----|
| `EventStore.ExpectedVersion` | `import { ExpectedVersion } from 'event-storage'` |
| `EventStore.OptimisticConcurrencyError` | `import { OptimisticConcurrencyError } from 'event-storage'` |
| `EventStore.LOCK_THROW` | `import { LOCK_THROW } from 'event-storage'` |
| `EventStore.LOCK_RECLAIM` | `import { LOCK_RECLAIM } from 'event-storage'` |
| `require('event-storage').Storage` | `import { Storage } from 'event-storage'` |
| `Storage.ReadOnly` | `Storage.ReadOnly` (unchanged — still a property of `Storage`) |
| `Storage.StorageLockedError` | `import { StorageLockedError } from 'event-storage'` |
| `Index.ReadOnly` | `Index.ReadOnly` (unchanged — still a property of `Index`) |
| `Index.Entry` | `Index.Entry` (unchanged — still a property of `Index`) |

### Full import surface

All public exports from the package entry point:

```js
import {
  EventStore, ExpectedVersion, OptimisticConcurrencyError,
  EventStream,
  Storage, StorageLockedError,
  Index,
  Consumer,
  LOCK_THROW, LOCK_RECLAIM
} from 'event-storage';
```

---

## 4. Common patterns updated

### Creating an event store

```js
// Before
const EventStore = require('event-storage');
const eventstore = new EventStore('my-store', { storageDirectory: './data' });

// After
import { EventStore } from 'event-storage';
const eventstore = new EventStore('my-store', { storageDirectory: './data' });
```

### Optimistic concurrency

```js
// Before
eventstore.commit('my-stream', events, EventStore.ExpectedVersion.EmptyStream, callback);
// …and catching errors:
if (err instanceof EventStore.OptimisticConcurrencyError) { … }

// After
import { EventStore, ExpectedVersion, OptimisticConcurrencyError } from 'event-storage';
eventstore.commit('my-stream', events, ExpectedVersion.EmptyStream, callback);
if (err instanceof OptimisticConcurrencyError) { … }
```

### Lock modes

```js
// Before
const EventStore = require('event-storage');
const eventstore = new EventStore('my-store', {
    storageConfig: { lock: EventStore.LOCK_RECLAIM }
});

// After
import { EventStore, LOCK_RECLAIM } from 'event-storage';
const eventstore = new EventStore('my-store', {
    storageConfig: { lock: LOCK_RECLAIM }
});
```

### Using the Storage class directly

```js
// Before
const Storage = require('event-storage').Storage;
const ReadOnlyStorage = Storage.ReadOnly;
const store = new ReadOnlyStorage('mystore', { dataDirectory: './data' });

// After
import { Storage } from 'event-storage';
const store = new Storage.ReadOnly('mystore', { dataDirectory: './data' });
```

### Custom serialization / compression

```js
// Before
const { encode, decode } = require('@msgpack/msgpack');

// After
import { encode, decode } from '@msgpack/msgpack';
```

---

## 5. Relative file imports require `.js` extensions

If your project imports internal modules from `node_modules/event-storage` by path (uncommon), note that ESM requires explicit file extensions. This change is internal to the library and should not affect application code that imports from `'event-storage'`.

---

## 6. `__dirname` / `__filename`

These globals are not available in ESM. If you reference them in your own application code, replace them with:

```js
import { fileURLToPath } from 'url';
import path from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);
```

---

## 7. Dynamic `require()` / `createRequire`

If you have tooling or test helpers that still need CJS-style loading, use `createRequire`:

```js
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
```

This is a compatibility shim and is generally not needed when consuming `event-storage` itself.

---

# Upgrading from 1.0 / 1.1 to 1.2

Version 1.2 refines the Consumer API and tightens Stream-naming validation based on real-world usage patterns.

---

## 1. `scanConsumers()` callback shape changed

The callback passed to `scanConsumers()` now receives a more useful descriptor object instead of raw filenames.

### Before (1.0/1.1)

```js
eventstore.scanConsumers((err, consumers) => {
    if (err) throw err;
    // consumers is string[]
    consumers.forEach(filename => {
        console.log(filename); // e.g., "my-store.my-id.json"
    });
});
```

### After (1.2)

```js
eventstore.scanConsumers((err, consumers) => {
    if (err) throw err;
    // consumers is Array<{ name: string, stream: string, identifier: string }>
    consumers.forEach(({ stream, identifier }) => {
        console.log(stream, identifier); // e.g., "my-stream", "my-id"
        
        // Open the consumer directly
        const consumer = eventstore.getConsumer(stream, identifier);
    });
});
```

**Why the change?** The raw filename was rarely useful; the parsed stream/identifier pair enables automatic consumer discovery and re-registration. If you need auto-start on boot, use:

```js
eventstore.scanConsumers((err, consumers) => {
    if (err) throw err;
    // autoStart=true opens all discovered consumers and registers them
    // (or call .getConsumer(stream, identifier) explicitly for each one)
}, autoStart = true);
```

---

## 2. Stream-naming conventions expanded (and `typeAccessor` stricter)

The `typeAccessor` (used when registering event types as streams) now **validates stream names** against a whitelist of safe characters.

### What's allowed now

| Character | Example | Notes |
|---|---|---|
| Alphanumeric + underscore | `user.created` | unchanged |
| Dot, slash, hyphen | `payment-v2/created` | unchanged |
| Colon | `account:verified` | Common in event sourcing patterns |
| At-sign | `email@confirmed` | Safe for domain-style naming |
| Tilde, plus, equals, hash | `saga~id`, `order+sku`, `version=1`, `tag#final` | Additional safe characters |

### What's NOT allowed

- **Whitespace** (space, tab, newline) — stream names must be single tokens
- Other special chars — `*`, `?`, `"`, `<`, `>`, `|`, `\` are forbidden
- Repeated separators are not allowed: names like `Order..Placed`, `order//created`, `a::b`, or trailing separators like `order-` are rejected

### Before (1.0/1.1)

Would silently accept any string from `typeAccessor`:

```js
const eventstore = new EventStore('my-store', {
    typeAccessor: (e) => e.type
});
eventstore.commit('stream-1', [{ type: 'user created', payload: {} }], (err) => {
    // 1.0: accepted "user created" (with space)
    // 1.2: throws "Invalid stream name"
});
```

### After (1.2)

Validates on each commit and raises a clear error if the stream name is invalid:

```js
// Before committing, ensure typeAccessor returns valid names:
const eventstore = new EventStore('my-store', {
    typeAccessor: (e) => e.type
});
eventstore.commit('stream-1', [{ type: 'user_created', payload: {} }], (err) => {
    // 1.2: accepted (underscores are fine)
});

// Or use separators like colon/dash instead of spaces:
eventstore.commit('stream-1', [{ type: 'user:created', payload: {} }], (err) => {
    // 1.2: accepted (colon is now allowed)
});
```

**Action required:** If your `typeAccessor` returns names with spaces (or other disallowed chars), update it to use safe separators (`_`, `:`, `-`, `.`, etc.).

