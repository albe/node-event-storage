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

In 0.x, constants and sub-classes were attached as properties on the default export. They are now individual named exports:

| 0.x | 1.0 |
|-----|-----|
| `EventStore.ExpectedVersion` | `import { ExpectedVersion } from 'event-storage'` |
| `EventStore.OptimisticConcurrencyError` | `import { OptimisticConcurrencyError } from 'event-storage'` |
| `EventStore.LOCK_THROW` | `import { LOCK_THROW } from 'event-storage'` |
| `EventStore.LOCK_RECLAIM` | `import { LOCK_RECLAIM } from 'event-storage'` |
| `require('event-storage').Storage` | `import { Storage } from 'event-storage'` |
| `Storage.ReadOnly` | `import { ReadOnlyStorage } from 'event-storage'` |
| `Storage.StorageLockedError` | `import { StorageLockedError } from 'event-storage'` |

### Full import surface

All public exports from the package entry point:

```js
import {
  EventStore, ExpectedVersion, OptimisticConcurrencyError,
  EventStream,
  Storage, ReadOnlyStorage, StorageLockedError,
  Index, ReadOnlyIndex, IndexEntry,
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

// After
import { Storage, ReadOnlyStorage } from 'event-storage';
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
