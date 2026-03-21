export { default as EventStore, ExpectedVersion, OptimisticConcurrencyError, LOCK_THROW, LOCK_RECLAIM } from './src/EventStore.js';
export { default as EventStream } from './src/EventStream.js';
export { default as Storage, ReadOnly as ReadOnlyStorage, StorageLockedError } from './src/Storage.js';
export { default as Index, ReadOnly as ReadOnlyIndex, Entry as IndexEntry } from './src/Index.js';
export { default as Consumer } from './src/Consumer.js';
