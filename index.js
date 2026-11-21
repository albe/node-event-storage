import packageJson from './package.json' with { type: 'json' };
import EventStore, { ExpectedVersion, OptimisticConcurrencyError, IdempotencyError, CommitCondition, LOCK_THROW, LOCK_RECLAIM, LOCK_AUTO } from './src/EventStore.js';
import EventStream from './src/EventStream.js';
import Storage, { StorageLockedError } from './src/Storage.js';
import Index from './src/Index.js';
import Consumer from './src/Consumer.js';
import { matches, buildRawBufferMatcher } from './src/utils/metadataUtil.js';

const VERSION = packageJson.version;
EventStore.VERSION = VERSION;

export {
	EventStore,
	EventStore as default,
	ExpectedVersion,
	OptimisticConcurrencyError,
	IdempotencyError,
	CommitCondition,
	LOCK_THROW,
	LOCK_RECLAIM,
	LOCK_AUTO,
	VERSION,
	EventStream,
	Storage,
	StorageLockedError,
	Index,
	Consumer,
	matches,
	buildRawBufferMatcher
};
