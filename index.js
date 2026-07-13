import packageJson from './package.json' with { type: 'json' };
import EventStore, { ExpectedVersion, OptimisticConcurrencyError, CommitCondition, LOCK_THROW, LOCK_RECLAIM } from './src/EventStore.js';
import EventStream from './src/EventStream.js';
import Storage, { StorageLockedError } from './src/Storage.js';
import Index from './src/Index.js';
import LegacyConsumer from './src/LegacyConsumer.js';
import Consumer from './src/api/Consumer.js';
import EventStreamQuery, { StreamQuery } from './src/api/EventStreamQuery.js';
import RangeQuery from './src/api/RangeQuery.js';
import EventStreamResult from './src/api/EventStreamResult.js';
import { matches, buildRawBufferMatcher } from './src/utils/metadataUtil.js';

const VERSION = packageJson.version;
EventStore.VERSION = VERSION;

export {
	EventStore,
	EventStore as default,
	ExpectedVersion,
	OptimisticConcurrencyError,
	CommitCondition,
	LOCK_THROW,
	LOCK_RECLAIM,
	VERSION,
	EventStream,
	Storage,
	StorageLockedError,
	Index,
	LegacyConsumer,
	Consumer,
	EventStreamQuery,
	StreamQuery,
	RangeQuery,
	EventStreamResult,
	matches,
	buildRawBufferMatcher
};
