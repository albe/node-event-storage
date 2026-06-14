import { assert } from '../utils/util.js';
import Storage from '../Storage/ReadableStorage.js';
import { matches } from '../utils/metadataUtil.js';

const DEFAULT_CATCH_UP_BATCH = 10;
const DEFAULT_OPTIONS = Object.freeze({ batchSize: DEFAULT_CATCH_UP_BATCH, matcher: null });

const toArray = (value) => Array.isArray(value) ? value : [value];

const toIndexName = (streamName) => {
    if (streamName === '_all' || streamName.startsWith('stream-')) {
        return streamName;
    }
    return 'stream-' + streamName;
};

function normalizeOptions(batchSizeOrOptions) {
    if (typeof batchSizeOrOptions === 'number') {
        return { batchSize: batchSizeOrOptions, matcher: null };
    }
    if (!batchSizeOrOptions) {
        return DEFAULT_OPTIONS;
    }
    return {
        batchSize: batchSizeOrOptions.batchSize ?? DEFAULT_CATCH_UP_BATCH,
        matcher: batchSizeOrOptions.matcher ?? null,
    };
}

/**
 * Storage-backed subscription helper for Consumer catch-up and live index-add notifications.
 */
class StorageSubscription {
    /**
     * @param {Storage} storage
     * @param {string|string[]} indexNames
     * @param {number|{ batchSize?: number, matcher?: object|function(object): boolean }} [batchSizeOrOptions]
     */
    constructor(storage, indexNames, batchSizeOrOptions = DEFAULT_CATCH_UP_BATCH) {
        assert(storage instanceof Storage, 'Must provide a storage for the consumer subscription.');
        const normalizedIndexNames = toArray(indexNames);
        assert(Array.isArray(normalizedIndexNames) && normalizedIndexNames.length > 0,
            'Must specify at least one index name for the consumer subscription.');

        const { batchSize, matcher } = normalizeOptions(batchSizeOrOptions);
        assert(Number.isInteger(batchSize) && batchSize > 0, 'Consumer subscription batchSize must be a positive integer.');
        assert(matcher === null || typeof matcher === 'function' || typeof matcher === 'object',
            'Consumer subscription matcher must be null, an object matcher, or a function.');

        this.storage = storage;
        this.indexNames = [...new Set(normalizedIndexNames)];
        for (const indexName of this.indexNames) {
            assert(typeof indexName === 'string' && indexName !== '', 'Consumer subscription index names must be non-empty strings.');
        }
        this.indexNameSet = new Set(this.indexNames);
        this.indexes = this.indexNames.map((indexName) => storage.openIndex(indexName));

        this.batchSize = batchSize;
        this.matcher = matcher;
        this.useGlobalOrdering = this.indexNames.length > 1 || this.matcher !== null;
        this.indexName = this.indexNames[0];
        this.index = this.useGlobalOrdering ? this.storage.index : this.indexes[0];

        this.handler = null;
        this.runId = 0;
    }

    /**
     * Build a StorageSubscription from an EventStream or JoinEventStream.
     *
     * In object mode, function predicates are adapted from `(payload, metadata) => boolean`
     * to document matcher functions accepted by StorageSubscription.
     *
     * @param {Storage} storage
     * @param {{ name?: string, streams?: string[], predicate?: object|function, raw?: boolean }} eventStream
     * @param {number} [batchSize=10]
     * @returns {StorageSubscription}
     */
    static fromEventStream(storage, eventStream, batchSize = DEFAULT_CATCH_UP_BATCH) {
        assert(eventStream && typeof eventStream === 'object', 'Must provide an EventStream or JoinEventStream.');
        assert(eventStream.raw !== true, 'StorageSubscription.fromEventStream() does not support raw EventStreams.');

        const streamNames = Array.isArray(eventStream.streams)
            ? eventStream.streams
            : [eventStream.name];
        assert(streamNames.length > 0 && streamNames.every((name) => typeof name === 'string' && name !== ''),
            'EventStream must provide at least one stream name.');

        let matcher = eventStream.predicate || null;
        if (typeof matcher === 'function') {
            const predicate = matcher;
            matcher = (document) => predicate(document.payload, document.metadata);
        }

        const indexNames = streamNames.map(toIndexName);
        return new StorageSubscription(storage, indexNames, { batchSize, matcher });
    }

    /**
     * @param {{
     *   getPosition: function(): number,
     *   onDocument: function(number, object): void,
     *   onCatchUpBatch: function(Array|Generator, function(): void): void,
     *   onCaughtUp: function(): void
     * }} handlers
     */
    start(handlers) {
        this.stop();
        const runId = ++this.runId;

        const catchUp = this.useGlobalOrdering ? this.createGlobalCatchUp(runId, handlers) : this.createSingleIndexCatchUp(runId, handlers);
        catchUp();
    }

    /**
     * @private
     * @param {number} runId
     * @param {{
     *   getPosition: function(): number,
     *   onDocument: function(number, object): void,
     *   onCatchUpBatch: function(Array|Generator, function(): void): void,
     *   onCaughtUp: function(): void
     * }} handlers
     * @returns {function(): void}
     */
    createSingleIndexCatchUp(runId, handlers) {
        const catchUp = () => {
            setImmediate(() => {
                if (runId !== this.runId) {
                    return;
                }

                if (this.index.length <= handlers.getPosition()) {
                    this.startListening(runId, handlers.onDocument, handlers.getPosition);
                    handlers.onCaughtUp();
                    return;
                }

                const maxBatchPosition = Math.min(handlers.getPosition() + this.batchSize + 1, this.index.length);
                const documents = this.storage.readRange(handlers.getPosition() + 1, maxBatchPosition, this.index);
                handlers.onCatchUpBatch(documents, () => {
                    if (runId === this.runId) {
                        catchUp();
                    }
                });
            });
        };
        return () => {
            catchUp();
        };
    }

    /**
     * @private
     * @param {number} runId
     * @param {{
     *   getPosition: function(): number,
     *   onDocument: function(number, object): void,
     *   onCatchUpBatch: function(Array|Generator, function(): void): void,
     *   onCaughtUp: function(): void
     * }} handlers
     * @returns {function(): void}
     */
    createGlobalCatchUp(runId, handlers) {
        const catchUp = () => {
            setImmediate(() => {
                if (runId !== this.runId) {
                    return;
                }

                const batch = this.readGlobalCatchUpBatch(handlers.getPosition());
                if (batch.length === 0) {
                    this.startListening(runId, handlers.onDocument, handlers.getPosition);
                    handlers.onCaughtUp();
                    return;
                }

                handlers.onCatchUpBatch(batch, () => {
                    if (runId === this.runId) {
                        catchUp();
                    }
                });
            });
        };
        return catchUp;
    }

    /**
     * @private
     * @param {number} position
     * @returns {Array<{ position: number, document: object }>}
     */
    readGlobalCatchUpBatch(position) {
        const from = position + 1;
        const until = this.storage.length;
        const batch = [];

        for (let current = from; current <= until; current++) {
            const document = this.storage.read(current, this.storage.index);
            if (!document) {
                continue;
            }
            if (!this.matchesPosition(current)) {
                continue;
            }
            if (!this.matchesDocument(document)) {
                continue;
            }

            batch.push({ position: current, document });
            if (batch.length >= this.batchSize) {
                break;
            }
        }

        return batch;
    }

    /**
     * @private
     * @param {number} position
     * @returns {boolean}
     */
    matchesPosition(position) {
        if (this.indexNameSet.has('_all')) {
            return true;
        }
        for (const index of this.indexes) {
            if (index === this.storage.index) {
                return true;
            }
            const indexPosition = index.find(position, true);
            if (indexPosition < 1) {
                continue;
            }
            const entry = index.get(indexPosition);
            if (entry && entry.number === position) {
                return true;
            }
        }
        return false;
    }

    /**
     * @private
     * @param {object} document
     * @returns {boolean}
     */
    matchesDocument(document) {
        if (!this.matcher) {
            return true;
        }
        if (typeof this.matcher === 'function') {
            return this.matcher(document);
        }
        return matches(document, this.matcher);
    }

    /**
     * @private
     * @param {number} runId
     * @param {(position: number, document: object) => void} onDocument
     * @param {() => number} getPosition
     */
    startListening(runId, onDocument, getPosition) {
        this.handler = (name, position, document) => {
            if (runId !== this.runId) {
                return;
            }
            if (!this.indexNameSet.has(name)) {
                return;
            }
            if (this.useGlobalOrdering) {
                if (position <= getPosition()) {
                    return;
                }
                if (!this.matchesDocument(document)) {
                    return;
                }
            } else if (position !== getPosition() + 1) {
                return;
            }
            onDocument(position, document);
        };
        this.storage.on('index-add', this.handler);
    }

    stop() {
        this.runId++;
        if (this.handler) {
            this.storage.removeListener('index-add', this.handler);
            this.handler = null;
        }
    }
}

export default StorageSubscription;