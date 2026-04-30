import fs from 'fs';
import path from 'path';
import events from 'events';
import Partition, { ReadOnly as ReadOnlyPartition } from '../Partition.js';
import Index, { ReadOnly as ReadOnlyIndex } from '../Index.js';
import { assert, wrapAndCheck, kWayMerge } from '../util.js';
import { createHmac, matches, buildMetadataForMatcher } from '../metadataUtil.js';

const DEFAULT_READ_BUFFER_SIZE = 4 * 1024;

/**
 * Default ordered list of document property paths used as discriminant keys when classifying
 * object matchers into the fast-lookup table. Each path may use dot-notation for nested access
 * (e.g. `'payload.type'` matches `{ payload: { type: … } }`). The first path that resolves to
 * a scalar value in a given matcher wins; remaining paths are not examined for that matcher.
 */
const DEFAULT_MATCHER_PROPERTIES = ['stream', 'payload.type'];

/**
 * Default maximum number of partition file descriptors kept open simultaneously.
 * Partitions beyond this limit are evicted using LRU order. 0 disables the limit.
 */
const DEFAULT_MAX_OPEN_PARTITIONS = 1024;

/**
 * Read a scalar value at a dot-notation path from an object.
 * Returns `undefined` if any path segment is absent or the intermediate value is not an object.
 *
 * @param {object} obj
 * @param {string} path  Dot-separated property path, e.g. `'payload.type'`.
 * @returns {*}
 */
function getPropAtPath(obj, path) {
    let current = obj;
    const parts = path.split('.');
    for (const part of parts) {
        if (current == null || typeof current !== 'object') return undefined;
        current = current[part];
    }
    return current;
}

/**
 * Reverses the items of an iterable
 * @param {Generator|Iterable} iterator
 * @returns {Generator<*>}
 */
function *reverse(iterator) {
    const items = Array.from(iterator);
    for (let i = items.length - 1; i >= 0; i--) {
        yield items[i];
    }
}

/**
 * @typedef {object|function(object):boolean} Matcher
 */

/**
 * An append-only storage with highly performant positional range scans.
 * It's highly optimized for an event-store and hence does not support compaction or data-rewrite, nor any querying
 */
class ReadableStorage extends events.EventEmitter {

    /**
     * @param {string} [storageName] The name of the storage.
     * @param {object} [config] An object with storage parameters.
     * @param {object} [config.serializer] A serializer object with methods serialize(document) and deserialize(data).
     * @param {function(object): string} config.serializer.serialize Default is JSON.stringify.
     * @param {function(string): object} config.serializer.deserialize Default is JSON.parse.
     * @param {string} [config.dataDirectory] The path where the storage data should reside. Default '.'.
     * @param {string} [config.indexDirectory] The path where the indexes should be stored. Defaults to dataDirectory.
     * @param {string} [config.indexFile] The name of the primary index. Default '{storageName}.index'.
     * @param {number} [config.readBufferSize] Size of the read buffer in bytes. Default 4096.
     * @param {object} [config.indexOptions] An options object that should be passed to all indexes on construction.
     * @param {string} [config.hmacSecret] A private key that is used to verify matchers retrieved from indexes.
     * @param {object} [config.metadata] A metadata object to be stored in all partitions belonging to this storage.
     * @param {string[]} [config.matcherProperties] Ordered list of document property paths (dot-notation) used as
     *   discriminant keys for the fast secondary-index lookup table. Only the first property that resolves to a scalar
     *   value inside a given object matcher is used; the rest are checked via the full `matches()` fallback.
     *   Default: `['stream', 'payload.type']`.
     * @param {number} [config.maxOpenPartitions] Maximum number of partition file descriptors kept open at one time.
     *   When the limit is reached the least-recently-used partition is closed to make room. 0 disables the limit.
     *   Default: 1024.
     */
    constructor(storageName = 'storage', config = {}) {
        super();
        if (typeof storageName !== 'string') {
            config = storageName;
            storageName = undefined;
        }

        this.storageFile = storageName || 'storage';
        const defaults = {
            serializer: { serialize: JSON.stringify, deserialize: JSON.parse },
            dataDirectory: '.',
            indexFile: this.storageFile + '.index',
            indexOptions: {},
            hmacSecret: '',
            metadata: {},
            matcherProperties: DEFAULT_MATCHER_PROPERTIES,
            maxOpenPartitions: DEFAULT_MAX_OPEN_PARTITIONS
        };
        config = Object.assign(defaults, config);
        this.serializer = config.serializer;

        this.hmac = createHmac(config.hmacSecret);

        this.dataDirectory = path.resolve(config.dataDirectory);

        this.matcherProperties = config.matcherProperties;
        this.maxOpenPartitions = config.maxOpenPartitions;
        /** @private Map<id, true> in LRU order (oldest entry first). */
        this._openPartitionLru = new Map();

        this.scanPartitions(config);
        this.initializeIndexes(config);
    }

    /**
     * @protected
     * @param {string} name
     * @param {object} [options]
     * @returns {{ index: ReadableIndex, matcher?: Matcher }}
     */
    createIndex(name, options = {}) {
        /** @type ReadableIndex */
        const index = new ReadOnlyIndex(name, options);
        return { index };
    }

    /**
     * @protected
     * @param {string} name
     * @param {object} [options]
     * @returns {ReadablePartition}
     */
    createPartition(name, options = {}) {
        return new ReadOnlyPartition(name, options);
    }

    /**
     * Create/open the primary index and build the base configuration for all secondary indexes.
     *
     * @private
     * @param {object} config The configuration object
     * @returns void
     */
    initializeIndexes(config) {
        this.indexDirectory = path.resolve(config.indexDirectory || this.dataDirectory);

        this.indexOptions = config.indexOptions;
        this.indexOptions.dataDirectory = this.indexDirectory;
        // Safety precaution to prevent accidentally restricting main index
        delete this.indexOptions.matcher;
        const { index } = this.createIndex(config.indexFile, this.indexOptions);
        this.index = index;
        this.secondaryIndexes = {};
        this.readonlyIndexes = {};

        /**
         * Fast discriminant lookup table.
         * Map<propPath: string, Map<discriminantValue: string, Set<indexName: string>>>
         * Only populated for object matchers that contain at least one of this.matcherProperties.
         * @private
         */
        this._matcherIndex = new Map();
        /** @private Set<indexName> for function matchers (always evaluated in full). */
        this._functionMatcherIndexes = new Set();
        /**
         * @private Set<indexName> for object matchers that contain none of this.matcherProperties,
         * and for matchers that are null/undefined (match everything).
         */
        this._unclassifiedObjectIndexes = new Set();
    }

    /**
     * The amount of documents in the storage.
     * @returns {number}
     */
    get length() {
        return this.index.length;
    }

    /**
     * Scan the data directory for all existing partitions.
     * Every file beginning with the storageFile name is considered a partition.
     *
     * @private
     * @param {object} config The configuration object containing options for the partitions.
     * @returns void
     */
    scanPartitions(config) {
        const defaults = {
            readBufferSize: DEFAULT_READ_BUFFER_SIZE
        };
        this.partitionConfig = Object.assign(defaults, config);
        this.partitions = Object.create(null);

        const files = fs.readdirSync(this.dataDirectory);
        for (let file of files) {
            if (file.substr(-6) === '.index') continue;
            if (file.substr(-7) === '.branch') continue;
            if (file.substr(-5) === '.lock') continue;
            if (file.substr(0, this.storageFile.length) !== this.storageFile) continue;

            const partition = this.createPartition(file, this.partitionConfig);
            this.partitions[partition.id] = partition;
        }
    }

    /**
     * Open the storage and indexes and create read and write buffers eagerly.
     * Will emit an 'opened' event if finished.
     *
     * @api
     * @returns {boolean}
     */
    open() {
        this.index.open();

        this.forEachSecondaryIndex(index => index.open());

        this.emit('opened');
        return true;
    }

    /**
     * Close the storage and frees up all resources.
     * Will emit a 'closed' event when finished.
     *
     * @api
     * @returns void
     */
    close() {
        this.index.close();
        this.forEachSecondaryIndex(index => index.close());
        for (let index of Object.values(this.readonlyIndexes)) {
            index.close();
        }
        this.forEachPartition(partition => partition.close());
        this.emit('closed');
    }

    /**
     * Get a partition either by name or by id.
     * If a partition with the given name does not exist, a new one will be created.
     * If a partition with the given id does not exist, an error is thrown.
     *
     * @protected
     * @param {string|number} partitionIdentifier The partition name or the partition Id
     * @returns {ReadablePartition}
     * @throws {Error} If an id is given and no such partition exists.
     */
    getPartition(partitionIdentifier) {
        assert(partitionIdentifier in this.partitions, `Partition #${partitionIdentifier} does not exist.`);

        const partition = this.partitions[partitionIdentifier];

        if (this.maxOpenPartitions > 0) {
            if (!partition.isOpen()) {
                // Partition needs to be opened. Evict the LRU open partition if at capacity.
                if (this._openPartitionLru.size >= this.maxOpenPartitions) {
                    for (const [lruId] of this._openPartitionLru) {
                        this._openPartitionLru.delete(lruId);
                        const lruPartition = this.partitions[lruId];
                        if (lruPartition && lruPartition.isOpen()) {
                            lruPartition.close();
                            break;
                        }
                        // Stale LRU entry (partition was closed externally) — keep looking.
                    }
                }
                this._openPartitionLru.set(partitionIdentifier, true);
            } else {
                // Already open: move to tail (most-recently-used end).
                this._openPartitionLru.delete(partitionIdentifier);
                this._openPartitionLru.set(partitionIdentifier, true);
            }
        }

        partition.open();
        return partition;
    }

    /**
     * Register a handler that is called before a document is read from a partition.
     * The handler receives the position and the partition metadata and may throw to abort the read.
     * Multiple handlers can be registered; all run on every read in registration order.
     * Equivalent to `storage.on('preRead', hook)`.
     *
     * @api
     * @param {function(number, object): void} hook A function receiving (position, partitionMetadata).
     */
    preRead(hook) {
        this.on('preRead', hook);
    }

    /**
     * @protected
     * @param {number} partitionId The partition to read from.
     * @param {number} position The file position to read from.
     * @param {number} [size] The expected byte size of the document at the given position.
     * @returns {object} The document stored at the given position.
     * @throws {Error} if the document at the given position can not be deserialized.
     */
    readFrom(partitionId, position, size) {
        const partition = this.getPartition(partitionId);
        if (this.listenerCount('preRead') > 0) {
            this.emit('preRead', position, partition.metadata);
        }
        const data = partition.readFrom(position, size);
        return this.serializer.deserialize(data);
    }

    /**
     * Read a single document from the given position, in the full index or in the provided index.
     *
     * @api
     * @param {number} number The 1-based document number (inside the given index) to read.
     * @param {ReadableIndex} [index] The index to use for finding the document position.
     * @returns {object} The document at the given position inside the index.
     */
    read(number, index) {
        index = index || this.index;

        if (!index.isOpen()) {
            index.open();
        }

        const entry = index.get(number);
        if (entry === false) {
            return false;
        }

        return this.readFrom(entry.partition, entry.position, entry.size);
    }

    /**
     * Read a range of documents from the given position range, in the full index or in the provided index.
     * Returns a generator in order to reduce memory usage and be able to read lots of documents with little latency.
     *
     * @api
     * @param {number} from The 1-based document number (inclusive) to start reading from.
     * @param {number} [until] The 1-based document number (inclusive) to read until. Defaults to index.length.
     * @param {ReadableIndex|false} [index] The index to use for finding the documents in the range.
     *   Pass `false` to skip the global index and iterate all partitions directly in sequenceNumber order
     *   (useful when the global index is unavailable or corrupted).
     * @returns {Generator<object>} A generator that will read each document in the range one by one.
     */
    *readRange(from, until = -1, index = null) {
        const lengthSource = index || this.index;
        if (!lengthSource.isOpen()) {
            lengthSource.open();
        }

        const readFrom = wrapAndCheck(from, lengthSource.length);
        const readUntil = wrapAndCheck(until, lengthSource.length);
        assert(readFrom > 0 && readUntil > 0, `Range scan error for range ${from} - ${until}.`);

        if (readFrom > readUntil) {
            const batchSize = 10;
            let batchUntil = readFrom;
            while (batchUntil >= readUntil) {
                const batchFrom = Math.max(readUntil, batchUntil - batchSize);
                yield* reverse(this.iterateRange(batchFrom, batchUntil, index));
                batchUntil = batchFrom - 1;
            }
            return undefined;
        }

        yield* this.iterateRange(readFrom, readUntil, index);
    }

    /**
     * Iterate all documents in this storage in range from to until inside the index.
     * If index is false, iterates all partitions directly in sequenceNumber order.
     * @private
     * @param {number} from
     * @param {number} until
     * @param {ReadableIndex|false|null} index
     * @returns {Generator<object>}
     */
    *iterateRange(from, until, index) {
        if (index === false) {
            // Explicitly disabled index: iterate all partitions and merge by sequenceNumber.
            // Document header sequenceNumber is 0-based; from/until are 1-based index positions.
            for (const entry of this.iterateDocumentsNoIndex(from - 1, until - 1)) {
                yield entry.document;
            }
            return;
        }

        const idx = index || this.index;
        const entries = idx.range(from, until);
        for (let entry of entries) {
            const document = this.readFrom(entry.partition, entry.position, entry.size);
            yield document;
        }
    }

    /**
     * Open an existing readonly index for reading, without registering it in the secondary indexes write path.
     * Use this for indexes whose files carry a status marker (e.g. `stream-foo.closed.index`).
     *
     * @api
     * @param {string} name The readonly index name (e.g. 'stream-foo.closed').
     * @returns {ReadableIndex}
     * @throws {Error} if the readonly index does not exist.
     */
    openReadonlyIndex(name) {
        if (name in this.readonlyIndexes) {
            return this.readonlyIndexes[name];
        }
        const indexName = this.storageFile + '.' + name + '.index';
        assert(fs.existsSync(path.join(this.indexDirectory, indexName)), `Index "${name}" does not exist.`);
        const { index } = this.createIndex(indexName, Object.assign({}, this.indexOptions));
        index.open();
        this.readonlyIndexes[name] = index;
        return index;
    }

    /**
     * Open an existing index.
     *
     * @api
     * @param {string} name The index name.
     * @param {Matcher} [matcher] The matcher object or function that the index needs to have been defined with. If not given it will not be validated.
     * @returns {ReadableIndex}
     * @throws {Error} if the index with that name does not exist.
     * @throws {Error} if the HMAC for the matcher does not match.
     */
    openIndex(name, matcher) {
        if (name === '_all') {
            return this.index;
        }
        if (name in this.secondaryIndexes) {
            return this.secondaryIndexes[name].index;
        }

        const indexName = this.storageFile + '.' + name + '.index';
        assert(fs.existsSync(path.join(this.indexDirectory, indexName)), `Index "${name}" does not exist.`);

        const metadata = buildMetadataForMatcher(matcher, this.hmac);
        let { index } = this.secondaryIndexes[name] = this.createIndex(indexName, Object.assign({}, this.indexOptions, { metadata }));

        // Register the actual stored matcher (may have been reconstructed from metadata by WritableStorage.createIndex).
        this._registerIndexMatcher(name, this.secondaryIndexes[name].matcher);

        index.open();
        return index;
    }

    /**
     * Remove a secondary index from the write path and the fast matcher lookup table.
     *
     * @api
     * @param {string} name The secondary index name to remove.
     */
    removeSecondaryIndex(name) {
        const entry = this.secondaryIndexes[name];
        if (entry) {
            this._unregisterIndexMatcher(name, entry.matcher);
            delete this.secondaryIndexes[name];
        }
    }

    /**
     * Find the first discriminant property for an object matcher.
     * Returns `{ propPath, value }` for the first entry in `this.matcherProperties` that resolves
     * to a non-null, non-object scalar inside the matcher, or `null` if none is found.
     *
     * @private
     * @param {object} matcher
     * @returns {{ propPath: string, value: string }|null}
     */
    _findDiscriminant(matcher) {
        for (const propPath of this.matcherProperties) {
            const value = getPropAtPath(matcher, propPath);
            if (value !== undefined && value !== null && typeof value !== 'object') {
                return { propPath, value: String(value) };
            }
        }
        return null;
    }

    /**
     * Register an index name in the fast discriminant lookup structures.
     *
     * - Function matchers go into `_functionMatcherIndexes`.
     * - Object matchers with a recognisable discriminant property go into `_matcherIndex`.
     * - Everything else (null/undefined matcher or object with no discriminant property) goes
     *   into `_unclassifiedObjectIndexes`.
     *
     * @private
     * @param {string} indexName
     * @param {Matcher} matcher
     */
    _registerIndexMatcher(indexName, matcher) {
        if (typeof matcher === 'function') {
            this._functionMatcherIndexes.add(indexName);
            return;
        }
        if (matcher && typeof matcher === 'object') {
            const discriminant = this._findDiscriminant(matcher);
            if (discriminant) {
                let propMap = this._matcherIndex.get(discriminant.propPath);
                if (!propMap) {
                    propMap = new Map();
                    this._matcherIndex.set(discriminant.propPath, propMap);
                }
                let indexSet = propMap.get(discriminant.value);
                if (!indexSet) {
                    indexSet = new Set();
                    propMap.set(discriminant.value, indexSet);
                }
                indexSet.add(indexName);
                return;
            }
        }
        // null/undefined matcher or object with no discriminant property.
        this._unclassifiedObjectIndexes.add(indexName);
    }

    /**
     * Remove an index name from the fast discriminant lookup structures.
     *
     * @private
     * @param {string} indexName
     * @param {Matcher} matcher
     */
    _unregisterIndexMatcher(indexName, matcher) {
        if (typeof matcher === 'function') {
            this._functionMatcherIndexes.delete(indexName);
            return;
        }
        if (matcher && typeof matcher === 'object') {
            const discriminant = this._findDiscriminant(matcher);
            if (discriminant) {
                const propMap = this._matcherIndex.get(discriminant.propPath);
                if (propMap) {
                    const indexSet = propMap.get(discriminant.value);
                    if (indexSet) {
                        indexSet.delete(indexName);
                        if (indexSet.size === 0) propMap.delete(discriminant.value);
                    }
                    if (propMap.size === 0) this._matcherIndex.delete(discriminant.propPath);
                }
                return;
            }
        }
        this._unclassifiedObjectIndexes.delete(indexName);
    }

    /**
     * Iterate documents across all partitions in sequenceNumber order using a k-way merge.
     * Opens any closed partition automatically.
     *
     * @protected
     * @param {number} [from=0] The 0-based sequenceNumber to start from (inclusive).
     * @param {number} [until=Number.MAX_SAFE_INTEGER] The 0-based sequenceNumber to read until (inclusive).
     * @returns {Generator<{document: object, sequenceNumber: number, partitionName: string, position: number, size: number, partition: number}>}
     */
    *iterateDocumentsNoIndex(from = 0, until = Number.MAX_SAFE_INTEGER) {
        const streams = [];

        this.forEachPartition(partition => {
            if (!partition.isOpen()) {
                partition.open();
            }

            const found = partition.findDocument(from);
            if (found && found.headerOut.sequenceNumber <= until) {
                const nextPosition = found.headerOut.position + partition.documentWriteSize(found.headerOut.dataSize);
                const reader = partition.readAll(nextPosition, found.headerOut);
                streams.push({ ...found, reader, partition: partition.id, partitionName: partition.name });
            }
        });

        const items = [];
        kWayMerge(
            streams,
            stream => stream.headerOut.sequenceNumber,
            stream => {
                const next = stream.reader.next();
                if (!next.done && stream.headerOut.sequenceNumber <= until) {
                    stream.data = next.value;
                    return true;
                }
                return false;
            },
            stream => items.push({
                document: this.serializer.deserialize(stream.data),
                sequenceNumber: stream.headerOut.sequenceNumber,
                partitionName: stream.partitionName,
                position: stream.headerOut.position,
                size: stream.headerOut.dataSize,
                partition: stream.partition,
            })
        );

        yield* items;
    }

    /**
     * Helper method to iterate over all documents, invoking a callback for each one.
     * Pass `noIndex = true` to iterate all partitions directly in sequenceNumber order
     * (useful when the global index is unavailable or corrupted).
     * When `noIndex` is false the second callback argument is the raw index `EntryInterface`.
     * When `noIndex` is true the second callback argument has `{ partition, position, size, sequenceNumber, partitionName }`.
     *
     * @protected
     * @param {function(object, object): void} iterationHandler
     * @param {boolean} [noIndex=false] When true, bypasses the index and iterates partitions directly.
     */
    forEachDocument(iterationHandler, noIndex = false) {
        /* istanbul ignore if  */
        if (typeof iterationHandler !== 'function') {
            return;
        }

        if (noIndex) {
            for (const { document, ...entryInfo } of this.iterateDocumentsNoIndex()) {
                iterationHandler(document, entryInfo);
            }
            return;
        }

        const entries = this.index.all();

        for (let entry of entries) {
            const document = this.readFrom(entry.partition, entry.position, entry.size);
            iterationHandler(document, entry);
        }
    }

    /**
     * Helper method to iterate over all secondary indexes.
     *
     * When `matchDocument` is provided and `this.matcherProperties` is non-empty, a fast O(1)
     * lookup via the discriminant table is used instead of evaluating every matcher:
     *
     *   1. For each configured property path, the document's value is looked up in
     *      `_matcherIndex` to retrieve the small candidate set of indexes registered under
     *      that discriminant value.
     *   2. `_unclassifiedObjectIndexes` (object matchers with no discriminant property) and
     *      `_functionMatcherIndexes` (arbitrary function matchers) are always included as
     *      candidates and evaluated via the full `matches()` check.
     *   3. Each candidate's full matcher is still verified with `matches()` to handle
     *      multi-property matchers where only the first property was used as discriminant.
     *
     * @protected
     * @param {function(ReadableIndex, string)} iterationHandler
     * @param {object} [matchDocument] If supplied, only indexes the document matches on will be iterated.
     */
    forEachSecondaryIndex(iterationHandler, matchDocument) {
        /* istanbul ignore if  */
        if (typeof iterationHandler !== 'function') {
            return;
        }

        if (!matchDocument || this.matcherProperties.length === 0) {
            // No document filter, or discriminant lookup is disabled: iterate all.
            for (let indexName of Object.keys(this.secondaryIndexes)) {
                if (!matchDocument || matches(matchDocument, this.secondaryIndexes[indexName].matcher)) {
                    iterationHandler(this.secondaryIndexes[indexName].index, indexName);
                }
            }
            return;
        }

        // Fast path: build a candidate set using the discriminant table.
        const candidates = new Set();

        for (const propPath of this.matcherProperties) {
            const docValue = getPropAtPath(matchDocument, propPath);
            if (docValue !== undefined && docValue !== null && typeof docValue !== 'object') {
                const propMap = this._matcherIndex.get(propPath);
                if (propMap) {
                    const indexSet = propMap.get(String(docValue));
                    if (indexSet) {
                        for (const name of indexSet) candidates.add(name);
                    }
                }
            }
        }

        // Unclassified object indexes and function matchers always need evaluation.
        for (const name of this._unclassifiedObjectIndexes) candidates.add(name);
        for (const name of this._functionMatcherIndexes) candidates.add(name);

        for (const indexName of candidates) {
            const entry = this.secondaryIndexes[indexName];
            if (entry && matches(matchDocument, entry.matcher)) {
                iterationHandler(entry.index, indexName);
            }
        }
    }

    /**
     * Helper method to iterate over all partitions.
     *
     * @protected
     * @param {function(ReadablePartition)} iterationHandler
     */
    forEachPartition(iterationHandler) {
        /* istanbul ignore if  */
        if (typeof iterationHandler !== 'function') {
            return;
        }

        for (let partition of Object.keys(this.partitions)) {
            iterationHandler(this.partitions[partition]);
        }
    }

}

export default ReadableStorage;
export { matches };
