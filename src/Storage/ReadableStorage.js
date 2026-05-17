import fs from 'fs';
import path from 'path';
import events from 'events';
import Partition, { ReadOnly as ReadOnlyPartition } from '../Partition.js';
import Index, { ReadOnly as ReadOnlyIndex } from '../Index.js';
import { assert, wrapAndCheck, kWayMerge } from '../util.js';
import { scanForFiles } from '../fsUtil.js';
import { createHmac, matches, buildMetadataForMatcher } from '../metadataUtil.js';
import IndexMatcher from '../IndexMatcher.js';
import PartitionPool from '../PartitionPool.js';

const DEFAULT_READ_BUFFER_SIZE = 4 * 1024;
const NDJSON_NEWLINE = Buffer.from('\n');

/**
 * Default ordered list of document property paths used as discriminant keys when
 * classifying object matchers into the fast-lookup table.  Each path may use
 * dot-notation for nested access (e.g. `'payload.type'`).  The first path that
 * resolves to a scalar value in a given matcher wins; remaining paths are not
 * examined for that matcher.
 */
const DEFAULT_MATCHER_PROPERTIES = ['stream', 'payload.type'];

/**
 * Default maximum number of partition file descriptors kept open simultaneously.
 * Partitions beyond this limit are evicted using LRU order. 0 disables the limit.
 */
const DEFAULT_MAX_OPEN_PARTITIONS = 1024;

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

        const partitionDefaults = { readBufferSize: DEFAULT_READ_BUFFER_SIZE };
        this.partitionConfig = Object.assign(partitionDefaults, config);
        this.partitions = new PartitionPool(config.maxOpenPartitions);

        // initialized: null = not started (or scan cancelled), false = in progress, true = done
        this.initialized = null;

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

        /** Fast secondary-index lookup — classifies matchers for O(1) candidate resolution on write. */
        this.indexMatcher = new IndexMatcher(config.matcherProperties);
    }

    /**
     * The amount of documents in the storage.
     * @returns {number}
     */
    get length() {
        return this.index.length;
    }

    /**
     * Scan partitions and secondary index files; emit 'index-created' for each found index.
     * @param {function} done Called when both scans finish.
     */
    scanFiles(done) {
        const escaped = this.storageFile.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const partitionPattern = new RegExp(`^(${escaped}.*)$`);
        scanForFiles(this.dataDirectory, partitionPattern, (file) => {
            if (file.endsWith('.index') || file.endsWith('.branch') || file.endsWith('.lock')) return;
            const partition = this.createPartition(file, this.partitionConfig);
            this.partitions.add(partition.id, partition);
        }, (partErr) => {
            /* istanbul ignore if */
            if (partErr) throw partErr;

            // Scan was cancelled by close() between the two scan phases.
            if (this.initialized === null) return;

            // No secondary indexes exist yet — nothing to scan.
            if (!fs.existsSync(this.indexDirectory)) {
                return done();
            }
            const indexPattern = new RegExp(`^${escaped}\\.(.+)\\.index$`);
            scanForFiles(this.indexDirectory, indexPattern, (name) => {
                this.emit('index-created', name);
            }, (indexErr) => {
                // The directory could disappear between existsSync and readdir (e.g. test cleanup).
                /* istanbul ignore if */
                if (indexErr && indexErr.code !== 'ENOENT') throw indexErr;
                done();
            });
        });
    }

    /**
     * Only the primary index is opened eagerly; secondary indexes open on demand.
     *
     * @protected
     */
    openIndexes() {
        this.index.open();
    }

    /**
     * Open the storage; scans existing partitions and indexes asynchronously on first open.
     * Re-opens after `close()` are synchronous.
     * Will emit an `'opened'` event when finished.
     *
     * @api
     * @param {function(): void} [callback] Called after indexes open, before `'opened'` is emitted.
     *   Can be used as a synchronous alternative to listening to the `'opened'` event.
     * @returns {boolean}
     */
    open(callback) {
        if (this.initialized === true) {
            this.openIndexes();
            callback?.();
            this.emit('opened');
            return true;
        }
        if (this.initialized === false) {
            return true;
        }
        this.initialized = false;
        this.scanFiles(() => {
            // Guard: close() while scanning resets initialized to null.
            if (this.initialized === null) return;
            this.initialized = true;
            this.openIndexes();
            callback?.();
            this.emit('opened');
        });
        return true;
    }

    /**
     * Close the storage and free up all resources.
     * Will emit a 'closed' event when finished.
     *
     * @api
     */
    close() {
        // Cancel in-progress scan so the callback does not re-open after an explicit close.
        if (this.initialized === false) {
            this.initialized = null;
        }
        this.index.close();
        this.forEachSecondaryIndex(index => index.close());
        for (let index of Object.values(this.readonlyIndexes)) {
            index.close();
        }
        this.forEachPartition(partition => partition.close());
        this.emit('closed');
    }

    /**
     * Get a partition by its id.
     * If a partition with the given id does not exist, an error is thrown.
     *
     * @protected
     * @param {number|string} partitionIdentifier The partition Id
     * @returns {ReadablePartition}
     * @throws {Error} If no such partition exists.
     */
    getPartition(partitionIdentifier) {
        assert(this.partitions.has(partitionIdentifier), `Partition #${partitionIdentifier} does not exist.`);
        return this.partitions.open(partitionIdentifier);
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
        return this.serializer.deserialize(data.toString('utf8'));
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
            if (from > until) {
                for (const entry of this.iterateDocumentsNoIndexBackwards(from - 1, until - 1)) {
                    yield entry.document;
                }
            } else {
                for (const entry of this.iterateDocumentsNoIndex(from - 1, until - 1)) {
                    yield entry.document;
                }
            }
            return;
        }

        const idx = index || this.index;
        if (from > until) {
            const entries = idx.range(until, from);
            if (!entries) return;
            for (let i = entries.length - 1; i >= 0; i--) {
                yield this.readFrom(entries[i].partition, entries[i].position, entries[i].size);
            }
            return;
        }

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
        this.indexMatcher.add(name, this.secondaryIndexes[name].matcher);

        index.open();
        return index;
    }

    /**
     * Remove a secondary index from the write path and the matcher lookup table.
     *
     * @api
     * @param {string} name The secondary index name to remove.
     */
    removeSecondaryIndex(name) {
        const entry = this.secondaryIndexes[name];
        if (entry) {
            this.indexMatcher.remove(name);
            delete this.secondaryIndexes[name];
        }
    }

    /**
     * Build the standard document result entry from a merge stream state.
     * @private
     */
    buildDocumentEntry(stream) {
        return {
            document: this.serializer.deserialize(stream.data.toString('utf8')),
            sequenceNumber: stream.headerOut.sequenceNumber,
            partitionName: stream.partitionName,
            position: stream.headerOut.position,
            size: stream.headerOut.dataSize,
            partition: stream.partition,
        };
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
            stream => items.push(this.buildDocumentEntry(stream))
        );

        yield* items;
    }

    /**
     * Iterate documents across all partitions in descending sequenceNumber order using a k-way merge.
     * Uses readAllBackwards on each partition.
     *
     * @protected
     * @param {number} [from=Number.MAX_SAFE_INTEGER] The 0-based sequenceNumber upper bound (inclusive).
     * @param {number} [until=0] The 0-based sequenceNumber lower bound (inclusive).
     * @returns {Generator<{document: object, sequenceNumber: number, partitionName: string, position: number, size: number, partition: number}>}
     */
    *iterateDocumentsNoIndexBackwards(from = Number.MAX_SAFE_INTEGER, until = 0) {
        const streams = [];

        this.forEachPartition(partition => {
            if (!partition.isOpen()) {
                partition.open();
            }

            const last = partition.readLast();
            if (!last || last.header.sequenceNumber < until) {
                return;
            }

            let startBefore;
            if (last.header.sequenceNumber <= from) {
                startBefore = partition.size;
            } else {
                const found = partition.findDocument(from + 1);
                if (!found) return;
                startBefore = found.headerOut.position;
            }

            const headerOut = {};
            const reader = partition.readAllBackwards(startBefore, headerOut);
            const first = reader.next();
            if (first.done || headerOut.sequenceNumber > from || headerOut.sequenceNumber < until) {
                return;
            }

            streams.push({ data: first.value, reader, headerOut, partition: partition.id, partitionName: partition.name });
        });

        const items = [];
        kWayMerge(
            streams,
            stream => stream.headerOut.sequenceNumber,
            stream => {
                const next = stream.reader.next();
                if (!next.done && stream.headerOut.sequenceNumber >= until) {
                    stream.data = next.value;
                    return true;
                }
                return false;
            },
            stream => items.push(this.buildDocumentEntry(stream)),
            false  // descending: pick the stream with the highest sequenceNumber
        );

        yield* items;
    }

    /**
     * Read a raw document Buffer from the given partition and position, firing the preRead hook.
     *
     * @protected
     * @param {number} partitionId
     * @param {number} position
     * @param {number} [size]
     * @returns {Buffer|false}
     */
    readBufferFrom(partitionId, position, size) {
        const partition = this.getPartition(partitionId);
        if (this.listenerCount('preRead') > 0) {
            this.emit('preRead', position, partition.metadata);
        }
        return partition.readFrom(position, size);
    }

    /**
     * Iterate raw document Buffers for the given index range.
     * Newline separators (for NDJSON) are the caller's responsibility.
     * Does not support the no-index (`false`) path.
     *
     * @protected
     * @param {number} from 1-based index position (inclusive).
     * @param {number} until 1-based index position (inclusive). May be less than `from` for reverse order.
     * @param {ReadableIndex|null} index
     * @returns {Generator<Buffer>}
     */
    *iterateRangeBuffers(from, until, index) {
        const idx = index || this.index;
        const ascending = from <= until;
        const lo = ascending ? from : until;
        const hi = ascending ? until : from;
        const entries = idx.range(lo, hi);
        if (!entries) return;
        if (ascending) {
            for (const entry of entries) {
                const buffer = this.readBufferFrom(entry.partition, entry.position, entry.size);
                if (buffer !== false) yield buffer;
            }
        } else {
            for (let i = entries.length - 1; i >= 0; i--) {
                const buffer = this.readBufferFrom(entries[i].partition, entries[i].position, entries[i].size);
                if (buffer !== false) yield buffer;
            }
        }
    }

    /**
     * Read a range of raw document Buffers from the index, with NDJSON newlines appended to each.
     * Yields one Buffer per document containing the serialized JSON followed by `\n`.
     * Deserialization is skipped; the on-disk bytes are forwarded as-is.
     *
     * @api
     * @param {number} from The 1-based document number (inclusive) to start reading from.
     * @param {number} [until] The 1-based document number (inclusive) to read until. Defaults to index.length.
     * @param {ReadableIndex|null} [index] The index to use. Defaults to the primary index.
     * @returns {Generator<Buffer>}
     */
    *readRangeBuffers(from, until = -1, index = null) {
        const lengthSource = index || this.index;
        if (!lengthSource.isOpen()) {
            lengthSource.open();
        }
        const readFrom = wrapAndCheck(from, lengthSource.length);
        const readUntil = wrapAndCheck(until, lengthSource.length);
        assert(readFrom > 0 && readUntil > 0, `Range scan error for range ${from} - ${until}.`);
        for (const buffer of this.iterateRangeBuffers(readFrom, readUntil, index)) {
            yield Buffer.concat([buffer, NDJSON_NEWLINE]);
        }
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
     * When `matchDocument` is provided, `this.indexMatcher.forEachMatch()` is used to
     * efficiently find only the matching indexes via the discriminant lookup table.
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

        if (!matchDocument) {
            // No document filter: iterate all secondary indexes unconditionally.
            for (const indexName of Object.keys(this.secondaryIndexes)) {
                iterationHandler(this.secondaryIndexes[indexName].index, indexName);
            }
            return;
        }

        this.indexMatcher.forEachMatch(matchDocument, indexName => {
            iterationHandler(this.secondaryIndexes[indexName].index, indexName);
        });
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

        this.partitions.forEach(iterationHandler);
    }

}

export default ReadableStorage;
export { matches };
