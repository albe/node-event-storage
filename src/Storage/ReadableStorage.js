const fs = require('fs');
const path = require('path');
const events = require('events');
const Partition = require('../Partition');
const Index = require('../Index');
const { assert, createHmac, matches, wrapAndCheck, buildMetadataForMatcher } = require('../util');

const DEFAULT_READ_BUFFER_SIZE = 4 * 1024;

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
            metadata: {}
        };
        config = Object.assign(defaults, config);
        this.serializer = config.serializer;

        this.hmac = createHmac(config.hmacSecret);

        this.dataDirectory = path.resolve(config.dataDirectory);

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
        const index = new Index.ReadOnly(name, options);
        return { index };
    }

    /**
     * @protected
     * @param {string} name
     * @param {object} [options]
     * @returns {ReadablePartition}
     */
    createPartition(name, options = {}) {
        return new Partition.ReadOnly(name, options);
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

        this.partitions[partitionIdentifier].open();
        return this.partitions[partitionIdentifier];
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
            for (const entry of this.iteratePartitionsBySequenceNumber(from - 1, until - 1)) {
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
     * Iterate documents across all partitions in sequenceNumber order using a k-way merge.
     * SequenceNumbers stored in document headers are 0-based.
     * Each yielded entry includes the deserialized document, its sequenceNumber, the partition name,
     * the byte position within the partition, the data size, and the partition id —
     * allowing callers to rebuild index entries.
     * @api
     * @param {number} fromSeq The 0-based sequenceNumber to start from (inclusive).
     * @param {number} untilSeq The 0-based sequenceNumber to read until (inclusive).
     * @returns {Generator<{document: object, sequenceNumber: number, partitionName: string, position: number, size: number, partitionId: number}>}
     */
    *iteratePartitionsBySequenceNumber(fromSeq, untilSeq) {
        const partitions = [];

        for (const partition of Object.values(this.partitions)) {
            if (!partition.isOpen()) {
                partition.open();
            }
            const headerOut = {};
            const reader = partition.readAll(0, headerOut);

            // Advance to the first document with sequenceNumber >= fromSeq.
            let result = reader.next();
            while (!result.done && headerOut.sequenceNumber < fromSeq) {
                result = reader.next();
            }

            if (!result.done && headerOut.sequenceNumber <= untilSeq) {
                partitions.push({ reader, headerOut, data: result.value, sequenceNumber: headerOut.sequenceNumber, position: headerOut.position, size: headerOut.dataSize, partitionId: partition.id, partitionName: partition.name });
            }
        }

        // K-way merge: at each step, yield the document with the smallest sequenceNumber
        while (partitions.length > 0) {
            let minIdx = 0;
            for (let i = 1; i < partitions.length; i++) {
                if (partitions[i].sequenceNumber < partitions[minIdx].sequenceNumber) {
                    minIdx = i;
                }
            }

            const { data, sequenceNumber, partitionName, position, size, partitionId } = partitions[minIdx];
            yield { document: this.serializer.deserialize(data), sequenceNumber, partitionName, position, size, partitionId };

            const next = partitions[minIdx].reader.next();
            if (!next.done && partitions[minIdx].headerOut.sequenceNumber <= untilSeq) {
                partitions[minIdx].data = next.value;
                partitions[minIdx].sequenceNumber = partitions[minIdx].headerOut.sequenceNumber;
                partitions[minIdx].position = partitions[minIdx].headerOut.position;
                partitions[minIdx].size = partitions[minIdx].headerOut.dataSize;
            } else {
                partitions.splice(minIdx, 1);
            }
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

        index.open();
        return index;
    }

    /**
     * Iterate documents across all partitions in sequenceNumber order using a k-way merge,
     * invoking a callback for each entry. Opens any closed partition automatically.
     *
     * @api
     * @param {number} from The 0-based sequenceNumber to start from (inclusive).
     * @param {number} until The 0-based sequenceNumber to read until (inclusive).
     * @param {function({document: object, sequenceNumber: number, partitionName: string, position: number, size: number, partitionId: number}): void} callback
     */
    forEachDocumentNoIndex(from, until, callback) {
        for (const entry of this.iteratePartitionsBySequenceNumber(from, until)) {
            callback(entry);
        }
    }

    /**
     * Helper method to iterate over all documents.
     *
     * @protected
     * @param {function(object, EntryInterface)} iterationHandler
     */
    forEachDocument(iterationHandler) {
        /* istanbul ignore if  */
        if (typeof iterationHandler !== 'function') {
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
     * @protected
     * @param {function(ReadableIndex, string)} iterationHandler
     * @param {object} [matchDocument] If supplied, only indexes the document matches on will be iterated.
     */
    forEachSecondaryIndex(iterationHandler, matchDocument) {
        /* istanbul ignore if  */
        if (typeof iterationHandler !== 'function') {
            return;
        }

        for (let indexName of Object.keys(this.secondaryIndexes)) {
            if (!matchDocument || matches(matchDocument, this.secondaryIndexes[indexName].matcher)) {
                iterationHandler(this.secondaryIndexes[indexName].index, indexName);
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

module.exports = ReadableStorage;
module.exports.matches = matches;
module.exports.CorruptFileError = Partition.CorruptFileError;
