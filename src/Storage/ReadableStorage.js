const fs = require('fs');
const path = require('path');
const EventEmitter = require('events');
const Partition = require('../Partition');
const Index = require('../Index');
const { createHmac, matches, buildMetadataForMatcher } = require('../util');

const DEFAULT_READ_BUFFER_SIZE = 4 * 1024;

/**
 * An append-only storage with highly performant positional range scans.
 * It's highly optimized for an event-store and hence does not support compaction or data-rewrite, nor any querying
 */
class ReadableStorage extends EventEmitter {

    /**
     * @param {string} [storageName] The name of the storage.
     * @param {Object} [config] An object with storage parameters.
     * @param {Object} [config.serializer] A serializer object with methods serialize(document) and deserialize(data).
     * @param {function(Object): string} config.serializer.serialize Default is JSON.stringify.
     * @param {function(string): Object} config.serializer.deserialize Default is JSON.parse.
     * @param {string} [config.dataDirectory] The path where the storage data should reside. Default '.'.
     * @param {string} [config.indexDirectory] The path where the indexes should be stored. Defaults to dataDirectory.
     * @param {string} [config.indexFile] The name of the primary index. Default '{storageName}.index'.
     * @param {number} [config.readBufferSize] Size of the read buffer in bytes. Default 4096.
     * @param {Object} [config.indexOptions] An options object that should be passed to all indexes on construction.
     * @param {string} [config.hmacSecret] A private key that is used to verify matchers retrieved from indexes.
     * @param {Object} [config.metadata] A metadata object to be stored in all partitions belonging to this storage.
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

        this.initializeIndexes(config);
        this.scanPartitions(config);
    }

    /**
     * @protected
     * @param {string} name
     * @param {object} [options]
     * @returns {{ index: ReadableIndex, matcher?: Object|function }}
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
     * @param {Object} config The configuration object
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
     * @param {Object} config The configuration object containing options for the partitions.
     * @returns void
     */
    scanPartitions(config) {
        const defaults = {
            readBufferSize: DEFAULT_READ_BUFFER_SIZE
        };
        this.partitionConfig = Object.assign(defaults, config);
        this.partitions = {};

        const files = fs.readdirSync(this.dataDirectory);
        for (let file of files) {
            if (file.substr(-6) === '.index') continue;
            if (file.substr(-7) === '.branch') continue;
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
        /* istanbul ignore next  */
        if (!this.partitions[partitionIdentifier]) {
            throw new Error(`Partition #${partitionIdentifier} does not exist.`);
        }

        this.partitions[partitionIdentifier].open();
        return this.partitions[partitionIdentifier];
    }

    /**
     * @protected
     * @param {number} partitionId The partition to read from.
     * @param {number} position The file position to read from.
     * @param {number} [size] The expected byte size of the document at the given position.
     * @returns {Object} The document stored at the given position.
     * @throws {Error} if the document at the given position can not be deserialized.
     */
    readFrom(partitionId, position, size) {
        const partition = this.getPartition(partitionId);
        const data = partition.readFrom(position);
        return this.serializer.deserialize(data);
    }

    /**
     * Read a single document from the given position, in the full index or in the provided index.
     *
     * @api
     * @param {number} number The 1-based document number (inside the given index) to read.
     * @param {ReadableIndex} [index] The index to use for finding the document position.
     * @returns {Object} The document at the given position inside the index.
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
     * @param {ReadableIndex} [index] The index to use for finding the documents in the range.
     * @returns {Generator} A generator that will read each document in the range one by one.
     */
    *readRange(from, until, index) {
        index = index || this.index;

        if (!index.isOpen()) {
            index.open();
        }

        const entries = index.range(from, until);
        if (entries === false) {
            throw new Error(`Range scan error for range ${from} - ${until}.`);
        }
        for (let entry of entries) {
            const document = this.readFrom(entry.partition, entry.position, entry.size);
            yield document;
        }
    }

    /**
     * Open an existing index.
     *
     * @api
     * @param {string} name The index name.
     * @param {Object|function} [matcher] The matcher object or function that the index needs to have been defined with. If not given it will not be validated.
     * @returns {ReadableIndex}
     * @throws {Error} if the index with that name does not exist.
     * @throws {Error} if the HMAC for the matcher does not match.
     */
    openIndex(name, matcher) {
        if (name in this.secondaryIndexes) {
            return this.secondaryIndexes[name].index;
        }

        const indexName = this.storageFile + '.' + name + '.index';
        if (!fs.existsSync(path.join(this.indexDirectory, indexName))) {
            throw new Error(`Index "${name}" does not exist.`);
        }
        const metadata = buildMetadataForMatcher(matcher, this.hmac);
        let { index } = this.secondaryIndexes[name] = this.createIndex(indexName, Object.assign({}, this.indexOptions, { metadata }));

        index.open();
        return index;
    }

    /**
     * Helper method to iterate over all documents.
     *
     * @protected
     * @param {function(Object, EntryInterface)} iterationHandler
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
     * @param {Object} [matchDocument] If supplied, only indexes the document matches on will be iterated.
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
