const fs = require('fs');
const crypto = require('crypto');
const mkdirpSync = require('mkdirp').sync;
const path = require('path');
const EventEmitter = require('events');
const Partition = require('./Partition');
const Index = require('./Index');

const DEFAULT_READ_BUFFER_SIZE = 4 * 1024;
const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;

/**
 * An append-only storage with highly performant positional range scans.
 * It's highly optimized for an event-store and hence does not support compaction or data-rewrite, nor any querying
 */
class Storage extends EventEmitter {

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
     * @param {number} [config.writeBufferSize] Size of the write buffer in bytes. Default 16384.
     * @param {number} [config.maxWriteBufferDocuments] How many documents to have in the write buffer at max. 0 means as much as possible. Default 0.
     * @param {boolean} [config.syncOnFlush] If fsync should be called on write buffer flush. Set this if you need strict durability. Defaults to false.
     * @param {boolean} [config.dirtyReads] If dirty reads should be allowed. This means that writes that are in write buffer but not yet flushed can be read. Defaults to true.
     * @param {function(Object, number): string} [config.partitioner] A function that takes a document and sequence number and returns a partition name that the document should be stored in. Defaults to write all documents to the primary partition.
     * @param {Object} [config.indexOptions] An options object that should be passed to all indexes on construction.
     * @param {string} [config.hmacSecret] A private key that is used to verify matchers retrieved from indexes.
     */
    constructor(storageName = 'storage', config = {}) {
        super();
        if (typeof storageName === 'object') {
            config = storageName;
            storageName = undefined;
        }

        this.storageFile = storageName || 'storage';
        let defaults = {
            serializer: { serialize: JSON.stringify, deserialize: JSON.parse },
            partitioner: (document, number) => '',
            dataDirectory: '.',
            indexFile: this.storageFile + '.index',
            indexOptions: {},
            hmacSecret: ''
        };
        config = Object.assign(defaults, config);
        this.serializer = config.serializer;
        this.partitioner = config.partitioner;

        this.hmac = string => {
            const hmac = crypto.createHmac('sha256', config.hmacSecret);
            hmac.update(string);
            return hmac.digest('hex');
        };

        this.dataDirectory = path.resolve(config.dataDirectory);
        if (!fs.existsSync(this.dataDirectory)) {
            mkdirpSync(this.dataDirectory);
        }

        this.indexDirectory = path.resolve(config.indexDirectory || this.dataDirectory);

        this.indexOptions = config.indexOptions;
        this.indexOptions.dataDirectory = this.indexDirectory;
        // Safety precaution to prevent accidentially restricting main index
        delete this.indexOptions.matcher;
        this.index = new Index(config.indexFile, this.indexOptions);
        this.secondaryIndexes = {};

        this.scanPartitions(config);
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
        let defaults = {
            readBufferSize: DEFAULT_READ_BUFFER_SIZE,
            writeBufferSize: DEFAULT_WRITE_BUFFER_SIZE,
            maxWriteBufferDocuments: 0,
            syncOnFlush: false,
            dirtyReads: true
        };
        this.partitionConfig = Object.assign(defaults, config);
        this.partitions = {};

        const files = fs.readdirSync(this.dataDirectory);
        for (let file of files) {
            if (file.substr(-6) === '.index') continue;
            if (file.substr(-7) === '.branch') continue;
            if (file.substr(0, this.storageFile.length) === this.storageFile) {
                //console.log('Found existing partition', file);
                const partition = new Partition(file, this.partitionConfig);
                this.partitions[partition.id] = partition;
            }
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
     * Add an index entry for the given document at the position and size.
     *
     * @private
     * @param {number} partitionId The partition where the document is stored.
     * @param {number} position The file offset where the document is stored.
     * @param {number} size The size of the stored document.
     * @param {Object} document The document to add to the index.
     * @param {function} [callback] The callback to call when the index is written to disk.
     * @returns {Index.Entry} The index entry item.
     */
    addIndex(partitionId, position, size, document, callback) {
        if (!this.index.isOpen()) {
            this.index.open();
        }

        /*if (this.index.lastEntry.position + this.index.lastEntry.size !== position) {
         this.emit('index-corrupted');
         throw new Error('Corrupted index, needs to be rebuilt!');
         }*/

        const entry = new Index.Entry(this.index.length + 1, position, size, partitionId);
        this.index.add(entry, (indexPosition) => {
            this.emit('wrote', document, entry, indexPosition);
            /* istanbul ignore if  */
            if (typeof callback === 'function') return callback(indexPosition);
        });
        return entry;
    }

    /**
     * Get a partition either by name or by id.
     * If a partition with the given name does not exist, a new one will be created.
     * If a partition with the given id does not exist, an error is thrown.
     *
     * @private
     * @param {string|number} partitionIdentifier The partition name or the partition Id
     * @returns {Partition}
     * @throws {Error} If an id is given and no such partition exists.
     */
    getPartition(partitionIdentifier) {
        if (typeof partitionIdentifier === 'string') {
            const partitionName = this.storageFile + (partitionIdentifier.length ? '.' + partitionIdentifier : '');
            partitionIdentifier = Partition.id(partitionName);
            if (!this.partitions[partitionIdentifier]) {
                this.partitions[partitionIdentifier] = new Partition(partitionName, this.partitionConfig);
            }
        } else /* istanbul ignore next  */ if (!this.partitions[partitionIdentifier]) {
            throw new Error(`Partition #${partitionIdentifier} does not exist.`);
        }

        this.partitions[partitionIdentifier].open();
        return this.partitions[partitionIdentifier];
    }

    /**
     * @api
     * @param {Object} document The document to write to storage.
     * @param {function} [callback] A function that will be called when the document is written to disk.
     * @returns {number} The 1-based document sequence number in the storage.
     */
    write(document, callback) {
        const data = this.serializer.serialize(document).toString();
        const dataSize = Buffer.byteLength(data, 'utf8');

        const partitionName = this.partitioner(document, this.index.length + 1);
        const partition = this.getPartition(partitionName);
        const position = partition.write(data, callback);

        /* istanbul ignore next  */
        if (position === false) {
            throw new Error('Error writing document.');
        }
        const indexEntry = this.addIndex(partition.id, position, dataSize, document);
        this.forEachSecondaryIndex((index, name) => {
            if (!index.isOpen()) {
                index.open();
            }
            index.add(indexEntry);
            this.emit('index-add', name, index.length, document);
        }, document);

        return this.index.length;
    }

    /**
     * @private
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
     * @param {Index} [index] The index to use for finding the document position.
     * @returns {Object} The document at the given position inside the index.
     */
    read(number, index) {
        index = index || this.index;

        if (!index.isOpen()) {
            index.open();
        }

        const entry = index.get(number);
        if (entry === false) return false;

        return this.readFrom(entry.partition, entry.position, entry.size);
    }

    /**
     * Read a range of documents from the given position range, in the full index or in the provided index.
     * Returns a generator in order to reduce memory usage and be able to read lots of documents with little latency.
     *
     * @api
     * @param {number} from The 1-based document number (inclusive) to start reading from.
     * @param {number} [until] The 1-based document number (inclusive) to read until. Defaults to index.length.
     * @param {Index} [index] The index to use for finding the documents in the range.
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
     * @private
     * @param {Object} document The document to check against the matcher.
     * @param {Object|function} matcher An object of properties and their values that need to match in the object.
     * @returns {boolean} True if the document matches the matcher or false otherwise.
     */
    matches(document, matcher) {
        if (typeof document === 'undefined') return false;
        if (typeof matcher === 'undefined') return true;

        if (typeof matcher === 'function') return matcher(document);
        for (let prop of Object.getOwnPropertyNames(matcher)) {
            if (typeof matcher[prop] === 'object') {
                if (!this.matches(document[prop], matcher[prop])) {
                    return false;
                }
            } else {
                if (document[prop] !== matcher[prop]) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * @private
     * @param {Object|function} matcher The matcher object or function that the index needs to have been defined with. If not given it will not be validated.
     * @returns {{metadata: {matcher: string, hmac: string}}}
     */
    buildMetadataForMatcher(matcher) {
        if (typeof matcher === 'object') {
            return { metadata: { matcher } };
        }
        const matcherString = matcher.toString();
        return { metadata: { matcher: matcherString, hmac: this.hmac(matcherString) } };
    }

    /**
     * Open an existing index.
     *
     * @api
     * @param {string} name The index name.
     * @param {Object|function} [matcher] The matcher object or function that the index needs to have been defined with. If not given it will not be validated.
     * @returns {Index}
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
        let metadata;
        if (matcher) {
            metadata = this.buildMetadataForMatcher(matcher);
        }

        const index = new Index(indexName, Object.assign({}, this.indexOptions, metadata));
        if (typeof index.metadata.matcher === 'object') {
            matcher = index.metadata.matcher;
        } else {
            if (index.metadata.hmac !== this.hmac(index.metadata.matcher)) {
                index.destroy();
                throw new Error('Invalid HMAC for matcher.');
            }
            matcher = eval('(' + index.metadata.matcher + ')').bind({}); // jshint ignore:line
        }
        this.secondaryIndexes[name] = { index, matcher };
        index.open();
        return index;
    }

    /**
     * Ensure that an index with the given name and document matcher exists.
     * Will create the index if it doesn't exist, otherwise return the existing index.
     *
     * @api
     * @param {string} name The index name.
     * @param {Object|function} [matcher] An object that describes the document properties that need to match to add it this index or a function that receives a document and returns true if the document should be indexed.
     * @returns {Index} The index containing all documents that match the query.
     * @throws {Error} if the index doesn't exist yet and no matcher was specified.
     */
    ensureIndex(name, matcher) {
        if (name in this.secondaryIndexes) {
            return this.secondaryIndexes[name].index;
        }

        const indexName = this.storageFile + '.' + name + '.index';
        if (fs.existsSync(path.join(this.indexDirectory, indexName))) {
            return this.openIndex(name, matcher);
        }

        if (!matcher) {
            throw new Error('Need to specify a matcher.');
        }

        const metadata = this.buildMetadataForMatcher(matcher);
        const newIndex = new Index(indexName, Object.assign({}, this.indexOptions, metadata));
        try {
            this.forEachDocument((document, indexEntry) => {
                if (this.matches(document, matcher)) {
                    newIndex.add(indexEntry);
                }
            });
        } catch (e) {
            newIndex.destroy();
            throw e;
        }

        this.secondaryIndexes[name] = { index: newIndex, matcher };
        this.emit('index-created', name);
        return newIndex;
    }

    /**
     * Truncate the storage after the given sequence number.
     *
     * @param {number} after The document sequence number to truncate after.
     */
    truncate(after) {
        /*
         To truncate the store following steps need to be done:

         1) find all partition positions after which their files should be truncated
         2) truncate all partitions accordingly
         3) truncate/rewrite all indexes
         */
        if (!this.index.isOpen()) {
            this.index.open();
        }
        const entries = this.index.range(after + 1);  // We need the first entry that is cut off
        if (entries === false || entries.length === 0) {
            return;
        }

        if (after === 0) {
            this.forEachPartition(partition => partition.truncate(0));
        } else {
            const partitions = [];
            const numPartitions = Object.keys(this.partitions).length;
            for (let entry of entries) {
                if (partitions.indexOf(entry.partition) >= 0) continue;
                partitions.push(entry.partition);
                this.getPartition(entry.partition).truncate(entry.position);

                if (partitions.length === numPartitions) {
                    break;
                }
            }
        }

        this.index.truncate(after);
        this.forEachSecondaryIndex(index => {
            let closeIndex = false;
            if (!index.isOpen()) {
                index.open();
                closeIndex = true;
            }
            index.truncate(index.find(after));
            if (closeIndex) {
                index.close();
            }
        });
    }

    /**
     * Helper method to iterate over all documents.
     *
     * @private
     * @param {function(Object, Index.Entry)} iterationHandler
     */
    forEachDocument(iterationHandler) {
        /* istanbul ignore if  */
        if (typeof iterationHandler !== 'function') return;

        const entries = this.index.all();

        for (let entry of entries) {
            const document = this.readFrom(entry.partition, entry.position, entry.size);
            iterationHandler(document, entry);
        }
    }

    /**
     * Helper method to iterate over all secondary indexes.
     *
     * @private
     * @param {function(Index)} iterationHandler
     * @param {Object} [matchDocument] If supplied, only indexes the document matches on will be iterated.
     */
    forEachSecondaryIndex(iterationHandler, matchDocument) {
        /* istanbul ignore if  */
        if (typeof iterationHandler !== 'function') return;

        for (let indexName of Object.keys(this.secondaryIndexes)) {
            if (!matchDocument || this.matches(matchDocument, this.secondaryIndexes[indexName].matcher)) {
                iterationHandler(this.secondaryIndexes[indexName].index, indexName);
            }
        }
    }

    /**
     * Helper method to iterate over all partitions.
     *
     * @private
     * @param {function(Partition)} iterationHandler
     */
    forEachPartition(iterationHandler) {
        /* istanbul ignore if  */
        if (typeof iterationHandler !== 'function') return;

        for (let partition of Object.keys(this.partitions)) {
            iterationHandler(this.partitions[partition]);
        }
    }

}

module.exports = Storage;