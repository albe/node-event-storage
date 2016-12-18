const fs = require('fs');
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
     * Config options:
     *   - serializer: A serializer object with methods serialize(document) and deserialize(data). Default is JSON.stringify/parse.
     *   - dataDirectory: The path where the storage data should reside. Default '.'.
     *   - indexDirectory: The path where the indexes should be stored. Defaults to dataDirectory.
     *   - indexFile: The name of the primary index. Default '{storageName}.index'.
     *   - readBufferSize: Size of the read buffer in bytes. Default 4096.
     *   - writeBufferSize: Size of the write buffer in bytes. Default 16384.
     *   - maxWriteBufferDocuments: How many documents to have in the write buffer at max. 0 means as much as possible. Default 0.
     *   - syncOnFlush: If fsync should be called on write buffer flush. Set this if you need strict durability. Defaults to false.
     *   - partitioner: A function that takes a document and sequence number and returns a partition name that the document should be stored in. Defaults to write all documents to the primary partition.
     *   - indexOptions: An options object that should be passed to all indexes on construction.
     *
     * @param {string} [storageName] The name of the storage.
     * @param {Object} [config] An object with storage parameters.
     */
    constructor(storageName = 'storage', config = {}) {
        super();
        if (typeof storageName === 'object') {
            config = storageName;
            storageName = undefined;
        }

        this.serializer = config.serializer || { serialize: JSON.stringify, deserialize: JSON.parse };
        this.storageFile = storageName || 'storage';

        this.dataDirectory = path.resolve(config.dataDirectory || '.');
        if (!fs.existsSync(this.dataDirectory)) {
            mkdirpSync(this.dataDirectory);
        }

        this.indexDirectory = config.indexDirectory || this.dataDirectory;

        this.indexOptions = config.indexOptions || {};
        this.indexOptions.dataDirectory = this.indexDirectory;
        let indexFile = config.indexFile || (this.storageFile + '.index');
        this.index = new Index(indexFile, this.indexOptions);
        this.secondaryIndexes = {};

        this.partitioner = config.partitioner || ((document, number) => '');
        this.partitionConfig = {
            dataDirectory: this.dataDirectory,
            readBufferSize: config.readBufferSize || DEFAULT_READ_BUFFER_SIZE,
            writeBufferSize: config.writeBufferSize || DEFAULT_WRITE_BUFFER_SIZE,
            maxWriteBufferDocuments: config.maxWriteBufferDocuments || 0,
            syncOnFlush: !!config.syncOnFlush
        };
        this.partitions = {};

        let files = fs.readdirSync(this.dataDirectory);
        for (let file of files) {
            if (file.substr(-6) === '.index') continue;
            if (file.substr(0, this.storageFile.length) === this.storageFile) {
                //console.log('Found existing partition', file);
                let partition = new Partition(file, this.partitionConfig);
                this.partitions[partition.id] = partition;
            }
        }
    }

    /**
     * The amount of documents in the storage.
     * @returns {number}
     */
    get length() {
        return this.index.length;
    }

    /**
     * Open the storage and indexes and create read and write buffers.
     * Will emit an 'opened' event if finished.
     *
     * @api
     * @returns {boolean}
     */
    open() {
        this.index.open();
        for (let indexName of Object.getOwnPropertyNames(this.secondaryIndexes)) {
            this.secondaryIndexes[indexName].index.open();
        }

        for (let partition of Object.getOwnPropertyNames(this.partitions)) {
            this.partitions[partition].open();
        }

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
        for (let indexName of Object.getOwnPropertyNames(this.secondaryIndexes)) {
            this.secondaryIndexes[indexName].index.close();
        }
        for (let partition of Object.getOwnPropertyNames(this.partitions)) {
            this.partitions[partition].close();
        }
        this.emit('closed');
    }

    /**
     * Flush the write buffer to disk.
     * This is a sync method and will invoke all previously registered flush callbacks.
     * Will emit a 'flush' event when done.
     *
     * @private
     * @returns {boolean}
     */
    flush() {
        for (let partition of Object.getOwnPropertyNames(this.partitions)) {
            this.partitions[partition].flush();
        }

        this.emit('flush');
        return true;
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

        /*if (this.index.lastEntry.position + this.index.lastEntry.size !== position) {
         this.emit('index-corrupted');
         throw new Error('Corrupted index, needs to be rebuilt!');
         }*/

        let entry = new Index.Entry(this.index.length + 1, position, size, partitionId);
        this.index.add(entry, (indexPosition) => {
            if (typeof callback === 'function') callback(indexPosition);
            this.emit('wrote', document, entry, indexPosition);
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
            let partitionName = this.storageFile + (partitionIdentifier.length ? '.' + partitionIdentifier : '');
            partitionIdentifier = Partition.id(partitionName);
            if (!this.partitions[partitionIdentifier]) {
                this.partitions[partitionIdentifier] = new Partition(partitionName, this.partitionConfig);
                this.partitions[partitionIdentifier].open();
            }
        } else if (!this.partitions[partitionIdentifier]) {
            throw new Error('Partition #' + partitionIdentifier + ' does not exist.');
        }
        return this.partitions[partitionIdentifier];
    }

    /**
     * @api
     * @param {Object} document The document to write to storage.
     * @param {function} [callback] A function that will be called when the document is written to disk.
     * @returns {Number} The 1-based document sequence number in the storage.
     */
    write(document, callback) {
        let data = this.serializer.serialize(document).toString();
        let dataSize = Buffer.byteLength(data, 'utf8');

        let partitionName = this.partitioner(document, this.index.length + 1);
        let partition = this.getPartition(partitionName);
        let position = partition.write(data, callback);

        if (position === false) {
            throw new Error('Error writing document.');
        }
        let indexEntry = this.addIndex(partition.id, position, dataSize, document);
        for (let indexName of Object.getOwnPropertyNames(this.secondaryIndexes)) {
            if (this.matches(document, this.secondaryIndexes[indexName].matcher)) {
                this.secondaryIndexes[indexName].index.add(indexEntry);
            }
        }

        return this.index.length;
    }

    /**
     * @private
     * @param {number} partitionId The partition to read from.
     * @param {number} position The file position to read from.
     * @param {number} [size] The expected byte size of the document at the given position.
     * @returns {Object} The document stored at the given position.
     * @throws {Error} if the storage entry at the given position is corrupted.
     * @throws {Error} if the document size at the given position does not match the provided size.
     * @throws {Error} if the document at the given position can not be deserialized.
     */
    readFrom(partitionId, position, size) {
        if (!this.partitions[partitionId]) {
            throw new Error('Partition #' + partitionId + ' does not exist.');
        }
        try {
            let data = this.partitions[partitionId].readFrom(position);
            return this.serializer.deserialize(data);
        } catch (e) {
            console.log('Error parsing document:', data, position);
            throw e;
        }
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

        let entry = index.get(number);
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

        let entries = index.range(from, until);
        if (entries === false) {
            throw new Error('Range scan error.');
        }
        for (let entry of entries) {
            let document = this.readFrom(entry.partition, entry.position, entry.size);
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
        if (typeof matcher === 'undefined') return true;
        if (typeof document === 'undefined') return false;
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
     * Ensure that an index with the given name and document matcher exists.
     * Will create the index if it doesn't exist, otherwise return the existing index.
     *
     * @api
     * @param {string} name The index name.
     * @param {Object|function} [matcher] An object that describes the document properties that need to match to add it this index or a function that receives a document and returns true if the document should be indexed.
     * @returns {Index} The index containing all documents that match the query.
     */
    ensureIndex(name, matcher) {
        /*if (!this.isopen) {
            throw new Error('Storage is not open yet.');
        }*/
        if (name in this.secondaryIndexes) {
            return this.secondaryIndexes[name].index;
        }

        let indexName = this.storageFile + '.' + name + '.index';
        if (fs.existsSync(path.join(this.indexDirectory, indexName))) {
            let metadata;
            if (matcher) {
                metadata = { metadata: { matcher: typeof matcher === 'object' ? JSON.stringify(matcher) : matcher.toString() } };
            }
            let index = new Index(this.EntryClass, indexName, Object.assign({}, this.indexOptions, metadata));
            if (typeof index.metadata.matcher === 'object') {
                matcher = index.metadata.matcher;
            } else {
                matcher = eval('(' + index.metadata.matcher + ')');
            }
            this.secondaryIndexes[name] = { index, matcher };
            index.open();
            return index;
        }

        let entries = this.index.all();
        if (entries === false) {
            throw new Error('Range scan error.');
        }
        if (!matcher) {
            throw new Error('Need to specify a matcher.');
        }
        let serializedMatcher = typeof matcher === 'object' ? JSON.stringify(matcher) : matcher.toString();
        let newIndex = new Index(this.EntryClass, indexName, Object.assign({}, this.indexOptions, { metadata: { matcher: serializedMatcher } }));
        for (let entry of entries) {
            let document = this.readFrom(entry.partition, entry.position);
            if (this.matches(document, matcher)) {
                newIndex.add(entry);
            }
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
        if (this.truncating <= after) {
            return;
        }
        this.truncating = after;
        /*
         To truncate the store following steps need to be done:

         1) switch to read-only mode
         2) find all partition positions after which their files should be truncated
         3) truncate all partitions accordingly
         4) truncate/rewrite all indexes
         5) switch back to write mode
         */
        let entries = this.index.range(after + 1);
        if (entries === false || entries.length === 0) {
            this.truncating = undefined;
            return;
        }
        let partitions = [];
        for (let entry of entries) {
            if (partitions.indexOf(entry.partition) >= 0) continue;
            partitions.push(entry.partition);
            this.getPartition(entry.partition).truncate(entry.position);
        }
        let entry = entries[0];
        this.index.truncate(after);
        for (let indexName of Object.getOwnPropertyNames(this.secondaryIndexes)) {
            let truncateAfter = this.secondaryIndexes[indexName].find(entry.position);
            this.secondaryIndexes[indexName].truncate(truncateAfter);
        }
        this.truncating = undefined;
    }

}

module.exports = Storage;