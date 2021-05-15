const fs = require('fs');
const mkdirpSync = require('mkdirp').sync;
const path = require('path');
const WritablePartition = require('../Partition/WritablePartition');
const WritableIndex = require('../Index/WritableIndex');
const ReadableStorage = require('./ReadableStorage');
const { assert, matches, buildMetadataForMatcher, buildMatcherFromMetadata } = require('../util');

const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;

class StorageLockedError extends Error {}

/**
 * @typedef {object|function(object):boolean} Matcher
 */

/**
 * An append-only storage with highly performant positional range scans.
 * It's highly optimized for an event-store and hence does not support compaction or data-rewrite, nor any querying
 */
class WritableStorage extends ReadableStorage {

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
     * @param {number} [config.writeBufferSize] Size of the write buffer in bytes. Default 16384.
     * @param {number} [config.maxWriteBufferDocuments] How many documents to have in the write buffer at max. 0 means as much as possible. Default 0.
     * @param {boolean} [config.syncOnFlush] If fsync should be called on write buffer flush. Set this if you need strict durability. Defaults to false.
     * @param {boolean} [config.dirtyReads] If dirty reads should be allowed. This means that writes that are in write buffer but not yet flushed can be read. Defaults to true.
     * @param {function(object, number): string} [config.partitioner] A function that takes a document and sequence number and returns a partition name that the document should be stored in. Defaults to write all documents to the primary partition.
     * @param {object} [config.indexOptions] An options object that should be passed to all indexes on construction.
     * @param {string} [config.hmacSecret] A private key that is used to verify matchers retrieved from indexes.
     */
    constructor(storageName = 'storage', config = {}) {
        if (typeof storageName !== 'string') {
            config = storageName;
            storageName = undefined;
        }
        const defaults = {
            partitioner: (document, number) => '',
            writeBufferSize: DEFAULT_WRITE_BUFFER_SIZE,
            maxWriteBufferDocuments: 0,
            syncOnFlush: false,
            dirtyReads: true,
            dataDirectory: '.'
        };
        config = Object.assign(defaults, config);
        config.indexOptions = Object.assign({ syncOnFlush: config.syncOnFlush }, config.indexOptions);
        if (!fs.existsSync(config.dataDirectory)) {
            try {
                mkdirpSync(config.dataDirectory);
            } catch (e) {
            }
        }
        super(storageName, config);
        this.partitioner = config.partitioner;
    }

    /**
     * @inheritDoc
     * @returns {boolean}
     * @throws {StorageLockedError} If this storage is locked by another process.
     */
    open() {
        if (!this.lock()) {
            return true;
        }
        return super.open();
    }

    /**
     * Check all partitions torn writes and truncate the storage to the position before the first torn write.
     * This might delete correctly written events in partitions, if their sequence number is higher than the
     * torn write in another partition.
     */
    checkTornWrites() {
        let lastValidSequenceNumber = Number.MAX_SAFE_INTEGER;
        this.forEachPartition(partition => {
            partition.open();
            const tornSequenceNumber = partition.checkTornWrite();
            if (tornSequenceNumber >= 0) {
                lastValidSequenceNumber = Math.min(lastValidSequenceNumber, tornSequenceNumber);
            }
        });
        if (lastValidSequenceNumber < Number.MAX_SAFE_INTEGER) {
            this.truncate(lastValidSequenceNumber);
        }
        this.forEachPartition(partition => partition.close());
    }

    /**
     * Attempt to lock this storage by means of a lock directory.
     * @returns {boolean} True if the lock was created or false if the lock is already in place.
     * @throws {StorageLockedError} If this storage is already locked by another process.
     * @throws {Error} If the lock could not be created.
     */
    lock() {
        if (this.locked) {
            return false;
        }
        this.lockFile = path.resolve(this.dataDirectory, this.storageFile + '.lock');
        try {
            fs.mkdirSync(this.lockFile);
            this.locked = true;
        } catch (e) {
            /* istanbul ignore if */
            if (e.code !== 'EEXIST') {
                throw new Error(`Error creating lock for storage ${this.storageFile}: ` + e.message);
            }
            throw new StorageLockedError(`Storage ${this.storageFile} is locked by another process`);
        }
        return true;
    }

    /**
     * Unlock this storage, no matter if it was previously locked by this writer.
     * Only use this if you are sure there is no other process still having a writer open.
     * Current implementation just deletes a lock file that is named like the storage.
     */
    unlock() {
        if (!this.locked && fs.existsSync(this.lockFile)) {
            this.checkTornWrites();
        }
        fs.rmdirSync(this.lockFile);
        this.locked = false;
    }

    /**
     * @inheritDoc
     */
    close() {
        if (this.locked) {
            this.unlock();
        }
        super.close();
    }

    /**
     * Add an index entry for the given document at the position and size.
     *
     * @private
     * @param {number} partitionId The partition where the document is stored.
     * @param {number} position The file offset where the document is stored.
     * @param {number} size The size of the stored document.
     * @param {object} document The document to add to the index.
     * @param {function} [callback] The callback to call when the index is written to disk.
     * @returns {EntryInterface} The index entry item.
     */
    addIndex(partitionId, position, size, document, callback) {
        if (!this.index.isOpen()) {
            this.index.open();
        }

        /*if (this.index.lastEntry.position + this.index.lastEntry.size !== position) {
         this.emit('index-corrupted');
         throw new Error('Corrupted index, needs to be rebuilt!');
         }*/

        const entry = new WritableIndex.Entry(this.index.length + 1, position, size, partitionId);
        this.index.add(entry, (indexPosition) => {
            this.emit('wrote', document, entry, indexPosition);
            /* istanbul ignore if  */
            if (typeof callback === 'function') {
                return callback(indexPosition);
            }
        });
        return entry;
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
        if (typeof partitionIdentifier === 'string') {
            const partitionName = this.storageFile + (partitionIdentifier.length ? '.' + partitionIdentifier : '');
            partitionIdentifier = WritablePartition.idFor(partitionName);
            if (!this.partitions[partitionIdentifier]) {
                this.partitions[partitionIdentifier] = this.createPartition(partitionName, this.partitionConfig);
                this.emit('partition-created', partitionIdentifier);
            }
            this.partitions[partitionIdentifier].open();
            return this.partitions[partitionIdentifier];
        }
        return super.getPartition(partitionIdentifier);
    }

    /**
     * @api
     * @param {object} document The document to write to storage.
     * @param {function} [callback] A function that will be called when the document is written to disk.
     * @returns {number} The 1-based document sequence number in the storage.
     */
    write(document, callback) {
        const data = this.serializer.serialize(document).toString();
        const dataSize = Buffer.byteLength(data, 'utf8');

        const partitionName = this.partitioner(document, this.index.length + 1);
        const partition = this.getPartition(partitionName);
        const position = partition.write(data, this.length, callback);

        assert(position !== false, 'Error writing document.');

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
     * Ensure that an index with the given name and document matcher exists.
     * Will create the index if it doesn't exist, otherwise return the existing index.
     *
     * @api
     * @param {string} name The index name.
     * @param {Matcher} [matcher] An object that describes the document properties that need to match to add it this index or a function that receives a document and returns true if the document should be indexed.
     * @returns {ReadableIndex} The index containing all documents that match the query.
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

        assert((typeof matcher === 'object' || typeof matcher === 'function') && matcher !== null, 'Need to specify a matcher.');

        const metadata = buildMetadataForMatcher(matcher, this.hmac);
        const { index } = this.createIndex(indexName, Object.assign({}, this.indexOptions, { metadata }));
        try {
            this.forEachDocument((document, indexEntry) => {
                if (matches(document, matcher)) {
                    index.add(indexEntry);
                }
            });
        } catch (e) {
            index.destroy();
            throw e;
        }

        this.secondaryIndexes[name] = { index, matcher };
        this.emit('index-created', name);
        return index;
    }

    /**
     * Flush all write buffers to disk.
     * This is a sync method and will invoke all previously registered flush callbacks.
     *
     * @api
     * @returns {boolean} Returns true if a flush on any partition or the main index was executed.
     */
    flush() {
        let result = this.index.flush();
        this.forEachPartition(partition => result = result | partition.flush());
        this.forEachSecondaryIndex(index => index.flush());
        return result;
    }

    /**
     * Iterate all distinct partitions in which the given iterable list of entries are stored.
     * @param {Iterable<Index.Entry>} entries
     * @param {function(Index.Entry)} iterationHandler
     */
    forEachDistinctPartitionOf(entries, iterationHandler) {
        const partitions = [];
        const numPartitions = Object.keys(this.partitions).length;
        for (let entry of entries) {
            if (partitions.indexOf(entry.partition) >= 0) {
                continue;
            }
            partitions.push(entry.partition);
            iterationHandler(entry);
            if (partitions.length === numPartitions) {
                break;
            }
        }
    }

    /**
     * Truncate all partitions after the given (global) sequence number.
     *
     * @private
     * @param {number} after The document sequence number to truncate after.
     */
    truncatePartitions(after) {
        if (after === 0) {
            this.forEachPartition(partition => partition.truncate(0));
            return;
        }

        const entries = this.index.range(after + 1);  // We need the first entry that is cut off
        if (entries === false || entries.length === 0) {
            return;
        }

        this.forEachDistinctPartitionOf(entries, entry => this.getPartition(entry.partition).truncate(entry.position));
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

        this.truncatePartitions(after);

        this.index.truncate(after);
        this.forEachSecondaryIndex(index => {
            if (!(index instanceof WritableIndex)) {
                return;
            }
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
     * @protected
     * @param {string} name
     * @param {object} [options]
     * @returns {{ index: WritableIndex, matcher: Matcher }}
     */
    createIndex(name, options = {}) {
        const index = new WritableIndex(name, options);
        let matcher;

        // If the index contains a matcher (possibly a serialized function) we check HMAC
        // to prevent evaluating unknown code.
        if (index.metadata.matcher) {
            try {
                matcher = buildMatcherFromMetadata(index.metadata, this.hmac);
            } catch (e) {
                index.destroy();
                throw e;
            }
        }

        return { index, matcher };
    }

    /**
     * @protected
     * @param {string} name
     * @param {object} [config]
     * @returns {WritablePartition}
     */
    createPartition(name, config = {}) {
        return new WritablePartition(name, config);
    }

}

module.exports = WritableStorage;
module.exports.StorageLockedError = StorageLockedError;