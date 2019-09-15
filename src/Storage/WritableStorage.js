const fs = require('fs');
const path = require('path');
const WritablePartition = require('../Partition/WritablePartition');
const WritableIndex = require('../Index/WritableIndex');
const ReadableStorage = require('./ReadableStorage');
const { matches, buildMetadataForMatcher } = require('../util');

const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;

/**
 * An append-only storage with highly performant positional range scans.
 * It's highly optimized for an event-store and hence does not support compaction or data-rewrite, nor any querying
 */
class WritableStorage extends ReadableStorage {

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
        if (typeof storageName !== 'string') {
            config = storageName;
            storageName = undefined;
        }
        const defaults = {
            partitioner: (document, number) => '',
            writeBufferSize: DEFAULT_WRITE_BUFFER_SIZE,
            maxWriteBufferDocuments: 0,
            syncOnFlush: false,
            dirtyReads: true
        };
        config = Object.assign(defaults, config);
        config.indexOptions = Object.assign({ syncOnFlush: config.syncOnFlush }, config.indexOptions);
        super(storageName, config);

        this.partitioner = config.partitioner;
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

        const entry = new WritableIndex.Entry(this.index.length + 1, position, size, partitionId);
        this.index.add(entry, (indexPosition) => {
            this.emit('wrote', document, entry, indexPosition);
            /* istanbul ignore if  */
            if (typeof callback === 'function') return callback(indexPosition);
        });
        return entry;
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
     * Ensure that an index with the given name and document matcher exists.
     * Will create the index if it doesn't exist, otherwise return the existing index.
     *
     * @api
     * @param {string} name The index name.
     * @param {Object|function} [matcher] An object that describes the document properties that need to match to add it this index or a function that receives a document and returns true if the document should be indexed.
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

        if (!matcher) {
            throw new Error('Need to specify a matcher.');
        }

        const metadata = buildMetadataForMatcher(matcher, this.hmac);
        const newIndex = this.createIndex(indexName, Object.assign({}, this.indexOptions, { metadata }));
        try {
            this.forEachDocument((document, indexEntry) => {
                if (matches(document, matcher)) {
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
     * Flush all write buffers to disk.
     * This is a sync method and will invoke all previously registered flush callbacks.
     *
     * @api
     * @returns {boolean} Returns true if a flush on any partition or the main index was executed.
     */
    flush() {
        let result = this.index.flush();
        this.forEachPartition(partition => result = result || partition.flush());
        this.forEachSecondaryIndex(index => index.flush());
        return result;
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
     * @returns {ReadableIndex}
     */
    createIndex(name, options = {}) {
        return new WritableIndex(name, options);
    }

    /**
     * @protected
     * @param args
     * @returns {ReadablePartition}
     */
    createPartition(...args) {
        return new WritablePartition(...args);
    }

}

module.exports = WritableStorage;
