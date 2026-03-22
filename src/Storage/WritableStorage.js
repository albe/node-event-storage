const fs = require('fs');
const path = require('path');
const WritablePartition = require('../Partition/WritablePartition');
const WritableIndex = require('../Index/WritableIndex');
const ReadableStorage = require('./ReadableStorage');
const { assert, matches, buildMetadataForMatcher, buildMatcherFromMetadata, ensureDirectory } = require('../util');

const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;

const LOCK_RECLAIM = 0x1;
const LOCK_THROW = 0x2;

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
     * @param {number} [config.lock] One of LOCK_* constants that defines how an existing lock should be handled.
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
        ensureDirectory(config.dataDirectory);
        super(storageName, config);

        this.lockFile = path.resolve(this.dataDirectory, this.storageFile + '.lock');
        if (config.lock === LOCK_RECLAIM) {
            this.unlock();
        }
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
        const result = super.open();
        this.emit('ready');
        return result;
    }

    /**
     * Check all partitions torn writes and truncate the storage to the position before the first torn write.
     * This might delete correctly written events in partitions, if their sequence number is higher than the
     * torn write in another partition.
     * Also detects when the primary index is lagging behind the actual partition data and automatically
     * repairs it by invoking reindex().
     */
    checkTornWrites() {
        let lastValidSequenceNumber = Number.MAX_SAFE_INTEGER;
        let maxPartitionSequenceNumber = -1;
        this.forEachPartition(partition => {
            partition.open();
            const result = partition.checkTornWrite();
            if (result < 0) {
                // Torn write: result encodes -(tornSeqnum + 1), so torn seqnum = -result - 1.
                const tornSeqnum = -result - 1;
                lastValidSequenceNumber = Math.min(lastValidSequenceNumber, tornSeqnum);
                // Any complete documents before the torn one contribute to the lagging check.
                // Their last seqnum is tornSeqnum - 1 (if > 0; otherwise no complete docs).
                if (tornSeqnum > 0) {
                    maxPartitionSequenceNumber = Math.max(maxPartitionSequenceNumber, tornSeqnum - 1);
                }
                // Physically remove the torn document from this partition so that subsequent
                // partition reads (e.g. in reindex()) don't encounter the corrupt data.
                const tornPosition = partition.findDocumentPositionBefore(partition.size);
                if (tornPosition !== false && tornPosition >= 0) {
                    partition.truncate(tornPosition);
                }
            } else if (result > 0) {
                // No torn write: result encodes (lastCompleteSeqnum + 1), so seqnum = result - 1.
                maxPartitionSequenceNumber = Math.max(maxPartitionSequenceNumber, result - 1);
            }
            // result === 0: empty partition, no action needed.
        });
        if (lastValidSequenceNumber < Number.MAX_SAFE_INTEGER) {
            this.truncate(lastValidSequenceNumber);
            // After truncation, account for documents beyond the truncation point being removed.
            // truncate(N) keeps index entries 1..N, so the last kept partition seqnum is N-1.
            maxPartitionSequenceNumber = Math.min(maxPartitionSequenceNumber, lastValidSequenceNumber - 1);
        }
        // Ensure index is open so its length can be checked accurately.
        if (!this.index.isOpen()) {
            this.index.open();
        }
        // A partition seqnum of N means the document was written when the index had N entries,
        // so the index should contain at least N+1 entries to be consistent.
        // Automatically repair a lagging index by reindexing from the current index length.
        if (maxPartitionSequenceNumber >= 0 && maxPartitionSequenceNumber + 1 > this.index.length) {
            this.reindex(this.index.length);
        }
        this.forEachPartition(partition => partition.close());
    }

    /**
     * Rebuild the primary index and all loaded secondary indexes starting from the given sequence
     * number by scanning the partition data directly.
     * This is the building block for both auto-repair (invoked automatically when the primary
     * index is found to be lagging in checkTornWrites()) and for user-driven re-indexing after
     * index corruption.
     *
     * @api
     * @param {number} [fromSequenceNumber=0] The number of primary index entries to keep intact.
     *   All index entries beyond this position will be removed and rebuilt from partition data.
     *   Defaults to 0, which rebuilds all indexes from scratch.
     */
    reindex(fromSequenceNumber = 0) {
        if (!this.index.isOpen()) {
            this.index.open();
        }

        this.index.truncate(fromSequenceNumber);

        // Truncate all loaded secondary indexes to match the new primary length.
        this.forEachSecondaryIndex(index => {
            /* istanbul ignore if */
            if (!(index instanceof WritableIndex)) {
                return;
            }
            if (!index.isOpen()) {
                index.open();
            }
            // find(0) returns 0, so truncate(0) will remove all entries when fromSequenceNumber===0
            index.truncate(fromSequenceNumber === 0 ? 0 : index.find(fromSequenceNumber));
        });

        // Ensure all partitions are open so iteratePartitionsBySequenceNumber can read them.
        this.forEachPartition(partition => {
            if (!partition.isOpen()) {
                partition.open();
            }
        });

        // Scan partitions in sequence-number order and rebuild index entries.
        for (const { document, partitionId, position, size } of
            this.iteratePartitionsBySequenceNumber(fromSequenceNumber, Number.MAX_SAFE_INTEGER)) {
            const newEntry = new WritableIndex.Entry(this.index.length + 1, position, size, partitionId);
            this.index.add(newEntry);

            this.forEachSecondaryIndex((secIndex, name) => {
                /* istanbul ignore if */
                if (!(secIndex instanceof WritableIndex)) {
                    return;
                }
                /* istanbul ignore if */
                if (!secIndex.isOpen()) {
                    secIndex.open();
                }
                const { matcher } = this.secondaryIndexes[name];
                if (matches(document, matcher)) {
                    secIndex.add(newEntry);
                }
            });
        }

        this.flush();
    }

    /**
     * @returns {boolean} True if the lock was created or false if the lock is already in place.
     * @throws {StorageLockedError} If this storage is already locked by another process.
     * @throws {Error} If the lock could not be created.
     */
    lock() {
        if (this.locked) {
            return false;
        }
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
        if (fs.existsSync(this.lockFile)) {
            if (!this.locked) {
                this.checkTornWrites();
            }
            fs.rmdirSync(this.lockFile);
        }
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
     * Register a handler that is called before a document is written to storage.
     * The handler receives the document and the partition metadata and may throw to abort the write.
     * Multiple handlers can be registered; all run on every write in registration order.
     * Equivalent to `storage.on('preCommit', hook)`.
     *
     * @api
     * @param {function(object, object): void} hook A function receiving (document, partitionMetadata).
     */
    preCommit(hook) {
        this.on('preCommit', hook);
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
            const partitionShortName = partitionIdentifier;
            const partitionName = this.storageFile + (partitionIdentifier.length ? '.' + partitionIdentifier : '');
            partitionIdentifier = WritablePartition.idFor(partitionName);
            if (!this.partitions[partitionIdentifier]) {
                const partitionConfig = typeof this.partitionConfig.metadata === 'function'
                    ? { ...this.partitionConfig, metadata: this.partitionConfig.metadata(partitionShortName) }
                    : this.partitionConfig;
                this.partitions[partitionIdentifier] = this.createPartition(partitionName, partitionConfig);
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
        if (this.listenerCount('preCommit') > 0) {
            this.emit('preCommit', document, partition.metadata);
        }
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
        if (name === '_all') {
            return this.index;
        }
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
        if (after < 0) {
            after += this.index.length;
        }

        this.truncatePartitions(after);

        this.index.truncate(after);
        this.forEachSecondaryIndex(index => {
            /* istanbul ignore if */
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
     * @inheritDoc
     * Open an existing secondary index and repair any inconsistency with the current primary
     * index length. Repairs both the case where the secondary is ahead of the primary (by
     * truncating stale entries) and the case where the secondary is behind (by scanning primary
     * index entries and adding any missing matches).
     */
    openIndex(name, matcher) {
        const index = super.openIndex(name, matcher);
        const lastEntry = index.lastEntry;
        if (lastEntry !== false && lastEntry.number > this.index.length) {
            // Secondary index is ahead of primary: truncate stale entries.
            index.truncate(index.find(this.index.length));
        } else {
            // Secondary index may be behind primary: rebuild any missing entries.
            const fromNumber = (lastEntry !== false ? lastEntry.number : 0) + 1;
            if (fromNumber <= this.index.length) {
                const { matcher: secMatcher } = this.secondaryIndexes[name];
                if (secMatcher) {
                    const entries = this.index.range(fromNumber, this.index.length);
                    if (entries !== false) {
                        for (const primaryEntry of entries) {
                            const document = this.readFrom(primaryEntry.partition, primaryEntry.position, primaryEntry.size);
                            if (matches(document, secMatcher)) {
                                index.add(primaryEntry);
                            }
                        }
                        index.flush();
                    }
                }
            }
        }
        return index;
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
module.exports.CorruptFileError = ReadableStorage.CorruptFileError;
module.exports.LOCK_THROW = LOCK_THROW;
module.exports.LOCK_RECLAIM = LOCK_RECLAIM;
