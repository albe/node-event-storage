import fs from 'fs';
import path from 'path';
import WritablePartition from '../Partition/WritablePartition.js';
import WritableIndex, { Entry as WritableIndexEntry } from '../Index/WritableIndex.js';
import ReadableStorage from './ReadableStorage.js';
import { assert, ensureDirectory } from '../util.js';
import { matches, buildMetadataForMatcher, buildMatcherFromMetadata } from '../metadataUtil.js';

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
     * Helper method to iterate over all writable secondary indexes.
     * Opens each index before calling the callback (passing the previous open status),
     * and closes it afterwards if it was not already open.
     *
     * @protected
     * @param {function(WritableIndex, string, boolean)} iterationHandler Called with (index, name, wasOpen).
     * @param {object} [matchDocument] If supplied, only indexes the document matches on will be iterated.
     */
    forEachWritableSecondaryIndex(iterationHandler, matchDocument) {
        this.forEachSecondaryIndex((index, name) => {
            /* istanbul ignore if */
            if (!(index instanceof WritableIndex)) return;
            const wasOpen = index.isOpen();
            if (!wasOpen) index.open();
            iterationHandler(index, name, wasOpen);
            if (!wasOpen) index.close();
        }, matchDocument);
    }

    /**
     * Scan every partition's last document to detect torn writes inline.
     * A document is torn when its expected end exceeds the actual file size:
     *   position + documentWriteSize(dataSize) > partition.size
     *
     * @private
     * @returns {{ lastValidSequenceNumber: number, maxPartitionSequenceNumber: number }}
     */
    findTornWriteBoundary() {
        let lastValidSequenceNumber = Number.MAX_SAFE_INTEGER;
        let maxPartitionSequenceNumber = -1;
        this.forEachPartition(partition => {
            partition.open();
            const last = partition.readLast();
            /* istanbul ignore if */
            if (!last) return;
            const { header: { sequenceNumber, dataSize }, position } = last;
            if (position + partition.documentWriteSize(dataSize) > partition.size) {
                // Torn write: the document extends beyond the end of the file.
                lastValidSequenceNumber = Math.min(lastValidSequenceNumber, sequenceNumber);
            } else {
                maxPartitionSequenceNumber = Math.max(maxPartitionSequenceNumber, sequenceNumber);
            }
        });
        return { lastValidSequenceNumber, maxPartitionSequenceNumber };
    }

    /**
     * Check all partitions for torn writes, physically repair each partition, truncate all indexes
     * to the torn-write boundary, and then reindex to rebuild any missing index entries.
     *
     * A document is torn when the partition file ends before the document's expected end position
     * (i.e. position + documentWriteSize(dataSize) > partition.size).  Detected inline in
     * findTornWriteBoundary(), without any checkTornWrite() call.
     *
     * Repair flow:
     * 1. findTornWriteBoundary() reads the last document of every partition and finds the global
     *    torn-write boundary (minimum torn sequence number across all partitions).
     * 2. If torn writes were found, truncateAfterSequence() removes all documents at or beyond
     *    the boundary from each partition.
     * 3. Truncate all indexes to the torn-write boundary, then reindex to fill any lagging entries.
     * 4. If no torn writes were found but the index is lagging, reindex directly.
     */
    checkTornWrites() {
        const { lastValidSequenceNumber, maxPartitionSequenceNumber } = this.findTornWriteBoundary();

        if (lastValidSequenceNumber < Number.MAX_SAFE_INTEGER) {
            // Phase 2: remove all documents at or beyond the torn-write boundary from each partition.
            this.forEachPartition(partition => {
                partition.open();
                partition.truncateAfterSequence(lastValidSequenceNumber - 1);
            });

            // Truncate all indexes to the torn-write boundary.
            this.index.open();
            this.index.truncate(lastValidSequenceNumber);
            /* istanbul ignore next */
            this.forEachWritableSecondaryIndex(index => {
                index.truncate(index.find(lastValidSequenceNumber));
            });

            // Reindex to fill in any missing complete-document entries.
            this.reindex(this.index.length);
        } else if (maxPartitionSequenceNumber >= 0 && maxPartitionSequenceNumber + 1 > this.index.length) {
            // No torn writes, but the index is lagging — repair it.
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
        this.index.truncate(fromSequenceNumber);

        // Truncate all loaded secondary indexes to match the new primary length.
        this.forEachWritableSecondaryIndex(index => {
            // find(0) returns 0, so truncate(0) will remove all entries when fromSequenceNumber===0
            index.truncate(fromSequenceNumber === 0 ? 0 : index.find(fromSequenceNumber));
        });

        // Scan partitions in sequence-number order and rebuild index entries.
        // iterateDocumentsNoIndex opens any closed partitions automatically.
        for (const { document, partition, position, size } of this.iterateDocumentsNoIndex(fromSequenceNumber, Number.MAX_SAFE_INTEGER)) {
            const newEntry = new WritableIndexEntry(this.index.length + 1, position, size, partition);
            this.index.add(newEntry);

            this.forEachWritableSecondaryIndex((secIndex) => {
                secIndex.add(newEntry);
            }, document);
        }

        this.flush();
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

        const entry = new WritableIndexEntry(this.index.length + 1, position, size, partitionId);
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
     * @param {boolean} [reindex=true] Whether to scan existing documents and populate the new index. Set to false when it is known that no existing documents can match the matcher.
     * @returns {ReadableIndex} The index containing all documents that match the query.
     * @throws {Error} if the index doesn't exist yet and no matcher was specified.
     */
    ensureIndex(name, matcher, reindex = true) {
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
        if (reindex) {
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
     * Assumes the primary index is fully consistent with the partition data. Looks up the first
     * index entry after `after` for each affected partition and truncates the partition file
     * at that entry's byte position.
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
        this.forEachWritableSecondaryIndex(index => {
            index.truncate(index.find(after));
        });
    }

    /**
     * @inheritDoc
     * Open an existing secondary index and repair any stale entries beyond the current primary
     * index length. Stale entries can be present when checkTornWrites() truncated the primary
     * index before this secondary index was loaded into memory.
     */
    openIndex(name, matcher) {
        const index = super.openIndex(name, matcher);
        const lastEntry = index.lastEntry;
        if (lastEntry !== false && lastEntry.number > this.index.length) {
            // Secondary index is ahead of primary: truncate stale entries.
            index.truncate(index.find(this.index.length));
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

export default WritableStorage;
export { StorageLockedError, LOCK_THROW, LOCK_RECLAIM };
