import ReadableStorage from './ReadableStorage.js';
import ReadablePartition from '../Partition/ReadablePartition.js';
import Watcher from '../Watcher.js';
import { scanForFilesSync } from '../utils/fsUtil.js';

/**
 * An append-only storage with highly performant positional range scans.
 * It's highly optimized for an event-store and hence does not support compaction or data-rewrite, nor any querying
 */
class ReadOnlyStorage extends ReadableStorage {

    /**
     * @inheritdoc
     */
    constructor(storageName = 'storage', config = {}) {
        super(storageName, config);
        this.storageFilesFilter = this.storageFilesFilter.bind(this);
        this.onStorageFileChanged = this.onStorageFileChanged.bind(this);
    }

    /**
     * Returns true if the given filename belongs to this storage.
     * @param {string} filename
     * @returns {boolean}
     */
    storageFilesFilter(filename) {
        return !filename.endsWith('.branch') && filename.substring(0, this.storageFile.length) === this.storageFile;
    }

    /**
     * Open the storage and indexes and create read and write buffers eagerly.
     * Will emit an 'opened' event if finished.
     *
     * @api
     * @param {function(): void} [callback] Called after indexes open, before `'opened'` is emitted.
     *   Can be used as a synchronous alternative to listening to the `'opened'` event.
     * @returns {boolean}
     */
    open(callback) {
        if (!this.watcher) {
            this.watcher = new Watcher([this.dataDirectory, this.indexDirectory], this.storageFilesFilter);
            this.watcher.on('rename', this.onStorageFileChanged);
        }
        return super.open(callback);
    }

    /**
     * @private
     * @param {string} filename
     */
    onStorageFileChanged(filename) {
        if (filename.endsWith('.index')) {
            const indexName = filename.substring(this.storageFile.length + 1, filename.length - 6);
            // New indexes are not automatically opened in the reader
            this.emit('index-created', indexName);
            return;
        }

        this.registerPartitionFile(filename);
    }

    /**
     * Register a partition by its relative file name if it is not already known.
     * Shared by the file-watch path and the index-append path so both stay consistent.
     *
     * @private
     * @param {string} filename
     * @returns {number} The id of the (now registered) partition.
     */
    registerPartitionFile(filename) {
        const partitionId = ReadablePartition.idFor(filename);
        if (!this.partitions.has(partitionId)) {
            const partition = this.createPartition(filename, this.partitionConfig);
            this.partitions.add(partition.id, partition);
            this.emit('partition-created', partition.id);
        }
        return partitionId;
    }

    /**
     * Ensure the partition referenced by an appended index entry is registered.
     *
     * The index file and the partition file are watched independently, so an `'append'` can be
     * observed before the corresponding partition-creation event has been dispatched. The writer
     * always flushes the partition before appending to the index, so the file already exists on
     * disk; a one-off synchronous scan registers it on demand and closes the race.
     *
     * @private
     * @param {number} partitionId
     * @returns {boolean} True if the partition is registered after this call.
     */
    ensurePartitionRegistered(partitionId) {
        if (this.partitions.has(partitionId)) {
            return true;
        }
        const escaped = this.storageFile.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const partitionPattern = new RegExp(`^(${escaped}.*)$`);
        scanForFilesSync(this.dataDirectory, partitionPattern, (file) => {
            if (file.endsWith('.index') || file.endsWith('.branch') || file.endsWith('.lock')) return;
            this.registerPartitionFile(file);
        });
        return this.partitions.has(partitionId);
    }

    /**
     * Close the storage and frees up all resources.
     * Will emit a 'closed' event when finished.
     *
     * @api
     * @returns void
     */
    close() {
        if (this.watcher) {
            this.watcher.close();
            this.watcher = null;
        }
        super.close();
    }

    /**
     * @protected
     * @param {string} name
     * @param {object} [options]
     * @returns {{ index: ReadableIndex, matcher?: object|function }}
     */
    createIndex(name, options = {}) {
        const { index } = super.createIndex(name, options);
        const indexShortName = name.replace(this.storageFile + '.', '').replace('.index', '');
        index.on('append', (prevLength, newLength) => {
            if (!this.watcher) {
                // If the watcher has been removed, this means this storage was closed and we don't want to handle events any more
                return;
            }
            const entries = index.range(prevLength + 1, newLength);
            /* c8 ignore next 3 */
            if (entries === false) {
                return;
            }
            this.processAppendedEntries(index, indexShortName, entries);
        });
        index.on('truncate', (prevLength, newLength) => {
            if (index === this.index) {
                this.emit('truncate', prevLength, newLength);
            }
        });
        return { index };
    }

    /**
     * Emit `'wrote'` / `'index-add'` for each appended index entry.
     *
     * If an entry references a partition that has not been registered yet (the partition-creation
     * watch event is still pending), the remaining entries are re-processed on the next tick instead
     * of throwing a hard `Partition #… does not exist.` error out of the synchronous emit path.
     *
     * @private
     * @param {ReadableIndex} index
     * @param {string} indexShortName
     * @param {IndexEntry[]} entries
     * @param {number} [startIndex] The entry offset to resume from on a retry.
     */
    processAppendedEntries(index, indexShortName, entries, startIndex = 0) {
        for (let i = startIndex; i < entries.length; i++) {
            const entry = entries[i];
            if (!this.ensurePartitionRegistered(entry.partition)) {
                this.scheduleAppendRetry(index, indexShortName, entries, i);
                return;
            }
            this.emitAppendedEntry(index, indexShortName, entry);
        }
    }

    /**
     * Re-process appended entries from the given offset on the next tick.
     * @private
     */
    scheduleAppendRetry(index, indexShortName, entries, startIndex) {
        setTimeout(() => {
            if (!this.watcher) {
                return;
            }
            this.processAppendedEntries(index, indexShortName, entries, startIndex);
        }, 1);
    }

    /**
     * Read a single appended entry and emit the corresponding event.
     * A read failure is surfaced as an `'error'` event (when observed) rather than crashing the reader.
     * @private
     */
    emitAppendedEntry(index, indexShortName, entry) {
        let document;
        try {
            document = this.readFrom(entry.partition, entry.position, entry.size);
        } catch (error) {
            /* c8 ignore next 3 */
            if (this.listenerCount('error') > 0) {
                this.emit('error', error);
            }
            return;
        }
        if (index === this.index) {
            this.emit('wrote', document, entry, entry.position);
        } else {
            this.emit('index-add', indexShortName, entry.number, document);
        }
    }
}

export default ReadOnlyStorage;
