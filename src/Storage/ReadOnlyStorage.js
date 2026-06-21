import ReadableStorage from './ReadableStorage.js';
import Watcher from '../Watcher.js';

const DEFAULT_SCAN_DELAY_MS = 10;

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
        return !filename.endsWith('.branch') && (filename === this.storageFile || filename.substring(0, this.storageFile.length + 1) === this.storageFile + '.');
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
            // Emit change events without filename as a signal to resync
            this.watcher.on('change', (filename) => !filename && this.scheduleScan());
        }
        return super.open(callback);
    }

    /**
     * Schedule a fresh scan of the filesystem to get the ReadOnly instance in sync.
     * Scheduling will be throttled so multiple calls will not cause multiple scan operations.
     * Note though that the callback will not be deduplicated, so calling this repeatedly with the same callback will
     * lead to this callback being executed multiple times once the scan is finished.
     *
     * @param {function} [callback] If provided, will be added to the callbacks invoked after the next full scan.
     * @param {number} [time=SCAN_DELAY_MS] The delay in ms to schedule the scan for
     */
    scheduleScan(callback, time = DEFAULT_SCAN_DELAY_MS) {
        if (typeof callback === 'function') {
            this.onScanFinished = this.onScanFinished || [];
            this.onScanFinished.push(callback);
        }
        this.scanSchedule = this.scanSchedule || setTimeout(() =>
            this.scanFiles(() => {
                this.scanSchedule = null;
                const callbacks = this.onScanFinished.slice();
                this.onScanFinished = [];
                callbacks.forEach(callback => callback());
            }), typeof callback === 'number' ? callback : time);
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
            if (!this.partitions.has(entry.partition)) {
                this.scheduleScan(() => {
                    this.processAppendedEntries(index, indexShortName, entries, i);
                });
                return;
            }
            this.emitAppendedEntry(index, indexShortName, entry);
        }
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
            /* c8 ignore next  */
            this.emit('error', error);
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
