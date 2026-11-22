import fs from 'fs';
import ReadableIndex, { Entry, CorruptedIndexError, HEADER_MAGIC, CHUNK_CAPACITY, createChunk } from './ReadableIndex.js';
import { assert, assertEqual } from '../utils/util.js';
import { buildMetadataHeader } from '../utils/metadataUtil.js';
import { ensureDirectory, syncFile } from '../utils/fsUtil.js';
import SyncScheduler from '../SyncScheduler.js';
import { normalizeNamedCtorArgs } from '../utils/apiHelpers.js';

/**
 * An index is a simple append-only file that stores an ordered list of entry elements pointing to the actual file position
 * where the matching document is found in the storage file.
 * It does not provide a key-value lookup and hence only allows random and range positional reads.
 * This is highly optimized for the usage as an index into an event store, where it's only necessary to query sequentially
 * within a version (sequence number) range inside a single stream.
 * It allows to store additional metadata about the index in the header on creation, which is verified to be unchanged on later
 * access.
 *
 * The index basically functions like a simplified LSM list.
 */
class WritableIndex extends ReadableIndex {

    /**
     * @param {string} [name] The name of the file to use for storing the index.
     * @param {object} [options] An object with additional index options.
     * @param {EntryInterface} [options.EntryClass] The entry class to use for index items. Must implement the EntryInterface methods.
     * @param {string} [options.dataDirectory] The directory to store the index file in. Default '.'.
     * @param {number} [options.writeBufferSize] The amount of bytes of unwritten entries after which they are flushed. Default 4096.
     * @param {number} [options.flushDelay] How many ms to delay the write buffer flush to optimize throughput. Default 100.
     * @param {object} [options.metadata] An object containing the metadata information for this index. Will be written on initial creation and checked on subsequent openings.
     * @param {SyncScheduler} [options.syncScheduler] Decides when a new index file is synced to disk. Its entries are only synced by a storage checkpoint. Defaults to never syncing.
     */
    constructor(name = '.index', options = {}) {
        ({ name, options } = normalizeNamedCtorArgs(name, options, '.index'));
        let defaults = {
            writeBufferSize: 4096,
            flushDelay: 100
        };
        options = Object.assign(defaults, options);
        super(name, options);
    }

    /**
     * @protected
     * @param {object} options
     */
    initialize(options) {
        super.initialize(options);
        ensureDirectory(options.dataDirectory);

        this.fileMode = 'a+';
        // New entries are only written into the in-memory entries. Those behind `flushedLength` are the write buffer.
        this.writeBufferSize = options.writeBufferSize >>> 0; // jshint ignore:line
        this.flushedLength = 0;
        this.flushCallbacks = [];
        this.flushDelay = options.flushDelay >>> 0; // jshint ignore:line
        this.syncScheduler = options.syncScheduler || new SyncScheduler();
        // If the file changed since the storage last synced it.
        this.modified = false;
    }

    /**
     * Check if the index file is still intact.
     *
     * @protected
     * @returns {number} The amount of entries in the file.
     * @throws {Error} If the file is corrupt or can not be read correctly.
     */
    checkFile() {
        try {
            const entries = super.checkFile();
            if (entries < 0) {
                // Freshly created index... write metadata initially.
                this.writeMetadata();
                return 0;
            }
            return entries;
        } catch (e) {
            if (e instanceof CorruptedIndexError) {
                this.truncate(e.size);
                return e.size;
            }
            throw e;
        }
    }

    /**
     * Open the index if it is not already open.
     * This will open a file handle and either write the metadata if the file is empty or read back the metadata and verify
     * it against the metadata provided in the constructor options.
     *
     * @api
     * @returns {boolean} True if the index was opened or false if it was already open.
     * @throws {Error} if the file can not be opened.
     */
    open() {
        if (this.opened) {
            return false;
        }
        this.flushCallbacks = [];
        const opened = super.open();
        this.flushedLength = this.length;
        return opened;
    }

    /**
     * Write the metadata to the file.
     *
     * @private
     * @returns void
     */
    writeMetadata() {
        if (!this.metadata) {
            this.metadata = {entryClass: this.EntryClass.name, entrySize: this.EntryClass.size};
        }
        const metadataBuffer = buildMetadataHeader(HEADER_MAGIC, this.metadata);
        fs.writeSync(this.getFileHandle(), metadataBuffer, 0, metadataBuffer.byteLength, 0);
        this.headerSize = metadataBuffer.byteLength;
        this.syncScheduler.created(this.fileName);
    }

    /**
     * Flush pending buffered writes before the pool closes the descriptor.
     */
    onBeforeClose() {
        if (this.flushCallbacks) {
            this.flush();
        }
    }

    /**
     * Write the buffered entries, sync them if the file changed, and close the index. The flush reopens the file
     * handle if the pool evicted it since the last write, so no buffered entry is lost.
     *
     * @api
     */
    close() {
        this.flush();
        this.sync();
        super.close();
    }

    /**
     * Sync the file if it changed since the last sync. While open, indexes are only synced by a storage checkpoint,
     * because the repair after a crash rebuilds their entries from the partitions.
     */
    sync() {
        if (this.modified) {
            syncFile(this.fileName);
            this.modified = false;
        }
    }

    /**
     * This destroys the index and deletes it from disk.
     * @api
     */
    destroy() {
        this.modified = false;
        this.close();
        fs.unlinkSync(this.fileName);
    }

    /**
     * Flush the write buffer to disk if it is not empty.
     *
     * @returns {boolean} If a flush actually was executed.
     */
    flush() {
        if (!this.isOpen()) {
            return false;
        }
        if (this.flushTimeout) {
            clearTimeout(this.flushTimeout);
            this.flushTimeout = null;
        }
        if (this.flushedLength === this.length) {
            return false;
        }

        this.writeUnflushedEntries(this.getFileHandle());
        this.modified = true;

        const callbacks = this.flushCallbacks;
        this.flushCallbacks = [];
        for (let i = 0; i < callbacks.length; i += 2) callbacks[i](callbacks[i + 1]);
        return true;
    }

    /**
     * Write the in-memory entries that are not in the file yet. They are already in the binary file format,
     * so this writes them directly from memory, one write per chunk they span.
     *
     * @private
     * @param {number} fd
     */
    writeUnflushedEntries(fd) {
        const entrySize = this.EntryClass.size;
        while (this.flushedLength < this.length) {
            const chunkIndex = Math.floor(this.flushedLength / CHUNK_CAPACITY);
            const from = this.flushedLength - chunkIndex * CHUNK_CAPACITY;
            const until = Math.min(CHUNK_CAPACITY, this.length - chunkIndex * CHUNK_CAPACITY);
            fs.writeSync(fd, this.chunks[chunkIndex].buffer, from * entrySize, (until - from) * entrySize);
            this.flushedLength += until - from;
        }
    }

    /**
     * Register a flush callback for the given index position.
     *
     * @private
     * @param {function} callback The callback function to execute on the next flush.
     * @param {number} position The index position that will be provided as parameter to the callback.
     */
    onFlush(callback, position) {
        if (typeof callback !== 'function') {
            return;
        }
        this.flushCallbacks.push(callback, position);
    }

    /**
     * Append a single entry to the end of this index.
     *
     * @api
     * @param {EntryInterface} entry The index entry to append.
     * @param {function} [callback] A callback function to execute when the index entry is flushed to disk.
     * @returns {number} The index position for the entry. It matches the index size after the insertion.
     */
    add(entry, callback) {
        assertEqual(entry.constructor.name, this.EntryClass.name, `Wrong entry object.`);
        assertEqual(entry.constructor.size, this.EntryClass.size, `Invalid entry size.`);

        const dataLen = this.length;
        assert(dataLen === 0 || this.numberAt(dataLen - 1) < entry.number, 'Consistency error. Tried to add an index that should come before existing last entry.');

        if (this.flushedLength === dataLen) {
            this.flushTimeout = setTimeout(() => this.flush(), this.flushDelay);
        }

        this.appendEntry(entry);
        this.onFlush(callback, this.length);
        if ((this.length - this.flushedLength) * this.EntryClass.size >= this.writeBufferSize) {
            this.flush();
        }

        return this.length;
    }

    /**
     * Write the entry into memory behind the last entry. It reaches the file with the next flush.
     *
     * @private
     * @param {EntryInterface} entry
     */
    appendEntry(entry) {
        const chunkIndex = Math.floor(this.length / CHUNK_CAPACITY);
        const position = this.length - chunkIndex * CHUNK_CAPACITY;
        let chunk = position > 0 ? this.chunkFor(this.length - 1) : createChunk(this.EntryClass.size, 1);
        assert(chunk, 'Can not append to the index, because its last entries could not be read.');
        if (chunk.capacity === position) {
            chunk = createChunk(this.EntryClass.size, position + 1, chunk);
        }
        this.chunks[chunkIndex] = chunk;
        entry.toBuffer(chunk.buffer, position * this.EntryClass.size);
        chunk.filled = position + 1;
        this.setLength(this.length + 1);
    }

    /**
     * Truncate the index after the given entry number.
     *
     * @param {number} after The index entry number to truncate after.
     */
    truncate(after) {
        if (!this.isOpen()) {
            return;
        }
        this.getFileHandle();
        if (after < 0) {
            after = 0;
        }
        this.flush();

        const stat = fs.statSync(this.fileName);
        const truncatePosition = this.headerSize + after * this.EntryClass.size;
        if (truncatePosition >= stat.size) {
            return;
        }
        fs.truncateSync(this.fileName, truncatePosition);
        this.modified = true;
        this.setLength(after);
        this.flushedLength = Math.min(this.flushedLength, after);
    }
}

export default WritableIndex;
export { Entry };
