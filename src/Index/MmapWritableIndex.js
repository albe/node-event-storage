import fs from 'fs';
import { assertEqual } from '../util.js';
import { buildMetadataHeader } from '../metadataUtil.js';
import { ensureDirectory } from '../fsUtil.js';
import MmapReadableIndex from './MmapReadableIndex.js';
import MmapReadOnlyIndex from './MmapReadOnlyIndex.js';
import { loadMmapModule, getMmapPackageName } from './MmapModule.js';

const HEADER_MAGIC = 'nesidx01';

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
class MmapWritableIndex extends MmapReadableIndex {

    /**
     * @protected
     * @param {object} options
     */
    initialize(options) {
        super.initialize(options);
        ensureDirectory(options.dataDirectory);

        this.fileMode = 'a+';
        this.flushCallbacks = [];
        this.flushDelay = (options.flushDelay === undefined ? 100 : options.flushDelay) >>> 0;
        this.syncOnFlush = !!options.syncOnFlush;
        this.flushTimeout = null;
        this.hasUnflushedWrites = false;
        this.lastNumber = 0;
    }

    /**
     * Check if the index file is still intact.
     *
     * @protected
     * @returns {number} The amount of entries in the file. Returns 0 for an empty writable file after writing metadata.
     * @throws {Error} If the file is corrupt or can not be read correctly.
     */
    checkFile() {
        const stat = fs.fstatSync(this.fd);
        if (stat.size === 0) {
            this.writeMetadata();
            return 0;
        }

        const dataBytes = stat.size - this.readMetadata();
        if (dataBytes < 0) {
            throw new Error('Invalid index file!');
        }

        const length = Math.floor(dataBytes / this.EntryClass.size);
        const validBytes = length * this.EntryClass.size;
        if (validBytes !== dataBytes) {
            fs.ftruncateSync(this.fd, this.headerSize + validBytes);
        }

        return length;
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
        if (this.fd) {
            return false;
        }

        this.flushCallbacks = [];
        this.hasUnflushedWrites = false;

        const opened = super.open();
        this.mapEntries(this.bytesForEntries(this.lengthValue), true);
        this.lastNumber = this.lengthValue > 0 ? this.read(this.lengthValue).number : 0;

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
            this.metadata = { entryClass: this.EntryClass.name, entrySize: this.EntryClass.size };
        }
        const metadataBuffer = buildMetadataHeader(HEADER_MAGIC, this.metadata);
        fs.writeSync(this.fd, metadataBuffer, 0, metadataBuffer.byteLength, 0);
        this.headerSize = metadataBuffer.byteLength;
    }

    /**
     * Ensure the writable mapping can hold the given entry count, growing by write-buffer chunk size as needed.
     *
     * @private
     * @param {number} length
     */
    mapEntriesForLength(length) {
        const requiredMapSize = this.bytesForEntries(length);
        if (requiredMapSize <= this.mappedSize && this.mapBuffer) {
            return;
        }

        const grownMapSize = Math.max(requiredMapSize, this.mappedSize + this.mapGrowthBytes);
        if (grownMapSize > this.mappedSize) {
            const stat = fs.fstatSync(this.fd);
            if (stat.size < grownMapSize) {
                fs.ftruncateSync(this.fd, grownMapSize);
            }
        }
        this.mapEntries(grownMapSize, true);
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
        assertEqual(entry.constructor.name, this.EntryClass.name, 'Wrong entry object.');
        assertEqual(entry.constructor.size, this.EntryClass.size, 'Invalid entry size.');

        if (this.lastNumber >= entry.number) {
            throw new Error(`Consistency error. Tried to add entry ${entry.number} but last entry is ${this.lastNumber}.`);
        }

        const nextLength = this.lengthValue + 1;
        const previousLength = this.lengthValue;
        this.mapEntriesForLength(nextLength);

        const offset = this.headerSize + (nextLength - 1) * this.EntryClass.size;
        entry.toBuffer(this.mapBuffer, offset);

        this.lengthValue = nextLength;
        this.data[previousLength] = entry;
        if (this.readUntil === previousLength - 1) {
            this.readUntil++;
        }
        this.lastNumber = entry.number;
        this.hasUnflushedWrites = true;

        if (!this.flushTimeout) {
            this.flushTimeout = setTimeout(() => this.flush(), this.flushDelay);
        }

        this.onFlush(callback, nextLength);
        return this.length;
    }

    /**
     * Flush the write buffer to disk if it is not empty.
     *
     * @returns {boolean} If a flush actually was executed.
     */
    flush() {
        if (!this.fd) {
            return false;
        }
        if (this.flushTimeout) {
            clearTimeout(this.flushTimeout);
            this.flushTimeout = null;
        }
        if (!this.hasUnflushedWrites) {
            return false;
        }

        this.mmap.sync(this.mapBuffer);
        if (this.syncOnFlush) {
            fs.fsyncSync(this.fd);
        }

        this.hasUnflushedWrites = false;
        const callbacks = this.flushCallbacks;
        this.flushCallbacks = [];
        // [callback, position, callback, position, ...]
        for (let i = 0; i < callbacks.length; i += 2) callbacks[i](callbacks[i + 1]);

        return true;
    }

    /**
     * Truncate the index after the given entry number.
     *
     * @param {number} after The index entry number to truncate after.
     */
    truncate(after) {
        if (!this.fd) {
            return;
        }
        if (after < 0) {
            after = 0;
        }

        // Ensure pending writes are visible before computing truncate boundaries.
        this.flush();

        const truncatePosition = this.bytesForEntries(after);
        const stat = fs.statSync(this.fileName);
        if (truncatePosition >= stat.size) {
            return;
        }

        fs.ftruncateSync(this.fd, truncatePosition);
        this.lengthValue = after;
        this.data.length = after;
        if (this.readUntil >= after) {
            this.readUntil = after - 1;
        }
        this.lastNumber = after > 0 ? this.read(after).number : 0;
        this.mapEntries(truncatePosition, true);
    }

    /**
     * Close the index and release the file handle.
     * @api
     */
    close() {
        if (this.fd) {
            this.flush();
            fs.ftruncateSync(this.fd, this.bytesForEntries(this.lengthValue));
            fs.fdatasyncSync(this.fd);
        }
        super.close();
    }

    /**
     * This destroys the index and deletes it from disk.
     * @api
     */
    destroy() {
        this.close();
        fs.unlinkSync(this.fileName);
    }
}

MmapWritableIndex.ReadOnly = MmapReadOnlyIndex;

export default MmapWritableIndex;
export { MmapReadableIndex, MmapReadOnlyIndex, loadMmapModule, getMmapPackageName };
