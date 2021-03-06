const fs = require('fs');
const ReadableIndex = require('./ReadableIndex');
const { assertEqual, buildMetadataHeader, ensureDirectory } = require('../util');

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
     * @param {number} [options.writeBufferSize] The number of bytes to use for the write buffer. Default 4096.
     * @param {number} [options.flushDelay] How many ms to delay the write buffer flush to optimize throughput. Default 100.
     * @param {object} [options.metadata] An object containing the metadata information for this index. Will be written on initial creation and checked on subsequent openings.
     */
    constructor(name = '.index', options = {}) {
        if (typeof name !== 'string') {
            options = name;
            name = '.index';
        }
        let defaults = {
            writeBufferSize: 4096,
            flushDelay: 100,
            syncOnFlush: false
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
        this.writeBuffer = Buffer.allocUnsafe(options.writeBufferSize >>> 0); // jshint ignore:line
        this.writeBufferCursor = 0;
        this.flushCallbacks = [];
        this.flushDelay = options.flushDelay >>> 0; // jshint ignore:line
        this.syncOnFlush = !!options.syncOnFlush;
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
            if (e instanceof ReadableIndex.CorruptedIndexError) {
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
        if (this.fd) {
            return false;
        }
        this.writeBufferCursor = 0;
        this.flushCallbacks = [];
        return super.open();
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
        const metadataBuffer = buildMetadataHeader(ReadableIndex.HEADER_MAGIC, this.metadata);
        fs.writeSync(this.fd, metadataBuffer, 0, metadataBuffer.byteLength, 0);
        this.headerSize = metadataBuffer.byteLength;
    }

    /**
     * Close the index and release the file handle.
     * @api
     */
    close() {
        if (this.fd) {
            this.flush();
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
        if (this.writeBufferCursor === 0) {
            return false;
        }

        fs.writeSync(this.fd, this.writeBuffer, 0, this.writeBufferCursor);
        if (this.syncOnFlush) {
            fs.fsyncSync(this.fd);
        }

        this.writeBufferCursor = 0;
        this.flushCallbacks.forEach(callback => callback());
        this.flushCallbacks = [];
        return true;
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
        this.flushCallbacks.push(() => callback(position));
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

        if (this.readUntil === this.data.length - 1) {
            this.readUntil++;
        }
        this.data[this.data.length] = entry;

        if (this.writeBufferCursor === 0) {
            this.flushTimeout = setTimeout(() => this.flush(), this.flushDelay);
        }

        this.writeBufferCursor += entry.toBuffer(this.writeBuffer, this.writeBufferCursor);
        this.onFlush(callback, this.length);
        if (this.writeBufferCursor >= this.writeBuffer.byteLength) {
            this.flush();
        }

        return this.length;
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
        this.flush();

        const stat = fs.statSync(this.fileName);
        const truncatePosition = this.headerSize + after * this.EntryClass.size;
        if (truncatePosition >= stat.size) {
            return;
        }
        fs.truncateSync(this.fileName, truncatePosition);
        this.data.splice(after);
        this.readUntil = Math.min(this.readUntil, after);
    }
}

module.exports = WritableIndex;
module.exports.Entry = ReadableIndex.Entry;
