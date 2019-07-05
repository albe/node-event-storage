const fs = require('fs');
const path = require('path');
const mkdirpSync = require('mkdirp').sync;
const Entry = require('./IndexEntry');
const { assertEqual, wrapAndCheck, binarySearch } = require('./util');

Buffer.poolSize = 64 * 1024;

// node-event-store-index V01
const HEADER_MAGIC = "nesidx01";

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
class Index {

    /**
     * @param {string} [name] The name of the file to use for storing the index.
     * @param {Object} [options] An object with additional index options.
     * @param {EntryInterface} [options.EntryClass] The entry class to use for index items. Must implement the EntryInterface methods.
     * @param {string} [options.dataDirectory] The directory to store the index file in. Default '.'.
     * @param {number} [options.writeBufferSize] The number of bytes to use for the write buffer. Default 4096.
     * @param {number} [options.flushDelay] How many ms to delay the write buffer flush to optimize throughput. Default 100.
     * @param {Object} [options.metadata] An object containing the metadata information for this index. Will be written on initial creation and checked on subsequent openings.
     */
    constructor(name = '.index', options = {}) {
        if (typeof name !== 'string') {
            options = name;
            name = '.index';
        }
        let defaults = {
            dataDirectory: '.',
            writeBufferSize: 4096,
            flushDelay: 100,
            EntryClass: Entry
        };
        options = Object.assign(defaults, options);
        const EntryClass = options.EntryClass;
        Entry.assertValidEntryClass(EntryClass);
        if (!fs.existsSync(options.dataDirectory)) {
            mkdirpSync(options.dataDirectory);
        }

        this.name = name;
        options.readBufferSize = EntryClass.size;
        this.initialize(options);

        this.EntryClass = EntryClass;
        if (options.metadata) {
            this.metadata = Object.assign({entryClass: EntryClass.name, entrySize: EntryClass.size}, options.metadata);
        }

        this.open();
    }

    /**
     * @private
     * @param {Object} options
     */
    initialize(options) {
        this.data = [];
        this.fd = null;
        this.fileName = path.resolve(options.dataDirectory, this.name);
        this.readBuffer = Buffer.allocUnsafe(options.readBufferSize >>> 0);
        this.writeBuffer = Buffer.allocUnsafe(options.writeBufferSize >>> 0);
        this.flushDelay = options.flushDelay >>> 0;
    }

    /**
     * Return the last entry in the index.
     *
     * @api
     * @returns {Entry|boolean} The last entry in the index or false if the index is empty.
     */
    get lastEntry() {
        if (this.length > 0) {
            return this.get(this.length);
        }

        return false;
    }

    /**
     * Return the amount of items in the index.
     *
     * @api
     * @returns {number}
     */
    get length() {
        return this.data.length;
    }

    /**
     * Check if the index is opened and ready for access.
     *
     * @api
     * @returns {boolean}
     */
    isOpen() {
        return !!this.fd;
    }

    /**
     * Check if the index file is still intact.
     *
     * @private
     * @returns {number} The amount of entries in the file.
     * @throws {Error} If the file is corrupt or can not be read correctly.
     */
    checkFile() {
        const stat = fs.fstatSync(this.fd);

        if (stat.size === 0) {
            // Freshly created index... write metadata initially.
            this.writeMetadata();
        } else {
            stat.size -= this.readMetadata();
            if (stat.size < 0) {
                throw new Error('Invalid index file!');
            }
        }

        const length = Math.floor(stat.size / this.EntryClass.size);
        if (stat.size > length * this.EntryClass.size) {
            // Corrupt index file
            throw new Error('Index file is corrupt!');
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

        this.fd = fs.openSync(this.fileName, 'a+');

        this.writeBufferCursor = 0;
        this.flushCallbacks = [];
        this.readUntil = -1;

        let length;
        try {
            length = this.checkFile();
        } catch (e) {
            this.close();
            throw e;
        }
        if (length > 0) {
            this.data = new Array(length);
            // Read last item to get the index started
            this.read(length);
        }

        return true;
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
        let metadata = JSON.stringify(this.metadata);
        let metadataSize = Buffer.byteLength(metadata, 'utf8');
        const pad = (16 - ((8 + 4 + metadataSize + 1) % 16)) % 16;
        metadata += ' '.repeat(pad) + "\n";
        metadataSize += pad + 1;
        const metadataBuffer = Buffer.allocUnsafe(8 + 4 + metadataSize);
        metadataBuffer.write(HEADER_MAGIC, 0, 8, 'utf8');
        metadataBuffer.writeUInt32BE(metadataSize, 8);
        metadataBuffer.write(metadata, 8 + 4, metadataSize, 'utf8');
        fs.writeSync(this.fd, metadataBuffer, 0, metadataBuffer.byteLength, 0);
        this.headerSize = 8 + 4 + metadataSize;
    }

    /**
     * Verify the metadata block read from the file against the expected metadata and set it.
     *
     * @private
     * @param {string} metadata Stringified metadata read from the file.
     * @throws {Error} if metadata is set and the read metadata does not match.
     */
    verifyAndSetMetadata(metadata) {
        // Verify metadata if it was set in constructor
        if (this.metadata && JSON.stringify(this.metadata) !== metadata) {
            throw new Error('Index metadata mismatch! ' + metadata);
        }
        try {
            this.metadata = JSON.parse(metadata);
        } catch (e) {
            throw new Error('Invalid metadata.');
        }
    }

    /**
     * Read the index metadata from the file.
     *
     * @private
     * @returns {number} The size of the metadata header.
     * @throws {Error} if the file header magic value is invalid.
     * @throws {Error} if the metadata size in the header is invalid.
     */
    readMetadata() {
        const headerBuffer = Buffer.allocUnsafe(8 + 4);
        fs.readSync(this.fd, headerBuffer, 0, 8 + 4, 0);
        const headerMagic = headerBuffer.toString('utf8', 0, 8);
        if (headerMagic !== HEADER_MAGIC) {
            if (headerMagic.substr(0, 6) === HEADER_MAGIC.substr(0, 6)) {
                throw new Error(`Invalid file version. The index ${this.fileName} was created with a different library version.`);
            }
            throw new Error('Invalid file header.');
        }
        const metadataSize = headerBuffer.readUInt32BE(8);
        if (metadataSize < 3) {
            throw new Error('Invalid metadata size.');
        }

        const metadataBuffer = Buffer.allocUnsafe(metadataSize - 1);
        metadataBuffer.fill(" ");
        fs.readSync(this.fd, metadataBuffer, 0, metadataSize - 1, 8 + 4);
        const metadata = metadataBuffer.toString('utf8').trim();
        this.verifyAndSetMetadata(metadata);
        this.headerSize = 8 + 4 + metadataSize;
        return this.headerSize;
    }

    /**
     * Close the index and release the file handle.
     * @api
     */
    close() {
        this.data = [];
        this.readUntil = -1;
        if (this.fd) {
            this.flush();
            fs.closeSync(this.fd);
            this.fd = undefined;
        }
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
     * @private
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
        if (this.writeBufferCursor === 0) return false;
        fs.writeSync(this.fd, this.writeBuffer, 0, this.writeBufferCursor);
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
        if (typeof callback !== 'function') return;
        this.flushCallbacks.push(() => callback(position));
    }

    /**
     * Append a single entry to the end of this index.
     *
     * @api
     * @param {Entry} entry The index entry to append.
     * @param {function} [callback] A callback function to execute when the index entry is flushed to disk.
     * @returns {number} The index position for the entry. It matches the index size after the insertion.
     */
    add(entry, callback) {
        assertEqual(entry.constructor.name, this.EntryClass.name, `Wrong entry object`);
        assertEqual(entry.constructor.size, this.EntryClass.size, `Invalid entry size`);

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
     * Read a single index entry from the given index position.
     * Will prevent reading if the entry has already been read sequentially from the start.
     *
     * @private
     * @param {number} index
     * @returns {Entry} The index entry at the given position.
     */
    read(index) {
        index--;

        fs.readSync(this.fd, this.readBuffer, 0, this.EntryClass.size, this.headerSize + index * this.EntryClass.size);
        if (index === this.readUntil + 1) {
            this.readUntil++;
        }
        this.data[index] = this.EntryClass.fromBuffer(this.readBuffer);

        return this.data[index];
    }

    /**
     * Read a range of entries from disk. This method will not do any range checks.
     * It will however optimize to prevent reading entries that have already been read sequentially from start.
     *
     * @private
     * @param {number} from The 1-based index position from where to read from (inclusive).
     * @param {number} until The 1-based index position until which to read to (inclusive).
     * @returns {Array<Entry>|boolean} An array of the index entries in the given range or false on error.
     */
    readRange(from, until) {
        if (until === from) {
            return this.read(from);
        }

        from--;
        until--;

        const readFrom = Math.max(this.readUntil + 1, from);
        const amount = (until - readFrom + 1);

        const readBuffer = Buffer.allocUnsafe(amount * this.EntryClass.size);
        let readSize = fs.readSync(this.fd, readBuffer, 0, readBuffer.byteLength, this.headerSize + readFrom * this.EntryClass.size);
        let index = 0;
        while (index < amount && readSize > 0) {
            this.data[index + readFrom] = this.EntryClass.fromBuffer(readBuffer, index * this.EntryClass.size);
            readSize -= this.EntryClass.size;
            index++;
        }
        if (from <= this.readUntil + 1) {
            this.readUntil = Math.max(this.readUntil, until);
        }

        return this.data.slice(from, until + 1);
    }

    /**
     * Read all index entries. Equal to range(1, index.length) with the exception that this returns
     * an empty array if the index is empty.
     *
     * @api
     * @returns {Array<Entry>} An array of all index entries.
     */
    all() {
        if (this.length === 0) {
            return [];
        }
        return this.range(1, this.length);
    }

    /**
     * Get a single index entry at given position, checking the boundaries.
     *
     * @api
     * @param {number} index The 1-based index position to get the entry for.
     * @returns {Entry|boolean} The entry at the given index position or false if out of bounds.
     */
    get(index) {
        index = wrapAndCheck(index, this.length);
        if (index === false) {
            return false;
        }

        if (this.data[index - 1]) {
            return this.data[index - 1];
        }

        return this.read(index);
    }

    /**
     * Check if the given range is within the bounds of the index.
     *
     * @api
     * @param {number} from The 1-based index position from where to get entries from (inclusive).
     * @param {number} until The 1-based index position until where to get entries to (inclusive).
     * @returns {boolean}
     */
    validRange(from, until) {
        if (from < 1 || from > this.length) {
            return false;
        }
        if (until < from || until > this.length) {
            return false;
        }
        return true;
    }

    /**
     * Get a range of index entries.
     *
     * @api
     * @param {number} from The 1-based index position from where to get entries from (inclusive). If < 0 will start at that position from end.
     * @param {number} [until] The 1-based index position until where to get entries to (inclusive). If < 0 will get until that position from the end. Defaults to this.length.
     * @returns {Array<Entry>|boolean} An array of entries for the given range or false on error.
     */
    range(from, until = -1) {
        from = wrapAndCheck(from, this.length);
        until = wrapAndCheck(until, this.length);

        if (from === false || until < from) {
            return false;
        }

        const readFrom = Math.max(this.readUntil + 1, from);
        let readUntil = until;
        while (readUntil >= readFrom && this.data[readUntil - 1]) readUntil--;

        if (readFrom <= readUntil) {
            this.readRange(readFrom, readUntil);
        }

        return this.data.slice(from - 1, until);
    }

    /**
     * Find the given global sequence number inside this index and return the last entry position with a sequence
     * number lower than or equal to `number`. This is equal to the `high` value in the binary search.
     * If the the parameter `min` is set to true, it will search for the first entry position that is at least equal
     * to `number`. This is equal to the `low` value in the binary search.
     *
     * Complexity: O(log {number}) - because we only need to search up to the {number}-th element maximum.
     *
     * @api
     * @param {number} number The sequence number to search for.
     * @param {boolean} [min] If set to true, will return the first entry that has a sequence number greater than or equal to `number`.
     * @returns {number} The last index entry position that is lower than or equal to the given number. Returns 0 if no index matches.
     */
    find(number, min = false) {
        // We only need to search until the searched number because entry.number is always >= position
        const [low, high] = binarySearch(number, Math.min(this.length, number), index => this.get(index).number);
        return min ? low : high;
    }

    /**
     * Truncate the index after the given entry number.
     *
     * @param {number} after The index entry number to truncate after.
     */
    truncate(after) {
        if (after > this.length) {
            return;
        }
        if (after < 0) {
            after = 0;
        }
        this.flush();

        fs.truncateSync(this.fileName, this.headerSize + after * this.EntryClass.size);
        this.data.splice(after);
        this.readUntil = Math.min(this.readUntil, after);
    }
}

module.exports = Index;
module.exports.Entry = Entry;
