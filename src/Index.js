const fs = require('fs');
const path = require('path');
const mkdirpSync = require('mkdirp').sync;

Buffer.poolSize = 64 * 1024;

/**
 * This is the interface that an Entry class needs to implement.
 */
class EntryInterface {
    static get size() {}
    static fromBuffer(buffer, offset = 0) {}
    toBuffer(buffer, offset) {}
}

/**
 * Default Entry item contains information about the sequence number, the file position, the document size and the partition number.
 */
class Entry extends Array {

    /**
     * @param {number} number The sequence number of the index entry.
     * @param {number} position The file position where the indexed document is stored.
     * @param {number} size The size of the stored document (for verification).
     * @param {number} partition The partition where the indexed document is stored.
     */
    constructor(number, position, size = 0, partition = 0) {
        super(4);
        this[0] = number;
        this[1] = position;
        this[2] = size;
        this[3] = partition;
    }

    static get size() {
        return 4 * 4;
    }

    static fromBuffer(buffer, offset = 0) {
        let number     = buffer.readUInt32LE(offset, true);
        let position   = buffer.readUInt32LE(offset +  4, true);
        let size       = buffer.readUInt32LE(offset +  8, true);
        let partition  = buffer.readUInt32LE(offset + 12, true);
        return new this(number, position, size, partition);
    }

    toBuffer(buffer, offset) {
        buffer.writeUInt32LE(this[0], offset, true);
        buffer.writeUInt32LE(this[1], offset +  4, true);
        buffer.writeUInt32LE(this[2], offset +  8, true);
        buffer.writeUInt32LE(this[2], offset + 12, true);
        return Entry.size;
    }

    get number() {
        return this[0];
    }

    get position() {
        return this[1];
    }

    get size() {
        return this[2];
    }

    get partition() {
        return this[3];
    }

}


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
     * @param {EntryInterface} [EntryClass] The entry class to use for index items. Must implement the EntryInterface methods.
     * @param {string} [name] The name of the file to use for storing the index.
     * @param {Object} [options] An object with additional index options.
     *   Possible options are:
     *    - dataDirectory: The directory to store the index file in. Default '.'.
     *    - writeBufferSize: The number of bytes to use for the write buffer. Default 4096.
     *    - flushDelay: How many ms to delay the write buffer flush to optimize throughput. Default 100.
     *    - metadata: An object containing the metadata information for this index. Will be written on initial creation and checked on subsequent openings.
     */
    constructor(EntryClass = Entry, name = '.index', options = {}) {
        if (typeof EntryClass === 'string') {
            if (typeof name === 'object') {
                options = name;
            }
            name = EntryClass;
            EntryClass = Entry;
        }
        if (typeof EntryClass === 'object') {
            options = EntryClass;
            EntryClass = Entry;
        }
        this.data = [];
        this.name = name || '.index';

        this.dataDirectory = options.dataDirectory || '.';
        if (!fs.existsSync(this.dataDirectory)) {
            mkdirpSync(this.dataDirectory);
        }
        this.fileName = path.resolve(this.dataDirectory, this.name);

        EntryClass = EntryClass || Entry;
        if (typeof EntryClass !== 'function'
            || typeof EntryClass.size === 'undefined'
            || typeof EntryClass.fromBuffer !== 'function'
            || typeof EntryClass.prototype.toBuffer !== 'function') {
            throw new Error('Invalid index entry class. Must implement EntryInterface methods.');
        }
        if (EntryClass.size < 1) {
            throw new Error('Entry class size is non-positive.');
        }
        let entrySize = EntryClass.size;
        this.readBuffer = Buffer.allocUnsafe(entrySize);
        let writeBufferSize = options.writeBufferSize || 4096;
        this.writeBuffer = Buffer.allocUnsafe(writeBufferSize);
        this.writeBufferCursor = 0;
        this.flushDelay = options.flushDelay || 100;
        this.flushCallbacks = [];

        this.EntryClass = EntryClass;
        if (options.metadata) {
            this.metadata = Object.assign({entryClass: EntryClass.name, entrySize: entrySize}, options.metadata);
        }
        this.headerSize = 8 + 4;
        this.readUntil = -1;

        this.fd = null;
        this.open();
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
     * Open the index if it is not already open.
     * This will open a file handle and either write the metadata if the file is empty or read back the metadata and verify
     * it against the metadata provided in the constructor options.
     * @api
     * @returns {boolean} True if the index was opened or false if it was already open.
     */
    open() {
        if (this.fd) {
            return false;
        }

        this.fd = fs.openSync(this.fileName, 'a+');
        if (!this.fd) {
            throw new Error('Error opening index file "' + this.fileName + '".');
        }
        let stat = fs.fstatSync(this.fd);
        try {
            if (!stat) {
                throw new Error('Error stat\'ing index file "' + this.fileName + '".');
            }
            if (stat.size > 0 && stat.size <= 4) {
                throw new Error('Invalid index file!');
            }

            if (stat.size === 0) {
                // Freshly created index... write metadata initially.
                this.writeMetadata();
            } else {
                stat.size -= this.readMetadata();
            }
        } catch (e) {
            this.close();
            throw e;
        }
        let length = Math.floor(stat.size / this.EntryClass.size);
        if (stat.size > length * this.EntryClass.size) {
            this.close();
            // Corrupt index file
            console.log('expected', length * this.EntryClass.size, 'got', stat.size);
            throw new Error('Index file is corrupt!');
        }

        if (length > 0) {
            this.data = new Array(length);
            // Read last item to get the index started
            this.read(length);
        }
        this.readUntil = -1;

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
        let metadata = JSON.stringify(this.metadata) + "\n";
        let metadataSize = Buffer.byteLength(metadata, 'utf8');
        let metadataBuffer = Buffer.allocUnsafe(8 + 4 + metadataSize);
        metadataBuffer.write(HEADER_MAGIC, 0, 8, 'utf8');
        metadataBuffer.writeUInt32BE(metadataSize, 8, true);
        metadataBuffer.write(metadata, 8 + 4, metadataSize, 'utf8');
        fs.writeSync(this.fd, metadataBuffer, 0, metadataBuffer.byteLength, 0);
        this.headerSize = 8 + 4 + metadataSize;
    }

    /**
     * Read the index metadata from the file.
     *
     * @private
     * @returns {number} The size of the metadata header.
     * @throws {Error} if the file header magic value is invalid.
     * @throws {Error} if the metadata size in the header is invalid.
     * @throws {Error} if metadata is set and the read metadata does not match.
     */
    readMetadata() {
        let headerBuffer = Buffer.allocUnsafe(8 + 4);
        fs.readSync(this.fd, headerBuffer, 0, 8 + 4, 0);
        let headerMagic = headerBuffer.toString('utf8', 0, 8);
        if (headerMagic !== HEADER_MAGIC) {
            if (headerMagic.substr(0, -2) === HEADER_MAGIC.substr(0, -2)) {
                throw new Error('Invalid file version. The index ' + this.fileName + ' was created with a different library version.');
            }
            throw new Error('Invalid file header.');
        }
        let metadataSize = headerBuffer.readUInt32BE(8, true);
        if (metadataSize < 3) {
            throw new Error('Invalid metadata size.');
        }

        let metadataBuffer = Buffer.allocUnsafe(metadataSize - 1);
        fs.readSync(this.fd, metadataBuffer, 0, metadataSize - 1, 8 + 4);
        let metadata = metadataBuffer.toString('utf8');

        // Verify metadata if it was set in constructor
        if (this.metadata && JSON.stringify(this.metadata) !== metadata) {
            throw new Error('Index metadata mismatch! ' + metadata);
        }
        this.metadata = JSON.parse(metadata);

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
     * @param {Number} position The index position that will be provided as parameter to the callback.
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
     * @returns {Number} The index position for the entry. It matches the index size after the insertion.
     */
    add(entry, callback) {
        if (entry.constructor.name !== this.EntryClass.name) {
            throw new Error('Wrong entry object, got ' + entry.constructor.name + ', expected ' + this.EntryClass.name);
        }
        if (entry.constructor.size !== this.EntryClass.size) {
            throw new Error('Invalid entry size, got ' + entry.constructor.size + ', expected ' + this.EntryClass.size);
        }
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
     * @param {Number} index
     * @returns {Entry|boolean} The index entry at the given position or false on error.
     */
    read(index) {
        if (!this.fd) {
            return false;
        }

        index--;

        if (index <= this.readUntil) {
            return this.data[index];
        }
        fs.readSync(this.fd, this.readBuffer, 0, this.EntryClass.size, this.headerSize + index * this.EntryClass.size);
        if (index === this.readUntil + 1) {
            this.readUntil++;
        }
        return this.data[index] = this.EntryClass.fromBuffer(this.readBuffer);
    }

    /**
     * Read a range of entries from disk. This method will not do any range checks.
     * It will however optimize to prevent reading entries that have already been read sequentially from start.
     *
     * @private
     * @param {Number} from The 1-based index position from where to read from (inclusive).
     * @param {Number} until The 1-based index position until which to read to (inclusive).
     * @returns {Array<Entry>|boolean} An array of the index entries in the given range or false on error.
     */
    readRange(from, until) {
        if (until < from) {
            return false;
        }
        if (until === from) {
            return this.read(from);
        }

        if (!this.fd) {
            return false;
        }

        from--;
        until--;

        let readFrom = Math.max(this.readUntil + 1, from);
        let amount = (until - readFrom + 1);

        // TODO: rewrite this to make use of a fixed read buffer
        let readBuffer = Buffer.allocUnsafe(amount * this.EntryClass.size);
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
     * @returns {Array<Entry>|boolean} An array of all index entries or false on error.
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
     * @param {Number} index The 1-based index position to get the entry for.
     * @returns {Entry|boolean} The entry at the given index position or false if out of bounds.
     */
    get(index) {
        if (index < 1 || index > this.length) {
            return false;
        }

        if (this.data[index - 1]) {
            return this.data[index - 1];
        }

        return this.read(index);
    }

    /**
     * Get a range of index entries.
     *
     * @api
     * @param {Number} from The 1-based index position from where to get entries from (inclusive). If < 0 will start at that position from end.
     * @param {Number} [until] The 1-based index position until where to get entries to (inclusive). If < 0 will get until that position from the end. Defaults to this.length.
     * @returns {Array<Entry>|boolean} An array of entries for the given range or false on error.
     */
    range(from, until) {
        if (from < 0) from = this.length + from;
        if (from < 1 || from > this.length) {
            return false;
        }
        until = until || this.length;
        if (until < 0) until += this.length;
        if (until < from || until > this.length) {
            return false;
        }

        let readFrom = Math.max(this.readUntil + 1, from);
        let readUntil = until;
        while (readUntil >= readFrom && this.data[readUntil - 1]) readUntil--;
        //while (readFrom <= until && this.data[readFrom - 1]) readFrom++;
        if (readFrom <= readUntil) {
            this.readRange(readFrom, readUntil);
        }

        return this.data.slice(from - 1, until);
    }

    /**
     * Find the last index entry that has a sequence number lower than or equal to `number`.
     *
     * @api
     * @param {number} number The sequence number to search for.
     * @returns {number} The last index entry position that is lower than or equal to the given number.
     */
    find(number) {
        let low = 1;
        let high = this.data.length;
        while (low <= high) {
            let mid = low + ((high - low) >> 1);
            let entry = this.get(mid);
            if (entry.number === number) {
                return mid;
            }
            if (entry.number < number) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return high;
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
        if (this.fd) {
            this.flush();
        }

        fs.truncateSync(this.fileName, this.headerSize + after * this.EntryClass.size);
        this.data.splice(after);
        this.readUntil = Math.min(this.readUntil, after);
    }
}

module.exports = Index;
module.exports.Entry = Entry;
