const fs = require('fs');
const path = require('path');
const events = require('events');
const Entry = require('../IndexEntry');
const { assert, wrapAndCheck, binarySearch } = require('../util');

// node-event-store-index V01
const HEADER_MAGIC = "nesidx01";

class CorruptedIndexError extends Error {}

/**
 * Returns a constructor for a CorruptedIndexError with the given size property.
 */
function CorruptedIndexErrorFactory(size) {
    return function (...args) {
        let error = new CorruptedIndexError(...args);
        error.size = size;
        return error;
    };
}

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
class ReadableIndex extends events.EventEmitter {

    /**
     * @param {string} [name] The name of the file to use for storing the index.
     * @param {object} [options] An object with additional index options.
     * @param {typeof EntryInterface} [options.EntryClass] The entry class to use for index items. Must implement the EntryInterface methods.
     * @param {string} [options.dataDirectory] The directory to store the index file in. Default '.'.
     * @param {number} [options.cacheSize] The number of most-recent index entries to keep in memory. Default 1024.
     * @param {number} [options.writeBufferSize] The number of bytes to use for the write buffer. Default 4096.
     * @param {number} [options.flushDelay] How many ms to delay the write buffer flush to optimize throughput. Default 100.
     * @param {object} [options.metadata] An object containing the metadata information for this index. Will be written on initial creation and checked on subsequent openings.
     */
    constructor(name = '.index', options = {}) {
        super();
        if (typeof name !== 'string') {
            options = name;
            name = '.index';
        }
        let defaults = {
            dataDirectory: '.',
            EntryClass: Entry
        };
        options = Object.assign(defaults, options);
        Entry.assertValidEntryClass(options.EntryClass);

        this.name = name;
        this.initialize(options);
        this.open();
    }

    /**
     * @protected
     * @param {object} options
     */
    initialize(options) {
        this._length = 0;
        this.cacheSize = options.cacheSize !== undefined ? Math.max(1, options.cacheSize >>> 0) : 1024; // jshint ignore:line
        /* @type Array<Entry> Ring buffer holding at most cacheSize entries */
        this.cache = new Array(this.cacheSize);
        this.fd = null;
        this.fileMode = 'r';
        this.EntryClass = options.EntryClass;
        this.dataDirectory = options.dataDirectory;
        this.fileName = path.resolve(options.dataDirectory, this.name);
        this.readBuffer = Buffer.allocUnsafe(options.EntryClass.size);

        if (options.metadata) {
            this.metadata = Object.assign({entryClass: options.EntryClass.name, entrySize: options.EntryClass.size}, options.metadata);
        }
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
        return this._length;
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
     * @protected
     * @returns {number} The amount of entries in the file. -1 is returned if the index file is empty.
     * @throws {Error} If the file is corrupt or can not be read correctly.
     */
    checkFile() {
        const stat = fs.fstatSync(this.fd);
        if (stat.size === 0) {
            return -1;
        }

        stat.size -= this.readMetadata();
        assert(stat.size >= 0, 'Invalid index file!');

        const length = Math.floor(stat.size / this.EntryClass.size);
        assert(stat.size === length * this.EntryClass.size, 'Index file is corrupt!', CorruptedIndexErrorFactory(length));

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

        this.fd = fs.openSync(this.fileName, this.fileMode);

        this.readUntil = -1;

        const length = this.readFileLength();
        if (length > 0) {
            this._length = length;
            // Read last item to get the index started
            this.read(length);
        }

        return true;
    }

    /**
     * @protected
     * @returns {number} The amount of entries in the file.
     * @throws {Error} If the file is corrupt or can not be read correctly.
     */
    readFileLength() {
        let length;
        try {
            length = this.checkFile();
            assert(length >= 0, 'Index file was truncated to empty!');
        } catch (e) {
            this.close();
            throw e;
        }
        return length;
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

        assert(headerMagic.substr(0, 6) === HEADER_MAGIC.substr(0, 6), 'Invalid file header.');
        assert(headerMagic === HEADER_MAGIC, `Invalid file version. The index ${this.fileName} was created with a different library version (${headerMagic.substr(6)}).`);

        const metadataSize = headerBuffer.readUInt32BE(8);
        assert(metadataSize >= 3, 'Invalid metadata size.');

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
        this._length = 0;
        this.readUntil = -1;
        this.cache.fill(null);
        this.readBuffer.fill(0);
        if (this.fd) {
            fs.closeSync(this.fd);
            this.fd = null;
        }
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
        const i = Number(index) - 1; // 0-based

        fs.readSync(this.fd, this.readBuffer, 0, this.EntryClass.size, this.headerSize + i * this.EntryClass.size);
        if (i === this.readUntil + 1) {
            this.readUntil++;
        }
        const entry = this.EntryClass.fromBuffer(this.readBuffer);
        // Store in ring buffer only if within the current cache window
        if (i >= this._length - this.cacheSize) {
            this.cache[i % this.cacheSize] = entry;
        }
        return entry;
    }

    /**
     * Read a range of entries from disk. This method will not do any range checks.
     * It will however optimize to prevent reading entries that have already been read sequentially from start.
     *
     * Entries within the ring buffer cache window are stored in the cache; entries outside the window
     * (older than cacheSize) are read from disk for the return value but not cached.
     *
     * @private
     * @param {number} from The 1-based index position from where to read from (inclusive).
     * @param {number} until The 1-based index position until which to read to (inclusive).
     * @returns {Array<Entry>|boolean} An array of the index entries in the given range or false on error.
     */
    readRange(from, until) {
        if (until === from) {
            return [this.read(from)];
        }

        const f = from - 1; // 0-based
        const u = until - 1; // 0-based
        const cacheStart = Math.max(0, this._length - this.cacheSize);

        // Build the result array up front
        const result = new Array(u - f + 1);

        // Part 1: Out-of-window entries [f, min(cacheStart-1, u)] — read from disk, do not cache
        const outEnd = Math.min(cacheStart - 1, u);
        if (f < cacheStart && outEnd >= f) {
            const count = outEnd - f + 1;
            const outBuf = Buffer.allocUnsafe(count * this.EntryClass.size);
            const bytesRead = fs.readSync(this.fd, outBuf, 0, outBuf.byteLength, this.headerSize + f * this.EntryClass.size);
            const entries = Math.floor(bytesRead / this.EntryClass.size);
            for (let idx = 0; idx < entries; idx++) {
                result[idx] = this.EntryClass.fromBuffer(outBuf, idx * this.EntryClass.size);
            }
        }

        // Part 2: In-window entries [max(cacheStart, f), u] — use cache + disk for uncached ones
        // All indices accessed below satisfy i >= inStart >= cacheStart, so each slot i % cacheSize
        // is exclusive to index i within the window and cannot hold a stale entry.
        const inStart = Math.max(cacheStart, f);
        if (inStart <= u) {
            // Optimisation: skip entries already loaded sequentially into the cache
            const readFrom = Math.max(this.readUntil + 1, inStart);

            // Trim trailing entries already present in the cache.
            // readUntil >= readFrom >= cacheStart throughout, so all slots checked are in-window.
            let readUntil = u;
            while (readUntil >= readFrom && readUntil >= cacheStart && this.cache[readUntil % this.cacheSize]) {
                readUntil--;
            }

            if (readFrom <= readUntil) {
                const count = readUntil - readFrom + 1;
                const inBuf = Buffer.allocUnsafe(count * this.EntryClass.size);
                const bytesRead = fs.readSync(this.fd, inBuf, 0, inBuf.byteLength, this.headerSize + readFrom * this.EntryClass.size);
                const entries = Math.floor(bytesRead / this.EntryClass.size);
                for (let idx = 0; idx < entries; idx++) {
                    const i = readFrom + idx;
                    this.cache[i % this.cacheSize] = this.EntryClass.fromBuffer(inBuf, idx * this.EntryClass.size);
                }
                if (inStart <= this.readUntil + 1) {
                    this.readUntil = Math.max(this.readUntil, readUntil);
                }
            }

            // Fill the result from the ring buffer for the in-window portion
            for (let i = inStart; i <= u; i++) {
                result[i - f] = this.cache[i % this.cacheSize];
            }
        }

        return result;
    }

    /**
     * Read all index entries. Equal to range(1, index.length) with the exception that this returns
     * an empty array if the index is empty.
     *
     * @api
     * @returns {Array<EntryInterface>} An array of all index entries.
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
        index = wrapAndCheck(index, this._length);
        if (index <= 0) {
            return false;
        }

        const i = index - 1; // 0-based
        // The ring buffer window is [_length - cacheSize, _length - 1].
        // Within this window every index maps to a unique slot (no two indices share a slot),
        // so a non-null slot is guaranteed to belong to index i and cannot be stale.
        if (i >= this._length - this.cacheSize) {
            const cached = this.cache[i % this.cacheSize];
            if (cached) return cached;
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
        return (until >= from && until <= this.length);
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
        from = wrapAndCheck(from, this._length);
        until = wrapAndCheck(until, this._length);

        if (from <= 0 || until < from) {
            return false;
        }

        const f = from - 1; // 0-based
        const u = until - 1; // 0-based
        const cacheStart = Math.max(0, this._length - this.cacheSize);

        // Determine if any disk reads are required
        const hasOutOfWindow = f < cacheStart;
        const inStart = Math.max(cacheStart, f);
        // Entries in [inStart, readUntil] are assumed cached (sequential read guarantee).
        // All indices in [readFrom, u] satisfy >= inStart >= cacheStart — unique, non-stale slots.
        const readFrom = Math.max(this.readUntil + 1, inStart);
        let needsDiskRead = hasOutOfWindow;
        if (!needsDiskRead && inStart <= u) {
            // Scan backwards for uncached in-window tail entries (all >= cacheStart, no stale slots).
            let scanUntil = u;
            while (scanUntil >= readFrom && scanUntil >= cacheStart && this.cache[scanUntil % this.cacheSize]) {
                scanUntil--;
            }
            needsDiskRead = readFrom <= scanUntil;
        }

        if (needsDiskRead) {
            return this.readRange(from, until);
        }

        // All required entries are already in the ring buffer — build result directly.
        // f >= cacheStart here (hasOutOfWindow is false), so all slots are in-window and valid.
        const result = new Array(u - f + 1);
        for (let i = f; i <= u; i++) {
            result[i - f] = this.cache[i % this.cacheSize];
        }
        return result;
    }

    /**
     * Find the given global sequence number inside this index and return the last entry position with a sequence
     * number lower than or equal to `number`. This is equal to the `high` value in the binary search.
     * If the the parameter `min` is set to true, it will search for the first entry position that is at least equal
     * to `number`. This is equal to the `low` value in the binary search.
     *
     * Complexity: O(log `number`) - because we only need to search up to the `number`-th element maximum.
     *
     * @api
     * @param {number} number The sequence number to search for.
     * @param {boolean} [min] If set to true, will return the first entry that has a sequence number greater than or equal to `number`.
     * @returns {number} The last index entry position that is lower than or equal to the `number`. Returns 0 if no index matches.
     */
    find(number, min = false) {
        if (this.length < 1) {
            return 0;
        }
        // We only need to search until the searched number because entry.number is always >= position
        const [low, high] = binarySearch(number, Math.min(this.length, number), index => this.get(index).number);
        return min ? low : high;
    }
}

module.exports = ReadableIndex;
module.exports.Entry = Entry;
module.exports.HEADER_MAGIC = HEADER_MAGIC;
module.exports.CorruptedIndexError = CorruptedIndexError;