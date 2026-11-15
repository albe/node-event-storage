import fs from 'fs';
import events from 'events';
import Entry, { assertValidEntryClass } from '../IndexEntry.js';
import FileHandlePool from '../FileHandlePool.js';
import EntryChunks from './EntryChunks.js';
import { assert, wrapAndCheck, binarySearch } from '../utils/util.js';
import { normalizeNamedCtorArgs } from '../utils/apiHelpers.js';
import { resolvePath } from "../utils/fsUtil.js";

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
     * @param {number} [options.writeBufferSize] The number of bytes to use for the write buffer. Default 4096.
     * @param {number} [options.flushDelay] How many ms to delay the write buffer flush to optimize throughput. Default 100.
     * @param {object} [options.metadata] An object containing the metadata information for this index. Will be written on initial creation and checked on subsequent openings.
     * @param {object} [options.fileHandlePool] Shared file-handle pool used to reopen evicted handles on demand.
     */
    constructor(name = '.index', options = {}) {
        super();
        this.setMaxListeners(0);
        ({ name, options } = normalizeNamedCtorArgs(name, options, '.index'));
        let defaults = {
            dataDirectory: '.',
            EntryClass: Entry
        };
        options = Object.assign(defaults, options);
        assertValidEntryClass(options.EntryClass);

        this.name = name;
        this.initialize(options);
        this.open();
    }

    /**
     * @protected
     * @param {object} options
     */
    initialize(options) {
        this.data = new EntryChunks(options.EntryClass, (buffer, offset, length, entryNumber) => this.readEntries(buffer, offset, length, entryNumber));
        this.opened = false;
        this.fileMode = 'r';
        this.fileHandlePool = options.fileHandlePool || new FileHandlePool();
        this.EntryClass = options.EntryClass;
        this.dataDirectory = options.dataDirectory;
        this.fileName = resolvePath(options.dataDirectory, this.name);

        if (options.metadata) {
            this.metadata = Object.assign({entryClass: options.EntryClass.name, entrySize: options.EntryClass.size}, options.metadata);
        }
        this.headerSize = 0;
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
        return this.opened;
    }

    /**
     * @returns {boolean}
     */
    hasFileHandle() {
        return this.fileHandlePool.has(this);
    }

    /**
     * @returns {number}
     */
    getFileHandle() {
        return this.fileHandlePool.get(this);
    }

    /**
     * Check if the index file is still intact.
     *
     * @protected
     * @returns {number} The amount of entries in the file. -1 is returned if the index file is empty.
     * @throws {Error} If the file is corrupt or can not be read correctly.
     */
    checkFile() {
        const stat = fs.fstatSync(this.getFileHandle());
        if (stat.size === 0) {
            return -1;
        }

        stat.size -= this.readMetadata();
        assert(stat.size >= 0, 'Invalid index file!');

        this.crtime = stat.birthtimeMs;
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
        if (this.opened) {
            return false;
        }
        this.opened = this.getFileHandle() !== null;

        const length = this.readFileLength();
        this.data.clear();
        this.data.setLength(length);

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
        if (this.headerSize > 0) return this.headerSize;
        const fd = this.getFileHandle();
        const headerBuffer = Buffer.allocUnsafe(8 + 4);
        fs.readSync(fd, headerBuffer, 0, 8 + 4, 0);
        const headerMagic = headerBuffer.toString('utf8', 0, 8);

        assert(headerMagic.substring(0, 6) === HEADER_MAGIC.substring(0, 6), 'Invalid file header.');
        assert(headerMagic === HEADER_MAGIC, `Invalid file version. The index ${this.fileName} was created with a different library version (${headerMagic.substring(6)}).`);

        const metadataSize = headerBuffer.readUInt32BE(8);
        assert(metadataSize >= 3, 'Invalid metadata size.');

        const metadataBuffer = Buffer.allocUnsafe(metadataSize - 1);
        metadataBuffer.fill(" ");
        fs.readSync(fd, metadataBuffer, 0, metadataSize - 1, 8 + 4);
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
        this.data.clear();
        this.fileHandlePool.evict(this, false);
        this.opened = false;
    }

    /**
     * Read raw entries from the index file.
     *
     * @private
     * @param {Buffer} buffer The buffer to read into.
     * @param {number} offset The buffer offset to read into.
     * @param {number} length The amount of bytes to read.
     * @param {number} entryNumber The 0-based number of the first entry to read.
     * @returns {number} The amount of bytes read.
     */
    readEntries(buffer, offset, length, entryNumber) {
        return fs.readSync(this.getFileHandle(), buffer, offset, length, this.headerSize + entryNumber * this.EntryClass.size);
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
        index = wrapAndCheck(index, this.length);
        if (index <= 0) {
            return false;
        }

        return this.data.get(index - 1);
    }

    /**
     * Get a single index entry without the boundary checks of get(). The returned object may be reused by the next
     * call, so only read its fields right away and never keep it.
     *
     * @param {number} index The 1-based index position, which must be within the index bounds.
     * @returns {Entry|boolean} The entry at the given index position or false if it could not be read.
     */
    peek(index) {
        return this.data.peek(index - 1);
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
     * @param {number} [until=-1] The 1-based index position until where to get entries to (inclusive). If < 0 will get until that position from the end. Defaults to this.length.
     * @returns {Array<Entry>|boolean} An array of entries for the given range or false on error.
     */
    range(from, until = -1) {
        from = wrapAndCheck(from, this.length);
        until = wrapAndCheck(until, this.length);

        if (from <= 0 || until < from) {
            return false;
        }

        return this.data.slice(from - 1, until);
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
     * @param {boolean} [min=false] If set to true, will return the first entry that has a sequence number greater than or equal to `number`.
     * @returns {number} The last index entry position that is lower than or equal to the `number`. Returns 0 if no index matches.
     */
    find(number, min = false) {
        if (this.length < 1) {
            return 0;
        }
        // We only need to search until the searched number because entry.number is always >= position
        const [low, high] = binarySearch(number, Math.min(this.length, number), index => this.data.numberAt(index - 1));
        return min ? low : high;
    }
}

export default ReadableIndex;
export { Entry, HEADER_MAGIC, CorruptedIndexError };