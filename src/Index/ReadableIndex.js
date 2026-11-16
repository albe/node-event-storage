import fs from 'fs';
import os from 'os';
import events from 'events';
import Entry, { assertValidEntryClass } from '../IndexEntry.js';
import FileHandlePool from '../FileHandlePool.js';
import { assert, wrapAndCheck, binarySearch } from '../utils/util.js';
import { normalizeNamedCtorArgs } from '../utils/apiHelpers.js';
import { resolvePath } from "../utils/fsUtil.js";

// node-event-store-index V01
const HEADER_MAGIC = "nesidx01";

// Entries per in-memory chunk. A full chunk of default entries is 64 KiB, so loading one is a single small read.
const CHUNK_CAPACITY = 4096;
const MIN_CHUNK_ALLOCATION = 8;
const LITTLE_ENDIAN_HOST = os.endianness() === 'LE';

/**
 * Create a chunk of raw entry bytes with room for `capacity` entries, optionally copying the entries of a smaller chunk.
 * A dedicated ArrayBuffer keeps the Uint32Array view aligned, which a pooled Buffer does not guarantee.
 *
 * @param {number} entrySize
 * @param {number} count The amount of entries the chunk needs room for. Rounded up to the next power of two.
 * @param {object} [from] The chunk to copy the valid entries from.
 * @returns {{buffer: Buffer, words: Uint32Array, capacity: number, filled: number}}
 */
function createChunk(entrySize, count, from) {
    let capacity = MIN_CHUNK_ALLOCATION;
    while (capacity < count) {
        capacity *= 2;
    }
    capacity = Math.min(capacity, CHUNK_CAPACITY);
    const arrayBuffer = new ArrayBuffer(capacity * entrySize);
    const chunk = { buffer: Buffer.from(arrayBuffer), words: new Uint32Array(arrayBuffer), capacity, filled: 0 };
    if (from) {
        from.buffer.copy(chunk.buffer, 0, 0, from.filled * entrySize);
        chunk.filled = from.filled;
    }
    return chunk;
}

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
        // Entries are kept as raw entry bytes in chunks of CHUNK_CAPACITY, which are read lazily from the file.
        // Only the first `filled` entries of a chunk are valid, the rest is room for appending.
        this.chunks = [];
        this.entryCount = 0;
        // Entries with the default binary layout are read straight from 32-bit words, without creating objects.
        this.wordsPerEntry = (LITTLE_ENDIAN_HOST && options.EntryClass.fromBuffer === Entry.fromBuffer) ? options.EntryClass.size / 4 : 0;
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
        return this.entryCount;
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
        this.setLength(0);
        this.setLength(length);

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
        this.setLength(0);
        this.fileHandlePool.evict(this, false);
        this.opened = false;
    }

    /**
     * Return the chunk that holds the given entry, reading the missing part of it from file if necessary.
     *
     * @private
     * @param {number} index The 0-based entry number.
     * @returns {object|null} The chunk or null if the entry could not be read.
     */
    chunkFor(index) {
        const chunkIndex = Math.floor(index / CHUNK_CAPACITY);
        const chunkStart = chunkIndex * CHUNK_CAPACITY;
        let chunk = this.chunks[chunkIndex];
        if (chunk && index - chunkStart < chunk.filled) {
            return chunk;
        }

        const count = Math.min(CHUNK_CAPACITY, this.length - chunkStart);
        if (!chunk || chunk.capacity < count) {
            chunk = this.chunks[chunkIndex] = createChunk(this.EntryClass.size, count, chunk);
        }
        const entrySize = this.EntryClass.size;
        const bytesRead = fs.readSync(this.getFileHandle(), chunk.buffer, chunk.filled * entrySize, (count - chunk.filled) * entrySize,
            this.headerSize + (chunkStart + chunk.filled) * entrySize);
        chunk.filled += Math.floor(bytesRead / entrySize);
        return index - chunkStart < chunk.filled ? chunk : null;
    }

    /**
     * Set the amount of entries in the index. Additional entries are read from the file on access,
     * entries beyond the new length are dropped from memory.
     *
     * @protected
     * @param {number} length
     */
    setLength(length) {
        if (length >= this.entryCount) {
            this.entryCount = length;
            return;
        }
        this.entryCount = length;
        const chunkCount = Math.ceil(length / CHUNK_CAPACITY);
        this.chunks.length = Math.min(this.chunks.length, chunkCount);
        const lastChunk = this.chunks[chunkCount - 1];
        if (lastChunk) {
            lastChunk.filled = Math.min(lastChunk.filled, length - (chunkCount - 1) * CHUNK_CAPACITY);
        }
    }

    /**
     * Append an entry to the in-memory entries, before it is written to the file.
     *
     * @protected
     * @param {EntryInterface} entry
     */
    appendInMemory(entry) {
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
        this.entryCount++;
    }

    /**
     * Create the entry object for a 0-based entry number. With `reuse` the default entry layout fills one shared
     * object instead of allocating a new one.
     *
     * @private
     * @param {number} index The 0-based entry number.
     * @param {boolean} [reuse=false]
     * @returns {Entry|boolean} The entry or false if it could not be read.
     */
    entryAt(index, reuse = false) {
        const chunk = this.chunkFor(index);
        if (!chunk) {
            return false;
        }
        const position = index % CHUNK_CAPACITY;
        if (!this.wordsPerEntry) {
            return this.EntryClass.fromBuffer(chunk.buffer, position * this.EntryClass.size);
        }
        const words = chunk.words;
        const word = position * this.wordsPerEntry;
        if (!reuse) {
            return new this.EntryClass(words[word], words[word + 1], words[word + 2], words[word + 3]);
        }
        const entry = this.sharedEntry || (this.sharedEntry = new this.EntryClass(0, 0));
        entry[0] = words[word];
        entry[1] = words[word + 1];
        entry[2] = words[word + 2];
        entry[3] = words[word + 3];
        return entry;
    }

    /**
     * Get the sequence number of an entry without creating an entry object for the default entry layout.
     *
     * @param {number} index The 0-based entry number.
     * @returns {number}
     */
    numberAt(index) {
        if (!this.wordsPerEntry) {
            return this.entryAt(index).number;
        }
        return this.chunkFor(index).words[(index % CHUNK_CAPACITY) * this.wordsPerEntry];
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

        return this.entryAt(index - 1);
    }

    /**
     * Get a single index entry without the boundary checks of get(). The returned object may be reused by the next
     * call, so only read its fields right away and never keep it.
     *
     * @param {number} index The 1-based index position, which must be within the index bounds.
     * @returns {Entry|boolean} The entry at the given index position or false if it could not be read.
     */
    peek(index) {
        return this.entryAt(index - 1, true);
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
        const [low, high] = binarySearch(number, Math.min(this.length, number), index => this.numberAt(index - 1));
        return min ? low : high;
    }
}

export default ReadableIndex;
export { Entry, HEADER_MAGIC, CorruptedIndexError, CHUNK_CAPACITY };