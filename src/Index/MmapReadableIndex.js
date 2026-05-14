import fs from 'fs';
import path from 'path';
import events from 'events';
import Entry, { assertValidEntryClass } from '../IndexEntry.js';
import { assert, wrapAndCheck, binarySearch } from '../util.js';
import { loadMmapModule, unmapBuffer } from './MmapModule.js';

// node-event-store-index V01
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
class MmapReadableIndex extends events.EventEmitter {

    /**
     * @param {string} [name] The name of the file to use for storing the index.
     * @param {object} [options] An object with additional index options.
     * @param {typeof EntryInterface} [options.EntryClass] The entry class to use for index items. Must implement the EntryInterface methods.
     * @param {string} [options.dataDirectory] The directory to store the index file in. Default '.'.
     * @param {number} [options.writeBufferSize] The number of bytes to use for mmap growth chunks. Default 4096.
     * @param {object} [options.metadata] An object containing the metadata information for this index. Will be checked on opening.
     */
    constructor(name = '.index', options = {}) {
        super();
        if (typeof name !== 'string') {
            options = name;
            name = '.index';
        }
        const defaults = {
            dataDirectory: '.',
            EntryClass: Entry,
            writeBufferSize: 4096,
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
        this.fd = null;
        this.fileMode = 'r';
        this.EntryClass = options.EntryClass;
        this.dataDirectory = options.dataDirectory;
        this.fileName = path.resolve(options.dataDirectory, this.name);
        this.lengthValue = 0;
        this.headerSize = 0;
        this.mmap = loadMmapModule();
        this.mapBuffer = null;
        this.mappedSize = 0;
        this.mapGrowthBytes = Math.max(options.writeBufferSize >>> 0, this.EntryClass.size);

        if (options.metadata) {
            this.metadata = Object.assign({ entryClass: options.EntryClass.name, entrySize: options.EntryClass.size }, options.metadata);
            this.serializedMetadata = this.serializeMetadata(this.metadata);
        }
    }

    /**
     * Return the amount of items in the index.
     *
     * @api
     * @returns {number}
     */
    get length() {
        return this.lengthValue;
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
     * This will open a file handle and read back metadata.
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

        const length = this.readFileLength();
        this.lengthValue = length;
        this.mapEntries(this.bytesForEntries(length), this.fileMode !== 'r');

        return true;
    }

    /**
     * Close the index and release the file handle.
     * @api
     */
    close() {
        this.lengthValue = 0;
        this.headerSize = 0;
        if (this.fd) {
            fs.closeSync(this.fd);
            this.fd = null;
        }
        this.unmapEntries();
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

        const dataBytes = stat.size - this.readMetadata();
        assert(dataBytes >= 0, 'Invalid index file!');

        const length = Math.floor(dataBytes / this.EntryClass.size);
        assert(dataBytes === length * this.EntryClass.size, 'Index file is corrupt!');

        return length;
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
        try {
            const parsedMetadata = JSON.parse(metadata);
            if (this.serializedMetadata && this.serializedMetadata !== this.serializeMetadata(parsedMetadata)) {
                throw new Error('Index metadata mismatch! ' + metadata);
            }
            this.metadata = parsedMetadata;
            if (!this.serializedMetadata) {
                this.serializedMetadata = this.serializeMetadata(parsedMetadata);
            }
        } catch (e) {
            if (e.message.startsWith('Index metadata mismatch!')) {
                throw e;
            }
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

        const headerBuffer = Buffer.allocUnsafe(8 + 4);
        fs.readSync(this.fd, headerBuffer, 0, 8 + 4, 0);
        const headerMagic = headerBuffer.toString('utf8', 0, 8);

        assert(headerMagic.slice(0, 6) === HEADER_MAGIC.slice(0, 6), 'Invalid file header.');
        assert(headerMagic === HEADER_MAGIC, `Invalid file version. The index ${this.fileName} was created with a different library version (${headerMagic.slice(6)}).`);

        const metadataSize = headerBuffer.readUInt32BE(8);
        assert(metadataSize >= 3, 'Invalid metadata size.');

        const metadataBuffer = Buffer.allocUnsafe(metadataSize - 1);
        metadataBuffer.fill(' ');
        fs.readSync(this.fd, metadataBuffer, 0, metadataSize - 1, 8 + 4);
        const metadata = metadataBuffer.toString('utf8').trim();
        this.verifyAndSetMetadata(metadata);

        this.headerSize = 8 + 4 + metadataSize;
        return this.headerSize;
    }

    /**
     * Read a single index entry from the given index position.
     *
     * @private
     * @param {number} index
     * @returns {Entry|boolean} The index entry at the given position.
     */
    read(index) {
        index = Number(index) - 1;
        if (index < 0 || index >= this.lengthValue) {
            return false;
        }

        return this.readEntryAt(this.headerSize + index * this.EntryClass.size);
    }

    /**
     * Read all index entries.
     *
     * @api
     * @returns {Generator<EntryInterface>} A generator of all index entries.
     */
    all() {
        if (this.length === 0) {
            return this.iterateEmptyEntries();
        }
        return this.iterateEntries(1, this.length, 1);
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
        return (until >= 1 && until <= this.length);
    }

    /**
     * Get a range of index entries.
     *
     * @api
      * @param {number} from The 1-based index position from where to get entries from (inclusive). If < 0 will start at that position from end.
      * @param {number} [until] The 1-based index position until where to get entries to (inclusive). If < 0 will get until that position from the end. Defaults to this.length.
     * @returns {Generator<Entry>|boolean} A generator of entries for the given range or false on error.
     */
    range(from, until = -1) {
        from = wrapAndCheck(from, this.length);
        until = wrapAndCheck(until, this.length);

        if (from <= 0 || from > this.length || until <= 0 || until > this.length) {
            return false;
        }

        if (until >= from) {
            return this.iterateEntries(from, until, 1);
        }
        return this.iterateEntries(from, until, -1);
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
        const [low, high] = binarySearch(number, Math.min(this.length, number), index => this.read(index).number);
        return min ? low : high;
    }

    /**
     * @private
     * @param {number} from The 1-based index position from where to iterate from (inclusive).
     * @param {number} until The 1-based index position until where to iterate to (inclusive).
     * @param {number} step The direction and step (+1 or -1).
     * @returns {Generator<Entry>}
     */
    *iterateEntries(from, until, step) {
        // Stop one step after the inclusive `until` boundary.
        // Forward: until=5 -> stopIndex=5; reverse: until=2 -> stopIndex=0.
        const stopIndex = until - 1 + step;
        for (let index = from - 1; index !== stopIndex; index += step) {
            yield this.readEntryAt(this.headerSize + index * this.EntryClass.size);
        }
    }

    *iterateEmptyEntries() {}

    readEntryAt(offset) {
        return this.EntryClass.fromBuffer(this.mapBuffer, offset);
    }

    bytesForEntries(length) {
        return this.headerSize + length * this.EntryClass.size;
    }

    mapEntries(mapSize, writable) {
        this.unmapEntries();

        if (mapSize < this.headerSize + this.EntryClass.size) {
            this.mappedSize = mapSize;
            return;
        }

        this.mapBuffer = this.mmap.map(
            mapSize,
            writable ? this.mmap.PROT_READ | this.mmap.PROT_WRITE : this.mmap.PROT_READ,
            this.mmap.MAP_SHARED,
            this.fd,
            0
        );
        this.mappedSize = mapSize;
    }

    unmapEntries() {
        if (!this.mapBuffer) {
            return;
        }
        unmapBuffer(this.mmap, this.mapBuffer);
        this.mapBuffer = null;
        this.mappedSize = 0;
    }

    serializeMetadata(metadata) {
        const ordered = {};
        for (const key of Object.keys(metadata).sort()) {
            ordered[key] = metadata[key];
        }
        return JSON.stringify(ordered);
    }
}

export default MmapReadableIndex;
