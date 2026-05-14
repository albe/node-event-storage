import events from 'events';
import { ReadableAppendOnlyMmapedFile } from '../File/AppendOnlyMmapedFile.js';
import Entry, { assertValidEntryClass } from '../IndexEntry.js';
import { assert, wrapAndCheck, binarySearch } from '../util.js';

// Must match the header magic used by the existing index for file-format compatibility.
const HEADER_MAGIC = 'nesidx01';

/**
 * A memory-mapped index that reads an ordered list of fixed-size entry records directly from a
 * mapped buffer, avoiding per-read syscalls and extra copying.
 *
 * File layout (identical header to the existing file-based index):
 *   [0..7]   8-byte magic "nesidx01"
 *   [8..11]  uint32BE: metadata JSON byte length (including padding + newline)
 *   [12..]   metadata JSON, space-padded, newline-terminated, aligned to 16 bytes
 *   [headerSize..fileSize-1]  fixed-size entry records, each of EntryClass.size bytes
 *
 * The file is backed by a WritableAppendOnlyMmapedFile (writable variant) or
 * ReadableAppendOnlyMmapedFile (read-only variant), which stores a 4-byte logical-size
 * marker at the end of the mapped region so that readers can resolve fileSize without
 * scanning.
 */
class MmapReadableIndex extends events.EventEmitter {

    constructor(name = '.index', options = {}) {
        super();
        if (typeof name !== 'string') {
            options = name;
            name = '.index';
        }
        options = Object.assign({ dataDirectory: '.', EntryClass: Entry }, options);
        assertValidEntryClass(options.EntryClass);

        this.name = name;
        this.EntryClass = options.EntryClass;
        this.headerSize = 0;
        this.data = [];
        this.metadata = options.metadata
            ? Object.assign({ entryClass: options.EntryClass.name, entrySize: options.EntryClass.size }, options.metadata)
            : undefined;

        this.file = this.createFile(name, options);
        this.initIndex(options);
        this.open();
    }

    createFile(name, options) {
        return new ReadableAppendOnlyMmapedFile(name, options);
    }

    // Protected hook called after createFile() and before open(), so subclasses can
    // initialise their own state before the file is opened.
    initIndex(options) {}

    get length() {
        if (!this.isOpen() || this.file.fileSize <= this.headerSize) {
            return 0;
        }
        return Math.floor((this.file.fileSize - this.headerSize) / this.EntryClass.size);
    }

    get lastEntry() {
        const len = this.length;
        return len > 0 ? this.get(len) : false;
    }

    isOpen() {
        return this.file.isOpen();
    }

    open() {
        if (this.file.isOpen()) {
            return false;
        }
        this.data = [];
        this.file.open();
        this.readMetadata();
        return true;
    }

    close() {
        this.data = [];
        this.file.close();
    }

    readMetadata() {
        assert(this.file.fileSize >= 12, 'Invalid index file!');
        const prefix = this.file.read(0, 12);
        const magic = prefix.toString('utf8', 0, 8);
        assert(magic.slice(0, 6) === HEADER_MAGIC.slice(0, 6), 'Invalid file header.');
        assert(magic === HEADER_MAGIC, `Invalid file version. The index ${this.file.fileName} was created with a different library version (${magic.slice(6)}).`);
        const metadataSize = prefix.readUInt32BE(8);
        assert(metadataSize >= 3, 'Invalid metadata size.');
        assert(this.file.fileSize >= 12 + metadataSize, 'Invalid index file!');
        const metadataJson = this.file.read(12, metadataSize).toString('utf8').trim();
        this.verifyAndSetMetadata(metadataJson);
        this.headerSize = 12 + metadataSize;
    }

    verifyAndSetMetadata(metadata) {
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
     * Get a single entry at the given 1-based position.
     *
     * @api
     * @param {number} index 1-based position.
     * @returns {Entry|false}
     */
    get(index) {
        index = wrapAndCheck(index, this.length);
        if (index <= 0) {
            return false;
        }
        if (this.data[index - 1]) {
            return this.data[index - 1];
        }
        const offset = this.headerSize + (index - 1) * this.EntryClass.size;
        const entry = this.EntryClass.fromBuffer(this.file.read(offset, this.EntryClass.size));
        this.data[index - 1] = entry;
        return entry;
    }

    /**
     * Get a range of entries.
     *
     * @api
     * @param {number} from  1-based start position (inclusive). Negative wraps from end.
     * @param {number} [until=-1] 1-based end position (inclusive). Negative wraps from end.
     * @returns {Array<Entry>|false}
     */
    range(from, until = -1) {
        from = wrapAndCheck(from, this.length);
        until = wrapAndCheck(until, this.length);
        if (from <= 0 || until < from) {
            return false;
        }
        const count = until - from + 1;

        // Determine the contiguous uncached region to batch-read.
        let readFrom = from;
        let readUntil = until;
        while (readUntil >= readFrom && this.data[readUntil - 1]) {
            readUntil--;
        }

        if (readFrom <= readUntil) {
            const readCount = readUntil - readFrom + 1;
            const offset = this.headerSize + (readFrom - 1) * this.EntryClass.size;
            const buf = this.file.read(offset, readCount * this.EntryClass.size);
            for (let i = 0; i < readCount; i++) {
                const entry = this.EntryClass.fromBuffer(buf, i * this.EntryClass.size);
                this.data[readFrom - 1 + i] = entry;
            }
        }

        return this.data.slice(from - 1, until);
    }

    /**
     * @api
     * @returns {Array<Entry>}
     */
    all() {
        if (this.length === 0) {
            return [];
        }
        return this.range(1, this.length);
    }

    /**
     * @api
     * @param {number} from 1-based start position.
     * @param {number} until 1-based end position.
     * @returns {boolean}
     */
    validRange(from, until) {
        if (from < 1 || from > this.length) {
            return false;
        }
        return (until >= from && until <= this.length);
    }

    /**
     * Binary-search for the last entry with sequence number <= `number`.
     * Pass `min=true` to find the first entry with number >= `number`.
     *
     * @api
     * @param {number} number
     * @param {boolean} [min=false]
     * @returns {number} 1-based position, or 0 if not found.
     */
    find(number, min = false) {
        if (this.length < 1) {
            return 0;
        }
        const [low, high] = binarySearch(number, Math.min(this.length, number), index => this.get(index).number);
        return min ? low : high;
    }
}

export default MmapReadableIndex;
export { HEADER_MAGIC };
