import fs from 'fs';
import path from 'path';
import events from 'events';
import { assert, alignTo, hash, binarySearch } from '../utils/util.js';
import { resolvePath } from "../utils/fsUtil.js";



const DEFAULT_READ_BUFFER_SIZE = 64 * 1024;
const DOCUMENT_HEADER_SIZE = 16;
const DOCUMENT_ALIGNMENT = 4;
const DOCUMENT_SEPARATOR = "\x00\x00\x1E\n";
const DOCUMENT_FOOTER_SIZE = 4 /* additional data size footer */ + DOCUMENT_SEPARATOR.length;

// node-event-store partition V03
const HEADER_MAGIC = "nesprt03";

const NES_EPOCH = new Date('2020-01-01T00:00:00');

class CorruptFileError extends Error {}
class InvalidDataSizeError extends Error {}

/**
 * A partition is a single file where the storage will write documents to depending on some partitioning rules.
 * In the case of an event store, this is most likely the (write) streams.
 */
class ReadablePartition extends events.EventEmitter {

    /**
     * Get the id for a specific partition name.
     *
     * @param {string} name
     * @returns {number}
     */
    static idFor(name) {
        return hash(name);
    }

    /**
     * @param {string} name The name of the partition.
     * @param {object} [config] An object with storage parameters.
     * @param {string} [config.dataDirectory] The path where the storage data should reside. Default '.'.
     * @param {number} [config.readBufferSize] Size of the read buffer in bytes. Default 4096.
     * @param {object} [config.fileHandlePool] Shared file-handle pool used to reopen evicted handles on demand.
     */
    constructor(name, config = {}) {
        super();
        assert(typeof name === 'string' && name !== '', 'Must specify a partition name.');

        let defaults = {
            dataDirectory: '.',
            readBufferSize: DEFAULT_READ_BUFFER_SIZE
        };
        config = Object.assign(defaults, config);
        this.dataDirectory = resolvePath(config.dataDirectory);

        this.name = name;
        this.id = ReadablePartition.idFor(name);
        this.fileName = path.resolve(this.dataDirectory, this.name);
        this.fileMode = 'r';
        this.fileHandlePool = config.fileHandlePool || null;
        this.headerSize = 0;
        this.fd = null;
        this.opened = false;

        this.readBufferSize = config.readBufferSize >>> 0;  // jshint ignore:line
    }

    /**
     * Check if the partition file is opened.
     *
     * @returns {boolean}
     */
    isOpen() {
        return this.opened;
    }

    hasFileHandle() {
        return !!this.fd;
    }

    getFileHandle() {
        assert(this.opened, 'Partition is not open.');
        if (this.fileHandlePool) {
            return this.fileHandlePool.get(this);
        }
        this.fd = this.fd || fs.openSync(this.fileName, this.fileMode);
        return this.fd;
    }

    /**
     * Open the partition storage and create read buffers.
     *
     * @api
     * @returns {boolean} Returns false if the file is not a valid partition.
     */
    open() {
        if (this.opened) {
            return true;
        }
        this.opened = true;

        try {
            this.getFileHandle();
        } catch (error) {
            this.opened = false;
            throw error;
        }

        try {
            // allocUnsafeSlow because we don't need buffer pooling for these relatively long-lived buffers
            this.readBuffer = Buffer.allocUnsafeSlow(this.readBufferSize);
            // Where inside the file the read buffer starts
            this.readBufferPos = -1;
            this.readBufferLength = 0;

            this.headerSize = 0;
            this.size = this.readFileSize();
            if (this.size <= 0) {
                this.close();
                return false;
            }

            this.size -= this.readMetadata();
            return true;
        } catch (error) {
            this.close();
            throw error;
        }
    }

    /**
     * Read the partition metadata from the file.
     *
     * @private
     * @returns {number} The size of the metadata header.
     * @throws {Error} if the file header magic value is invalid.
     * @throws {Error} if the metadata size in the header is invalid.
     */
    readMetadata() {
        assert(this.size >= 16, `Invalid file.`);

        const fd = this.getFileHandle();
        const headerBuffer = Buffer.allocUnsafe(8 + 4);
        fs.readSync(fd, headerBuffer, 0, 8 + 4, 0);
        const headerMagic = headerBuffer.toString('utf8', 0, 8);

        assert(headerMagic.substring(0, 6) === HEADER_MAGIC.substring(0, 6), `Invalid file header in partition ${this.name}.`);

        this.header = headerMagic;
        assert(headerMagic === HEADER_MAGIC, `Invalid file version. The partition ${this.name} was created with a different library version (${headerMagic.substring(6)}).`);

        const metadataSize = headerBuffer.readUInt32BE(8);
        assert(metadataSize > 2 && metadataSize <= 4096, 'Invalid metadata size.');

        const metadataBuffer = Buffer.allocUnsafe(metadataSize - 1);
        metadataBuffer.fill(" ");
        fs.readSync(fd, metadataBuffer, 0, metadataSize - 1, 8 + 4);
        const metadata = metadataBuffer.toString('utf8').trim();
        try {
            this.metadata = JSON.parse(metadata);
            this.metadata.epoch = this.metadata.epoch /* c8 ignore next */|| NES_EPOCH.getTime();
        } catch (e) {
            throw new Error('Invalid metadata.');
        }
        this.headerSize = 8 + 4 + metadataSize;
        return this.headerSize;
    }

    /**
     * Get the storage size for a document of a given size.
     *
     * @param {number} dataSize The actual data size of the document.
     * @returns {number} The size of the data including header, padded to 16 bytes alignment and ended with a line break.
     */
    documentWriteSize(dataSize) {
        const padSize = alignTo(dataSize + DOCUMENT_FOOTER_SIZE, DOCUMENT_ALIGNMENT);
        return DOCUMENT_HEADER_SIZE + dataSize + padSize + DOCUMENT_FOOTER_SIZE;
    }

    /**
     * @protected
     * @returns {number} The file size not including the file header.
     */
    readFileSize() {
        const stat = fs.statSync(this.fileName);
        this.crtime = stat.birthtimeMs;
        return stat.size - this.headerSize;
    }

    /**
     * Close the partition and frees up all resources.
     *
     * @api
     * @returns void
     */
    close() {
        if (this.fileHandlePool) {
            // `false` indicates an explicit close (not an eviction), so logical open state is cleared afterwards.
            this.fileHandlePool.evict(this, false);
        } else if (this.fd) {
            const fd = this.fd;
            try {
                this.beforeFileHandleClose?.(false);
            } finally {
                this.fd = null;
                fs.closeSync(fd);
            }
        }
        this.opened = false;
        if (this.readBuffer) {
            this.readBuffer = null;
            this.readBufferPos = -1;
            this.readBufferLength = 0;
        }
    }

    /**
     * Fill the internal read buffer starting from the given position.
     *
     * @private
     * @param {number} [from] The file position to start filling the read buffer from. Default 0.
     */
    fillBuffer(from = 0) {
        const fd = this.getFileHandle();
        this.readBufferLength = fs.readSync(fd, this.readBuffer, 0, this.readBuffer.byteLength, this.headerSize + from);
        this.readBufferPos = from;
    }

    /**
     * @private
     * @param {Buffer} buffer The buffer to read the data length from.
     * @param {number} offset The position inside the buffer to start reading from.
     * @param {number} position The file position to start reading from.
     * @param {number} [size] The expected byte size of the document at the given position.
     * @returns {{ dataSize: number, sequenceNumber: number, time64: number }} The metadata fields of the document
     * @throws {Error} if the storage entry at the given position is corrupted.
     * @throws {InvalidDataSizeError} if the document size at the given position does not match the provided size.
     * @throws {CorruptFileError} if the document at the given position can not be read completely.
     */
    readDocumentHeader(buffer, offset, position, size) {
        const dataSize = buffer.readUInt32BE(offset + 0);
        assert(dataSize > 0 && dataSize <= 64 * 1024 * 1024, `Error reading document size from ${position}, got ${dataSize}.`);

        assert(!size || dataSize === size, `Invalid document size ${dataSize} at position ${position}, expected ${size}.`, InvalidDataSizeError);

        const sequenceNumber = buffer.readUInt32BE(offset + 4);
        const time64 = buffer.readDoubleBE(offset + 8);
        return ({ dataSize, sequenceNumber, time64 });
    }

    resolveIterationPosition(position) {
        return position < 0 ? this.size + position + 1 : position;
    }

    selectReader(position, size, backwardsHint) {
        if (size > 0 && backwardsHint) {
            const bufferOffset = DOCUMENT_HEADER_SIZE + size;
            const reader = this.prepareReadBufferBackwards(position + bufferOffset, bufferOffset);
            return { reader, bufferOffset };
        }
        return { reader: this.prepareReadBuffer(position), bufferOffset: 0 };
    }

    assignHeaderOutput(headerOut, header) {
        if (headerOut === null) {
            return;
        }
        headerOut.dataSize = header.dataSize;
        headerOut.sequenceNumber = header.sequenceNumber;
        // Denormalize time64 relative to this partition's epoch so callers can compare
        // timestamps across partitions without needing to know the epoch value.
        headerOut.time64 = this.metadata.epoch + header.time64;
    }

    /**
     * Read the data from the given position.
     *
     * @api
     * @param {number} position The file position to read from.
     * @param {number} [size] The expected byte size of the document at the given position.
     * @param {object|null} [headerOut] Optional object to populate with the document header fields
     *   (`dataSize`, `sequenceNumber`, `time64`). Pass an existing object to avoid extra allocation.
     * @param {boolean} [backwardsHint] If set to true, will optimize buffering for backwards reads.
     * @returns {Buffer|boolean} The data stored at the given position or false if no data could be read.
     * @throws {Error} if the storage entry at the given position is corrupted.
     * @throws {InvalidDataSizeError} if the document size at the given position does not match the provided size.
     * @throws {CorruptFileError} if the document at the given position can not be read completely.
     */
    readFrom(position, size = 0, headerOut = null, backwardsHint = false) {
        const fd = this.getFileHandle();
        assert((position % DOCUMENT_ALIGNMENT) === 0, `Invalid read position ${position}. Needs to be a multiple of ${DOCUMENT_ALIGNMENT}.`);

        const { reader, bufferOffset } = this.selectReader(position, size, backwardsHint);
        if (reader.length < DOCUMENT_HEADER_SIZE) {
            return false;
        }

        // prepareReadBufferBackwards positions the cursor at position + bufferOffset (the end of
        // the document data), so the previous document in file order lands inside the buffer on the next
        // backwards read. Adjust the cursor back to the document header start before reading.
        reader.cursor -= bufferOffset;
        let dataPosition = reader.cursor + DOCUMENT_HEADER_SIZE;
        const header = this.readDocumentHeader(reader.buffer, reader.cursor, position, size);
        const dataSize = header.dataSize;
        this.assignHeaderOutput(headerOut, header);

        const writeSize = this.documentWriteSize(dataSize);
        assert(position + writeSize <= this.size, `Invalid document at position ${position}. This may be caused by an unfinished write.`, CorruptFileError);

        if (dataSize + DOCUMENT_HEADER_SIZE > reader.buffer.byteLength) {
            const tempReadBuffer = Buffer.allocUnsafe(dataSize);
            fs.readSync(fd, tempReadBuffer, 0, dataSize, this.headerSize + position + DOCUMENT_HEADER_SIZE);
            return tempReadBuffer;
        }

        if (reader.cursor > 0 && dataPosition + dataSize > reader.length) {
            this.fillBuffer(position);
            dataPosition = DOCUMENT_HEADER_SIZE;
        }

        // reader.buffer is a shared buffer filled by fillBuffer; callers must consume the returned
        // view before the next readFrom call (which may fill the same buffer region).
        return reader.buffer.subarray(dataPosition, dataPosition + dataSize);
    }

    /**
     * Prepare the read buffer for reading from the specified position.
     *
     * @protected
     * @param {number} position The position in the file to prepare the read buffer for reading from.
     * @returns {{ buffer: Buffer|null, cursor: number, length: number }} A reader object with properties `buffer`, `cursor` and `length`.
     */
    prepareReadBuffer(position) {
        if (position + DOCUMENT_HEADER_SIZE >= this.size) {
            return ({ buffer: null, cursor: 0, length: 0 });
        }
        let bufferCursor = position - this.readBufferPos;
        if (this.readBufferPos < 0 || bufferCursor < 0 || bufferCursor + DOCUMENT_HEADER_SIZE + DOCUMENT_ALIGNMENT > this.readBufferLength) {
            this.fillBuffer(position);
            bufferCursor = 0;
        }
        return ({ buffer: this.readBuffer, cursor: bufferCursor, length: this.readBufferLength });
    }

    /**
     * Prepare the read buffer for reading *before* the specified position. Don't try to read *after* the returned cursor.
     *
     * @protected
     * @param {number} position The position in the file to prepare the read buffer for reading before.
     * @param {number} [size] The amount of bytes that need to be buffered before position. By default, only guarantees that the document footer can be read.
     * @returns {{ buffer: Buffer|null, cursor: number, length: number }} A reader object with properties `buffer`, `cursor` and `length`.
     */
    prepareReadBufferBackwards(position, size = 0) {
        if (position < 0) {
            return ({ buffer: null, cursor: 0, length: 0 });
        }
        let bufferCursor = position - this.readBufferPos;
        if (this.readBufferPos < 0 || (this.readBufferPos > 0 && bufferCursor < size + DOCUMENT_FOOTER_SIZE) || bufferCursor > this.readBufferLength) {
            this.fillBuffer(Math.max(position - this.readBuffer.byteLength, 0));
            bufferCursor = position - this.readBufferPos;
        }
        return ({ buffer: this.readBuffer, cursor: bufferCursor, length: this.readBufferLength });
    }

    /**
     * Find the start position of the document that precedes the given position.
     *
     * @protected
     * @param {number} position The file position to read backwards from.
     * @returns {number|boolean} The start position of the first document before the given position or false if no header could be found.
     */
    findDocumentPositionBefore(position) {
        this.getFileHandle();
        if (position > 0) position -= (position % DOCUMENT_ALIGNMENT);
        if (position <= 0) {
            return false;
        }

        const separatorSize = DOCUMENT_SEPARATOR.length;
        // Optimization if we are at an exact document boundary, where we can just read the document size
        let reader = this.prepareReadBufferBackwards(position);
        const block = reader.buffer.toString('ascii', reader.cursor - separatorSize, reader.cursor);
        if (block === DOCUMENT_SEPARATOR) {
            const dataSize = reader.buffer.readUInt32BE(reader.cursor - separatorSize - 4);
            return position - this.documentWriteSize(dataSize);
        }

        do {
            reader = this.prepareReadBufferBackwards(position - separatorSize);

            const bufferSeparatorPosition = reader.buffer.lastIndexOf(DOCUMENT_SEPARATOR, reader.cursor - separatorSize, 'ascii');
            if (bufferSeparatorPosition >= 0) {
                position = this.readBufferPos + bufferSeparatorPosition + separatorSize;
                break;
            }
            position -= this.readBufferLength;
        } while (position > 0);
        return Math.max(0, position);
    }

    /**
     * Find the document that starts immediately before `position`, fill the read buffer
     * centered around that document's start, and return its file position and parsed header.
     * The buffer is centered by calling `prepareReadBufferBackwards` with a position
     * half a buffer-length ahead of the document start, clamped to file size, so the
     * document start lands near the middle of the buffer.
     *
     * @private
     * @param {number} position The file position to search before.
     * @returns {{ header: {dataSize: number, sequenceNumber: number, time64: number}, position: number }|null}
     *   The document header and file position, or null if no document could be found.
     */
    readDocumentBefore(position) {
        const docPos = this.findDocumentPositionBefore(position);
        /* c8 ignore next */
        if (docPos === false || docPos < 0) return null;
        const reader = this.prepareReadBufferBackwards(Math.min(docPos + (this.readBuffer.byteLength >> 1), this.size));
        /* c8 ignore next */
        if (!reader.buffer) return null;
        const cursor = docPos - this.readBufferPos;
        /* c8 ignore next */
        if (cursor < 0 || cursor + DOCUMENT_HEADER_SIZE > reader.length) return null;
        const header = this.readDocumentHeader(reader.buffer, cursor, docPos);
        return { header, position: docPos };
    }

    /**
     * Read the header and file position of the last document in this partition.
     *
     * @api
     * @returns {{ header: {dataSize: number, sequenceNumber: number, time64: number}, position: number } | null}
     *   The last document's header and its file position, or null if the partition is empty or unreadable.
     */
    readLast() {
        if (this.size === 0) return null;
        return this.readDocumentBefore(this.size);
    }

    /**
     * Find a document around the target sequence number using one of two binary-search modes.
     * Uses readLast() to short-circuit when the partition contains no such document.
     * Uses a binary search over file positions via readDocumentBefore() to locate the
     * document and tracks nearest candidates on both sides of the target.
     *
     * @api
     * @param {number} sequenceNumber The 0-based sequence number to search for.
     * @param {boolean} [min=true] When true, returns the first document with sequenceNumber >= target.
     *   When false, returns the last document with sequenceNumber <= target.
     * @returns {{ data: Buffer, header: object, position: number }|null}
     *   The matched document with its header and position, or null if no such document exists.
     */
    findDocument(sequenceNumber, min = true) {
        const last = this.readLast();
        if (!last || (min && last.header.sequenceNumber < sequenceNumber)) {
            return null;
        }

        const [low, high] = binarySearch(
            sequenceNumber,
            this.size,
            (pos) => this.readDocumentBefore(pos)?.header.sequenceNumber ?? Number.MIN_SAFE_INTEGER
        );

        const position = this.findDocumentPositionBefore(min ? low : high);
        if (position === false || position < 0) {
            return null;
        }

        const header = {};
        const data = this.readFrom(position, 0, header);
        /* c8 ignore next 3 */
        if (data === false) {
            return null;
        }
        return { data, header, position };
    }

    /**
     * @api
     * @param {number} [after] The document position to start reading from.
     * @param {object|null} [headerOut] Optional object to populate with document header fields
     *   (`dataSize`, `sequenceNumber`, `time64`, `position`) on each yield. Pass an existing object
     *   to avoid extra allocation. The object is mutated in place before each yield.
     * @returns {Generator<Buffer>} A generator that returns all documents in this partition.
     */
    *readAll(after = 0, headerOut = null) {
        let position = this.resolveIterationPosition(after);
        const internalHeader = headerOut !== null ? headerOut : {};
        let data;
        while ((data = this.readFrom(position, 0, internalHeader)) !== false) {
            if (headerOut !== null) {
                headerOut.position = position;
            }
            yield data;
            position += this.documentWriteSize(internalHeader.dataSize);
        }
    }

    /**
     * @api
     * @param {number} [before] The document position to start reading backward from.
     * @param {object|null} [headerOut] Optional object to populate with document header fields
     *   (`dataSize`, `sequenceNumber`, `time64`, `position`) on each yield.
     * @returns {Generator<Buffer>} A generator that returns all documents in this partition in reverse order.
     */
    *readAllBackwards(before = -1, headerOut = null) {
        let position = this.resolveIterationPosition(before);
        const internalHeader = headerOut !== null ? headerOut : {};
        while ((position = this.findDocumentPositionBefore(position)) !== false) {
            const data = this.readFrom(position, 0, internalHeader, true);
            if (headerOut !== null) {
                headerOut.position = position;
            }
            yield data;
        }
    }

    /**
     * Read documents with sequenceNumber in the inclusive range [from, until].
     * If from > until, documents are yielded in reverse order.
     *
     * @api
     * @param {number} [from=0]
     * @param {number} [until=Number.MAX_SAFE_INTEGER]
     * @returns {Generator<{ data: Buffer, header: object, entry: object }>}
     */
    *readRange(from = 0, until = Number.MAX_SAFE_INTEGER) {
        const forwards = from <= until;
        const lo = Math.min(from, until);
        const hi = Math.max(from, until);
        const found = this.findDocument(forwards ? lo : hi, forwards);
        if (!found || found.header.sequenceNumber < lo || found.header.sequenceNumber > hi) {
            return;
        }

        const doc = {
            entry: { partition: this.id },
            header: {}
        };
        const iterator = forwards
            ? this.readAll(found.position, doc.header)
            : this.readAllBackwards(found.position + this.documentWriteSize(found.header.dataSize), doc.header);

        for (const data of iterator) {
            if (doc.header.sequenceNumber < lo || doc.header.sequenceNumber > hi) {
                return;
            }
            doc.entry.number = doc.header.sequenceNumber;
            doc.entry.position = doc.header.position;
            doc.entry.size = doc.header.dataSize;
            doc.data = data;
            yield doc;
        }
    }

}

export default ReadablePartition;
export { CorruptFileError, InvalidDataSizeError, HEADER_MAGIC, DOCUMENT_SEPARATOR, DOCUMENT_ALIGNMENT, DOCUMENT_HEADER_SIZE, DOCUMENT_FOOTER_SIZE };
