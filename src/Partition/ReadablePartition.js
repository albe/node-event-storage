import fs from 'fs';
import path from 'path';
import events from 'events';
import mmap from '@fayzanx/mmap-io';
import { assert, alignTo, hash, binarySearch } from '../util.js';

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
     */
    constructor(name, config = {}) {
        super();
        assert(typeof name === 'string' && name !== '', 'Must specify a partition name.');

        let defaults = {
            dataDirectory: '.',
            readBufferSize: DEFAULT_READ_BUFFER_SIZE
        };
        config = Object.assign(defaults, config);
        this.dataDirectory = path.resolve(config.dataDirectory);

        this.name = name;
        this.id = ReadablePartition.idFor(name);
        this.fileName = path.resolve(this.dataDirectory, this.name);
        this.fileMode = 'r';
        this.headerSize = 0;

        this.readBufferSize = config.readBufferSize >>> 0;  // jshint ignore:line
    }

    /**
     * Check if the partition file is opened.
     *
     * @returns {boolean}
     */
    isOpen() {
        return !!this.fd;
    }

    /**
     * Open the partition storage and memory-map the file for zero-copy reads.
     *
     * @api
     * @returns {boolean} Returns false if the file is not a valid partition.
     */
    open() {
        if (this.fd) {
            return true;
        }

        this.fd = fs.openSync(this.fileName, this.fileMode);

        this.headerSize = 0;
        this.size = this.readFileSize();
        if (this.size <= 0) {
            this.close();
            return false;
        }

        this.size -= this.readMetadata();

        this._mmapFile();

        return true;
    }

    /**
     * Map the partition file into memory for zero-copy reads.
     * Sets readBuffer / readBufferPos / readBufferLength so that the existing
     * arithmetic in findDocumentPositionBefore / readDocumentBefore still works.
     *
     * Invariant: buffer[headerSize + position] == first byte of data at position.
     * So cursor = headerSize + position, and readBufferPos = -headerSize gives
     *   cursor = position - readBufferPos  (the same formula as the old code).
     *
     * @protected
     */
    _mmapFile() {
        if (this.size <= 0) {
            this.mmapBuffer = null;
            this.readBuffer = null;
            this.readBufferPos = -1;
            this.readBufferLength = 0;
            return;
        }
        this.mmapBuffer = mmap.map(
            this.headerSize + this.size,
            mmap.PROT_READ,
            mmap.MAP_SHARED,
            this.fd,
            0,
            mmap.MADV_SEQUENTIAL
        );
        // Alias readBuffer to the mmap so that the invariant-based arithmetic
        // in findDocumentPositionBefore / readDocumentBefore keeps working.
        this.readBuffer = this.mmapBuffer;
        this.readBufferPos = -this.headerSize;
        this.readBufferLength = this.headerSize + this.size;
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

        const headerBuffer = Buffer.allocUnsafe(8 + 4);
        fs.readSync(this.fd, headerBuffer, 0, 8 + 4, 0);
        const headerMagic = headerBuffer.toString('utf8', 0, 8);

        assert(headerMagic.substr(0, 6) === HEADER_MAGIC.substr(0, 6), `Invalid file header in partition ${this.name}.`);

        this.header = headerMagic;
        assert(headerMagic === HEADER_MAGIC, `Invalid file version. The partition ${this.name} was created with a different library version (${headerMagic.substr(6)}).`);

        const metadataSize = headerBuffer.readUInt32BE(8);
        assert(metadataSize > 2 && metadataSize <= 4096, 'Invalid metadata size.');

        const metadataBuffer = Buffer.allocUnsafe(metadataSize - 1);
        metadataBuffer.fill(" ");
        fs.readSync(this.fd, metadataBuffer, 0, metadataSize - 1, 8 + 4);
        const metadata = metadataBuffer.toString('utf8').trim();
        try {
            this.metadata = JSON.parse(metadata);
            this.metadata.epoch = this.metadata.epoch /* istanbul ignore next */|| NES_EPOCH.getTime();
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
        return stat.size - this.headerSize;
    }

    /**
     * Close the partition and frees up all resources.
     *
     * @api
     * @returns void
     */
    close() {
        if (this.fd) {
            fs.closeSync(this.fd);
            this.fd = null;
        }
        this.mmapBuffer = null;
        this.readBuffer = null;
        this.readBufferPos = -1;
        this.readBufferLength = 0;
    }

    /**
     * No-op: the memory-mapped buffer already reflects the on-disk contents.
     *
     * @private
     * @param {number} [from]
     */
    fillBuffer(from = 0) { // jshint ignore:line
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

        if (size && dataSize !== size) {
            throw new InvalidDataSizeError(`Invalid document size ${dataSize} at position ${position}, expected ${size}.`);
        }

        const sequenceNumber = buffer.readUInt32BE(offset + 4);
        const time64 = buffer.readDoubleBE(offset + 8);
        return ({ dataSize, sequenceNumber, time64 });
    }

    /**
     * Return a reader anchored at `position` inside the memory-mapped buffer.
     *
     * cursor = headerSize + position so that  buffer[cursor]  is the first
     * byte of the document at data-offset position.
     * length = mmapBuffer.byteLength (the full mapped extent) so that the
     * arithmetic inside findDocumentPositionBefore / readDocumentBefore keeps
     * working unchanged:
     *   position = readBufferPos + bufferSeparatorPosition + separatorSize
     *            = -headerSize  + absOffset                + separatorSize
     *
     * @protected
     * @param {number} position The position in the file to prepare the read buffer for reading from.
     * @returns {{ buffer: Buffer|null, cursor: number, length: number }} A reader object.
     */
    prepareReadBuffer(position) {
        if (!this.mmapBuffer) {
            return ({ buffer: null, cursor: 0, length: 0 });
        }
        const cursor = this.headerSize + position;
        if (cursor + DOCUMENT_HEADER_SIZE >= this.mmapBuffer.byteLength) {
            return ({ buffer: null, cursor: 0, length: 0 });
        }
        return ({
            buffer: this.mmapBuffer,
            cursor,
            length: this.mmapBuffer.byteLength
        });
    }

    /**
     * Return a reader anchored *before* `position` inside the memory-mapped buffer.
     *
     * @protected
     * @param {number} position The position in the file to prepare the read buffer for reading before.
     * @returns {{ buffer: Buffer|null, cursor: number, length: number }} A reader object.
     */
    prepareReadBufferBackwards(position) {
        if (position < 0 || !this.mmapBuffer) {
            return ({ buffer: null, cursor: 0, length: 0 });
        }
        const cursor = this.headerSize + position;
        if (cursor > this.mmapBuffer.byteLength) {
            return ({ buffer: null, cursor: 0, length: 0 });
        }
        return ({
            buffer: this.mmapBuffer,
            cursor,
            length: this.mmapBuffer.byteLength
        });
    }

    /**
     * Read the data from the given position using the zero-copy memory-mapped buffer for flushed data.
     *
     * @api
     * @param {number} position The file position to read from.
     * @param {number} [size] The expected byte size of the document at the given position.
     * @param {object|null} [headerOut] Optional object to populate with the document header fields
     *   (`dataSize`, `sequenceNumber`, `time64`). Pass an existing object to avoid extra allocation.
     * @returns {string|boolean} The data stored at the given position or false if no data could be read.
     * @throws {Error} if the storage entry at the given position is corrupted.
     * @throws {InvalidDataSizeError} if the document size at the given position does not match the provided size.
     * @throws {CorruptFileError} if the document at the given position can not be read completely.
     */
    readFrom(position, size = 0, headerOut = null) {
        assert(this.fd, 'Partition is not opened.');
        assert((position % DOCUMENT_ALIGNMENT) === 0, `Invalid read position ${position}. Needs to be a multiple of ${DOCUMENT_ALIGNMENT}.`);

        const reader = this.prepareReadBuffer(position);
        if (!reader.buffer) {
            return false;
        }

        const { dataSize, sequenceNumber, time64 } = this.readDocumentHeader(reader.buffer, reader.cursor, position, size);
        if (headerOut !== null) {
            headerOut.dataSize = dataSize;
            headerOut.sequenceNumber = sequenceNumber;
            headerOut.time64 = time64;
        }

        // TODO: This should only be checked on opening
        const writeSize = this.documentWriteSize(dataSize);
        if (position + writeSize > this.size) {
            throw new CorruptFileError(`Invalid document at position ${position}. This may be caused by an unfinished write.`);
        }

        const dataStart = reader.cursor + DOCUMENT_HEADER_SIZE;
        const dataEnd = dataStart + dataSize;

        // For very large documents that exceed the current buffer boundary, fall back to a direct read.
        if (dataEnd > reader.buffer.byteLength) {
            const tempReadBuffer = Buffer.allocUnsafe(dataSize);
            fs.readSync(this.fd, tempReadBuffer, 0, dataSize, this.headerSize + position + DOCUMENT_HEADER_SIZE);
            return tempReadBuffer.toString('utf8');
        }

        return reader.buffer.toString('utf8', dataStart, dataEnd);
    }

    /**
     * Find the start position of the document that precedes the given position.
     *
     * @protected
     * @param {number} position The file position to read backwards from.
     * @returns {number|boolean} The start position of the first document before the given position or false if no header could be found.
     */
    findDocumentPositionBefore(position) {
        assert(this.fd, 'Partition is not opened.');
        position -= (position % DOCUMENT_ALIGNMENT);
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
        /* istanbul ignore if */
        if (docPos === false || docPos < 0) return null;
        const reader = this.prepareReadBufferBackwards(Math.min(docPos + (this.readBuffer.byteLength >> 1), this.size));
        /* istanbul ignore if */
        if (!reader.buffer) return null;
        const cursor = docPos - this.readBufferPos;
        /* istanbul ignore if */
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
     * Find the first document whose sequenceNumber is >= the given value.
     * Uses readLast() to short-circuit when the partition contains no such document.
     * Uses a binary search over file positions via readDocumentBefore() to locate the
     * document. The search tracks both the lower bound (position just after the last
     * confirmed "< sequenceNumber" doc) and the upper bound (minimum position of any
     * probed doc with sequenceNumber >= target). The upper bound, when available, is
     * the exact target document, so no further linear scan is needed.
     *
     * @api
     * @param {number} sequenceNumber The 0-based sequence number to search for.
     * @returns {{ reader: Generator<string>, headerOut: object, data: string }|null}
     *   The matched document with its reader and shared headerOut, or null if no such document exists.
     */
    findDocument(sequenceNumber) {
        const last = this.readLast();
        if (!last || last.header.sequenceNumber < sequenceNumber) {
            return null;
        }

        let startPosition = this.size;
        binarySearch(
            sequenceNumber,
            this.size,
            (pos) => {
                const doc = this.readDocumentBefore(pos);
                if (!doc) return sequenceNumber;
                if (doc.header.sequenceNumber < sequenceNumber) {
                    startPosition = Math.max(startPosition, doc.position + this.documentWriteSize(doc.header.dataSize));
                } else {
                    startPosition = Math.min(startPosition, doc.position);
                }
                return doc.header.sequenceNumber;
            }
        );

        const headerOut = {};
        const data = this.readFrom(startPosition, 0, headerOut);
        /* istanbul ignore if */
        if (data === false) {
            return null;
        }
        headerOut.position = startPosition;
        return { headerOut, data };
    }

    /**
     * @api
     * @param {number} [after] The document position to start reading from.
     * @param {object|null} [headerOut] Optional object to populate with document header fields
     *   (`dataSize`, `sequenceNumber`, `time64`, `position`) on each yield. Pass an existing object
     *   to avoid extra allocation. The object is mutated in place before each yield.
     * @returns {Generator<string>} A generator that returns all documents in this partition.
     */
    *readAll(after = 0, headerOut = null) {
        let position = after < 0 ? this.size + after + 1 : after;
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
     * @returns {Generator<string>} A generator that returns all documents in this partition in reverse order.
     */
    *readAllBackwards(before = -1) {
        let position = before < 0 ? this.size + before + 1 : before;
        while ((position = this.findDocumentPositionBefore(position)) !== false) {
            const data = this.readFrom(position);
            yield data;
        }
    }
}

export default ReadablePartition;
export { CorruptFileError, InvalidDataSizeError, HEADER_MAGIC, DOCUMENT_SEPARATOR, DOCUMENT_ALIGNMENT, DOCUMENT_HEADER_SIZE, DOCUMENT_FOOTER_SIZE };