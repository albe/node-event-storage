const fs = require('fs');
const path = require('path');
const events = require('events');
const { assert, alignTo } = require('../util');

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
 * Method for hashing a string (partition name) to a 32-bit unsigned integer.
 *
 * @param {string} str
 * @returns {number}
 */
function hash(str) {
    /* istanbul ignore if */
    if (str.length === 0) {
        return 0;
    }
    let hash = 5381,
        i    = str.length;

    while(i) {
        hash = ((hash << 5) + hash) ^ str.charCodeAt(--i); // jshint ignore:line
    }

    /* JavaScript does bitwise operations (like XOR, above) on 32-bit signed
     * integers. Since we want the results to be always positive, convert the
     * signed int to an unsigned by doing an unsigned bitshift. */
    return hash >>> 0; // jshint ignore:line
}

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
     * Open the partition storage and create read buffers.
     *
     * @api
     * @returns {boolean} Returns false if the file is not a valid partition.
     */
    open() {
        if (this.fd) {
            return true;
        }

        this.fd = fs.openSync(this.fileName, this.fileMode);

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
    }

    /**
     * @returns {number} -1 if the partition is ok and the sequence number of the broken document if a torn write was detected.
     */
    checkTornWrite() {
        const reader = this.prepareReadBufferBackwards(this.size);
        const separator = reader.buffer.toString('ascii', reader.cursor - DOCUMENT_SEPARATOR.length, reader.cursor);
        if (separator !== DOCUMENT_SEPARATOR) {
            const position = this.findDocumentPositionBefore(this.size);
            const reader = this.prepareReadBuffer(position);
            const { sequenceNumber } = this.readDocumentHeader(reader.buffer, reader.cursor, position);
            return sequenceNumber;
        }
        return -1;
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
        this.readBufferLength = fs.readSync(this.fd, this.readBuffer, 0, this.readBuffer.byteLength, this.headerSize + from);
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

        if (size && dataSize !== size) {
            throw new InvalidDataSizeError(`Invalid document size ${dataSize} at position ${position}, expected ${size}.`);
        }

        const sequenceNumber = buffer.readUInt32BE(offset + 4);
        const time64 = buffer.readDoubleBE(offset + 8);
        return ({ dataSize, sequenceNumber, time64 });
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
     * Prepare the read buffer for reading *before* the specified position. Don't try to reader *after* the returned cursor.
     *
     * @protected
     * @param {number} position The position in the file to prepare the read buffer for reading before.
     * @returns {{ buffer: Buffer|null, cursor: number, length: number }} A reader object with properties `buffer`, `cursor` and `length`.
     */
    prepareReadBufferBackwards(position) {
        if (position < 0) {
            return ({ buffer: null, cursor: 0, length: 0 });
        }
        let bufferCursor = position - this.readBufferPos;
        if (this.readBufferPos < 0 || (this.readBufferPos > 0 && bufferCursor < DOCUMENT_FOOTER_SIZE)) {
            this.fillBuffer(Math.max(position - this.readBuffer.byteLength, 0));
            bufferCursor = position - this.readBufferPos;
        }
        return ({ buffer: this.readBuffer, cursor: bufferCursor, length: this.readBufferLength });
    }

    /**
     * Read the data from the given position.
     *
     * @api
     * @param {number} position The file position to read from.
     * @param {number} [size] The expected byte size of the document at the given position.
     * @returns {string|boolean} The data stored at the given position or false if no data could be read.
     * @throws {Error} if the storage entry at the given position is corrupted.
     * @throws {InvalidDataSizeError} if the document size at the given position does not match the provided size.
     * @throws {CorruptFileError} if the document at the given position can not be read completely.
     */
    readFrom(position, size = 0) {
        assert(this.fd, 'Partition is not opened.');
        assert((position % DOCUMENT_ALIGNMENT) === 0, `Invalid read position ${position}. Needs to be a multiple of ${DOCUMENT_ALIGNMENT}.`);

        const reader = this.prepareReadBuffer(position);
        if (reader.length < size + DOCUMENT_HEADER_SIZE) {
            return false;
        }

        let dataPosition = reader.cursor + DOCUMENT_HEADER_SIZE;
        const { dataSize } = this.readDocumentHeader(reader.buffer, reader.cursor, position, size);

        // TODO: This should only be checked on opening
        const writeSize = this.documentWriteSize(dataSize);
        if (position + writeSize > this.size) {
            throw new CorruptFileError(`Invalid document at position ${position}. This may be caused by an unfinished write.`);
        }

        if (dataSize + DOCUMENT_HEADER_SIZE > reader.buffer.byteLength) {
            //console.log('sync read for large document size', dataLength, 'at position', position);
            const tempReadBuffer = Buffer.allocUnsafe(dataSize);
            fs.readSync(this.fd, tempReadBuffer, 0, dataSize, this.headerSize + position + DOCUMENT_HEADER_SIZE);
            return tempReadBuffer.toString('utf8');
        }

        if (reader.cursor > 0 && dataPosition + dataSize > reader.length) {
            this.fillBuffer(position);
            dataPosition = DOCUMENT_HEADER_SIZE;
        }

        return reader.buffer.toString('utf8', dataPosition, dataPosition + dataSize);
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
     * @api
     * @param {number} [after] The document position to start reading from.
     * @returns {Generator<string>} A generator that returns all documents in this partition.
     */
    *readAll(after = 0) {
        let position = after < 0 ? this.size + after + 1 : after;
        let data;
        while ((data = this.readFrom(position)) !== false) {
            yield data;
            position += this.documentWriteSize(Buffer.byteLength(data, 'utf8'));
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

module.exports = ReadablePartition;
module.exports.CorruptFileError = CorruptFileError;
module.exports.InvalidDataSizeError = InvalidDataSizeError;
module.exports.HEADER_MAGIC = HEADER_MAGIC;
module.exports.DOCUMENT_SEPARATOR = DOCUMENT_SEPARATOR;
module.exports.DOCUMENT_ALIGNMENT = DOCUMENT_ALIGNMENT;
module.exports.DOCUMENT_HEADER_SIZE = DOCUMENT_HEADER_SIZE;
module.exports.DOCUMENT_FOOTER_SIZE = DOCUMENT_FOOTER_SIZE;