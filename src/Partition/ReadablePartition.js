const fs = require('fs');
const mkdirpSync = require('mkdirp').sync;
const path = require('path');
const EventEmitter = require('events');

const DEFAULT_READ_BUFFER_SIZE = 4 * 1024;

// node-event-store partition V01
const HEADER_MAGIC = "nesprt01";

class CorruptFileError extends Error {}
class InvalidDataSizeError extends Error {}

/**
 * Method for hashing a string (partition name) to a 32-bit unsigned integer.
 *
 * @param {string} str
 * @returns {number}
 */
function hash(str) {
    if (str.length === 0) return 0;
    let hash = 5381,
        i    = str.length;

    while(i) {
        hash = ((hash << 5) + hash) ^ str.charCodeAt(--i);
    }

    /* JavaScript does bitwise operations (like XOR, above) on 32-bit signed
     * integers. Since we want the results to be always positive, convert the
     * signed int to an unsigned by doing an unsigned bitshift. */
    return hash >>> 0;
}

/**
 * A partition is a single file where the storage will write documents to depending on some partitioning rules.
 * In the case of an event store, this is most likely the (write) streams.
 */
class ReadablePartition extends EventEmitter {

    /**
     * Get the id for a specific partition name.
     *
     * @param {string} name
     * @returns {number}
     */
    static id(name) {
        return hash(name);
    }

    /**
     * @param {string} name The name of the partition.
     * @param {Object} [config] An object with storage parameters.
     * @param {string} [config.dataDirectory] The path where the storage data should reside. Default '.'.
     * @param {number} [config.readBufferSize] Size of the read buffer in bytes. Default 4096.
     */
    constructor(name, config = {}) {
        super();
        if (!name || typeof name !== 'string') {
            throw new Error('Must specify a partition name.');
        }

        let defaults = {
            dataDirectory: '.',
            readBufferSize: DEFAULT_READ_BUFFER_SIZE
        };
        config = Object.assign(defaults, config);
        this.dataDirectory = path.resolve(config.dataDirectory);
        if (!fs.existsSync(this.dataDirectory)) {
            mkdirpSync(this.dataDirectory);
        }

        this.name = name;
        this.id = hash(name);
        this.fileName = path.resolve(this.dataDirectory, this.name);
        this.fileMode = 'r';

        this.readBufferSize = config.readBufferSize >>> 0;
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
        this.readBuffer = Buffer.allocUnsafeSlow(10 + this.readBufferSize);
        // Where inside the file the read buffer starts
        this.readBufferPos = -1;
        this.readBufferLength = 0;

        const stat = fs.statSync(this.fileName);
        this.headerSize = HEADER_MAGIC.length + 1;
        if (stat.size === 0) {
            return false;
        }

        this.size = this.readFileSize();

        return true;
    }

    /**
     * @private
     * @returns {number} The file size not including the file header.
     */
    readFileSize() {
        const stat = fs.statSync(this.fileName);
        const headerBuffer = Buffer.allocUnsafe(HEADER_MAGIC.length);
        fs.readSync(this.fd, headerBuffer, 0, HEADER_MAGIC.length, 0);
        if (headerBuffer.toString() !== HEADER_MAGIC) {
            this.close();
            if (headerBuffer.toString().substr(0, 6) === HEADER_MAGIC.substr(0, 6)) {
                throw new Error(`Invalid file version. The partition ${this.name} was created with a different library version.`);
            }
            throw new Error(`Invalid file header in partition ${this.name}.`);
        }
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
        if (!this.fd) {
            return;
        }
        this.readBufferLength = fs.readSync(this.fd, this.readBuffer, 0, this.readBuffer.byteLength, this.headerSize + from);
        this.readBufferPos = from;
    }

    /**
     * @private
     * @param {Buffer} buffer The buffer to read the data length from.
     * @param {number} offset The position inside the buffer to start reading from.
     * @param {number} position The file position to start reading from.
     * @param {number} [size] The expected byte size of the document at the given position.
     * @returns {number} The length of the document at the given position.
     * @throws {Error} if the storage entry at the given position is corrupted.
     * @throws {InvalidDataSizeError} if the document size at the given position does not match the provided size.
     * @throws {CorruptFileError} if the document at the given position can not be read completely.
     */
    readDataLength(buffer, offset, position, size) {
        const dataLengthStr = buffer.toString('utf8', offset, offset + 10);
        const dataLength = parseInt(dataLengthStr, 10);
        if (!dataLength || isNaN(dataLength) || !/^\s+[0-9]+$/.test(dataLengthStr)) {
            throw new Error(`Error reading document size from ${position}, got ${dataLength}.`);
        }
        if (size && dataLength !== size) {
            throw new InvalidDataSizeError(`Invalid document size ${dataLength} at position ${position}, expected ${size}.`);
        }

        if (position + dataLength + 11 > this.size) {
            throw new CorruptFileError(`Invalid document at position ${position}. This may be caused by an unfinished write.`);
        }

        return dataLength;
    }

    /**
     * Prepare the read buffer for reading from the specified position.
     *
     * @protected
     * @param {number} position The position in the file to prepare the read buffer for reading from.
     * @returns {Object} A reader object with properties `buffer`, `cursor` and `length`.
     */
    prepareReadBuffer(position) {
        let buffer = this.readBuffer;
        let bufferPos = this.readBufferPos;
        let bufferLength = this.readBufferLength;
        let bufferCursor = position - bufferPos;
        if (bufferPos < 0 || bufferCursor < 0 || bufferCursor + 10 > bufferLength) {
            this.fillBuffer(position);
            bufferCursor = 0;
            buffer = this.readBuffer;
            bufferLength = this.readBufferLength;
        }
        return { buffer, cursor: bufferCursor, length: bufferLength };
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
        if (!this.fd) {
            return false;
        }
        if (position + 10 >= this.size) {
            return false;
        }
        const reader = this.prepareReadBuffer(position);

        if (reader.length < size + 10) {
            return false;
        }

        let dataPosition = reader.cursor + 10;
        const dataLength = this.readDataLength(reader.buffer, reader.cursor, position, size);

        if (dataLength + 10 > reader.buffer.byteLength) {
            //console.log('sync read for large document size', dataLength, 'at position', position);
            const tempReadBuffer = Buffer.allocUnsafe(dataLength);
            fs.readSync(this.fd, tempReadBuffer, 0, dataLength, this.headerSize + position + 10);
            return tempReadBuffer.toString('utf8');
        }

        if (reader.cursor > 0 && dataPosition + dataLength > reader.length) {
            this.fillBuffer(position);
            dataPosition = 10;
        }

        return reader.buffer.toString('utf8', dataPosition, dataPosition + dataLength);
    }

    /**
     * @api
     * @return {Generator} A generator that returns all documents in this partition.
     */
    *readAll() {
        let position = 0;
        let data;
        while ((data = this.readFrom(position)) !== false) {
            yield data;
            position += Buffer.byteLength(data, 'utf8') + 11;
        }
    }

}

module.exports = ReadablePartition;
module.exports.CorruptFileError = CorruptFileError;
module.exports.InvalidDataSizeError = InvalidDataSizeError;
module.exports.HEADER_MAGIC = HEADER_MAGIC;