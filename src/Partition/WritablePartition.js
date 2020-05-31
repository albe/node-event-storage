const fs = require('fs');
const mkdirpSync = require('mkdirp').sync;
const ReadablePartition = require('./ReadablePartition');
const { assert, buildMetadataHeader } = require('../util');
const Clock = require('../Clock');

const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;
const DOCUMENT_HEADER_SIZE = 16;
const DOCUMENT_ALIGNMENT = 4;
const DOCUMENT_PAD = ' '.repeat(15) + "\n";

const NES_EPOCH = new Date('2020-01-01T00:00:00');

/**
 * @param {number} dataSize
 * @returns {string} The data padded to 16 bytes alignment and ended with a line break.
 */
function padData(dataSize) {
    const padSize = (DOCUMENT_ALIGNMENT - ((dataSize + 1) % DOCUMENT_ALIGNMENT)) % DOCUMENT_ALIGNMENT;
    return DOCUMENT_PAD.substr(-padSize - 1);
}

/**
 * A partition is a single file where the storage will write documents to depending on some partitioning rules.
 * In the case of an event store, this is most likely the (write) streams.
 */
class WritablePartition extends ReadablePartition {

    /**
     * @param {string} name The name of the partition.
     * @param {object} [config] An object with storage parameters.
     * @param {string} [config.dataDirectory] The path where the storage data should reside. Default '.'.
     * @param {number} [config.readBufferSize] Size of the read buffer in bytes. Default 4096.
     * @param {number} [config.writeBufferSize] Size of the write buffer in bytes. Default 16384.
     * @param {number} [config.maxWriteBufferDocuments] How many documents to have in the write buffer at max. 0 means as much as possible. Default 0.
     * @param {boolean} [config.syncOnFlush] If fsync should be called on write buffer flush. Set this if you need strict durability. Defaults to false.
     * @param {boolean} [config.dirtyReads] If dirty reads should be allowed. This means that writes that are in write buffer but not yet flushed can be read. Defaults to true.
     * @param {object} [config.metadata] A metadata object that will be written to the file header.
     * @param {typeof Clock} [config.clock] The constructor to a clock interface
     */
    constructor(name, config = {}) {
        let defaults = {
            writeBufferSize: DEFAULT_WRITE_BUFFER_SIZE,
            maxWriteBufferDocuments: 0,
            syncOnFlush: false,
            dirtyReads: true,
            metadata: {
                epoch: Date.now()
            },
            clock: Clock
        };
        config = Object.assign(defaults, config);
        super(name, config);
        if (!fs.existsSync(this.dataDirectory)) {
            mkdirpSync(this.dataDirectory);
        }
        this.fileMode = 'a+';
        this.writeBufferSize = config.writeBufferSize >>> 0; // jshint ignore:line
        this.maxWriteBufferDocuments = config.maxWriteBufferDocuments >>> 0; // jshint ignore:line
        this.syncOnFlush = !!config.syncOnFlush;
        this.dirtyReads = !!config.dirtyReads;
        this.metadata = config.metadata;
        assert(typeof(config.clock.prototype) === 'object' && typeof(config.clock.prototype.time) === 'function', 'Clock needs to implement the method time()');
        this.ClockConstructor = config.clock;
    }

    /**
     * Open the partition storage and create read and write buffers.
     *
     * @api
     * @returns {boolean}
     */
    open() {
        if (this.fd) {
            return true;
        }

        this.writeBuffer = Buffer.allocUnsafeSlow(this.writeBufferSize);
        // Where inside the write buffer the next write is added
        this.writeBufferCursor = 0;
        // How many documents are currently in the write buffer
        this.writeBufferDocuments = 0;
        this.flushCallbacks = [];

        if (super.open() === false) {
            const stat = fs.statSync(this.fileName);
            if (stat.size !== 0) {
                return false;
            }
            this.metadata.epoch = Date.now();
            this.writeMetadata();
            this.size = 0;
        }
        this.clock = new this.ClockConstructor(this.metadata.epoch || NES_EPOCH.getTime());

        return true;
    }

    /**
     * Close the partition and frees up all resources.
     *
     * @api
     * @returns void
     */
    close() {
        if (this.fd) {
            this.flush();
            fs.fsyncSync(this.fd);
        }
        if (this.writeBuffer) {
            this.writeBuffer = null;
            this.writeBufferCursor = 0;
            this.writeBufferDocuments = 0;
        }
        super.close();
    }

    /**
     * Write the header and metadata to the file.
     *
     * @private
     * @returns void
     */
    writeMetadata() {
        const metadataBuffer = buildMetadataHeader(ReadablePartition.HEADER_MAGIC, this.metadata);
        fs.writeSync(this.fd, metadataBuffer, 0, metadataBuffer.byteLength, 0);
        this.headerSize = metadataBuffer.byteLength;
    }

    /**
     * Flush the write buffer to disk.
     * This is a sync method and will invoke all previously registered flush callbacks.
     *
     * @returns {boolean}
     */
    flush() {
        if (!this.fd) {
            return false;
        }
        if (this.writeBufferCursor === 0) {
            return false;
        }

        fs.writeSync(this.fd, this.writeBuffer, 0, this.writeBufferCursor);
        if (this.syncOnFlush) {
            fs.fsyncSync(this.fd);
        }

        this.writeBufferCursor = 0;
        this.writeBufferDocuments = 0;
        this.flushCallbacks.forEach(callback => callback());
        this.flushCallbacks = [];

        return true;
    }

    /**
     * @private
     * @param {number} dataSize The size of the data that needs to go into the write buffer next.
     */
    flushIfWriteBufferTooSmall(dataSize) {
        if (this.writeBufferCursor > 0 && dataSize + this.writeBufferCursor > this.writeBuffer.byteLength) {
            this.flush();
        }
        if (this.writeBufferCursor === 0) {
            process.nextTick(() => this.flush());
        }
    }

    /**
     * @private
     */
    flushIfWriteBufferDocumentsFull() {
        if (this.maxWriteBufferDocuments > 0 && this.writeBufferDocuments >= this.maxWriteBufferDocuments) {
            this.flush();
        }
    }

    /**
     * @private
     * @param {Buffer} buffer The buffer to write the document header to
     * @param {number} offset The offset inside the buffer to start writing at
     * @param {number} dataSize The size of the document
     * @param {number|null} [sequenceNumber] The external sequence number for this document
     * @param {number|null} [time64] The number of microseconds relative to the partition base since the creation of the document
     * @returns {number} The size of the document header
     */
    writeDocumentHeader(buffer, offset, dataSize, sequenceNumber = null, time64 = null) {
        if (sequenceNumber === null) {
            sequenceNumber = 0;
        }
        if (time64 === null) {
            time64 = this.clock.time();
        }
        if (time64 < 0) {
            throw new Error('Time may not be negative!');
        }
        buffer.writeUInt32BE(dataSize, offset);
        buffer.writeUInt32BE(sequenceNumber, offset + 4);
        buffer.writeDoubleBE(time64, offset + 8);
        return DOCUMENT_HEADER_SIZE;
    }

    /**
     * Write the given data to the partition without buffering.
     * @private
     * @param {string} data The (padded) data to write to storage.
     * @param {number} dataSize The size of the raw document without padding.
     * @param {number} [sequenceNumber] The external sequence number to store with the document.
     * @param {function} [callback] A function that will be called when the document is written to disk.
     * @returns {number} Number of bytes written.
     */
    writeUnbuffered(data, dataSize, sequenceNumber, callback) {
        this.flush();
        const dataHeader = Buffer.alloc(DOCUMENT_HEADER_SIZE);
        this.writeDocumentHeader(dataHeader, 0, dataSize, sequenceNumber);

        let bytesWritten = 0;
        bytesWritten += fs.writeSync(this.fd, dataHeader);
        bytesWritten += fs.writeSync(this.fd, data);
        if (typeof callback === 'function') {
            process.nextTick(callback);
        }
        return bytesWritten;
    }

    /**
     * Write the given data to the partition with buffering. Will flush the write buffer if it is necessary.
     * @private
     * @param {string} data The (padded) data to write to storage.
     * @param {number} dataSize The size of the raw document without padding.
     * @param {number} [sequenceNumber] The external sequence number to store with the document.
     * @param {function} [callback] A function that will be called when the document is written to disk.
     * @returns {number} Number of bytes written.
     */
    writeBuffered(data, dataSize, sequenceNumber, callback) {
        const bytesToWrite = Buffer.byteLength(data, 'utf8') + DOCUMENT_HEADER_SIZE;
        this.flushIfWriteBufferTooSmall(bytesToWrite);

        let bytesWritten = 0;
        bytesWritten += this.writeDocumentHeader(this.writeBuffer, this.writeBufferCursor, dataSize, sequenceNumber);
        bytesWritten += this.writeBuffer.write(data, this.writeBufferCursor + bytesWritten, 'utf8');
        this.writeBufferCursor += bytesWritten;
        this.writeBufferDocuments++;
        if (typeof callback === 'function') {
            this.flushCallbacks.push(callback);
        }
        this.flushIfWriteBufferDocumentsFull();
        return bytesWritten;
    }

    /**
     * @api
     * @param {string} data The data to write to storage.
     * @param {number} [sequenceNumber] The external sequence number to store with the document.
     * @param {function} [callback] A function that will be called when the document is written to disk.
     * @returns {number|boolean} The file position at which the data was written or false on error.
     */
    write(data, sequenceNumber, callback) {
        if (!this.fd) {
            return false;
        }
        if (typeof sequenceNumber === 'function') {
            callback = sequenceNumber;
            sequenceNumber = null;
        }
        const dataSize = Buffer.byteLength(data, 'utf8');
        assert(dataSize <= 64 * 1024 * 1024, 'Document is too large! Maximum is 64 MB');

        data += padData(dataSize);
        const dataPosition = this.size;
        if (dataSize + DOCUMENT_HEADER_SIZE >= this.writeBuffer.byteLength * 4 / 5) {
            this.size += this.writeUnbuffered(data, dataSize, sequenceNumber, callback);
        } else {
            this.size += this.writeBuffered(data, dataSize, sequenceNumber, callback);
        }
        assert(this.size >= dataPosition + dataSize + DOCUMENT_HEADER_SIZE, `Error while writing document at position ${dataPosition}.`);
        return dataPosition;
    }

    /**
     * Prepare the read buffer for reading from the specified position.
     *
     * @protected
     * @param {number} position The position in the file to prepare the read buffer for reading from.
     * @returns {object} A reader object with properties `buffer`, `cursor` and `length`.
     */
    prepareReadBuffer(position) {
        if (position + DOCUMENT_HEADER_SIZE >= this.size) {
            return { buffer: null, cursor: 0, length: 0 };
        }
        const bufferPos = this.size - this.writeBufferCursor;
        // Handle the case when data that is still in write buffer is supposed to be read
        if (this.dirtyReads && this.writeBufferCursor > 0 && position >= bufferPos) {
            return { buffer: this.writeBuffer, cursor: position - bufferPos, length: this.writeBufferCursor };
        }
        return super.prepareReadBuffer(position);
    }

    /**
     * Truncate the internal read buffer after the given position.
     * @param {number} after The byte position to truncate the read buffer after.
     */
    truncateReadBuffer(after) {
        if (this.readBufferPos >= after) {
            this.readBufferPos = -1;
            this.readBufferLength = 0;
        } else if (this.readBufferPos + this.readBufferLength > after) {
            this.readBufferLength -= (this.readBufferPos + this.readBufferLength) - after;
        }
    }

    /**
     * Truncate the partition storage at the given position.
     *
     * @param {number} after The file position after which to truncate the partition.
     */
    truncate(after) {
        if (after > this.size) {
            return;
        }
        if (after < 0) {
            after = 0;
        }
        this.flush();

        let position = after, data;
        try {
            data = this.readFrom(position);
        } catch (e) {
            throw new Error('Can only truncate on valid document boundaries.');
        }
        // copy all truncated documents to some delete log
        const deletedBranch = new WritablePartition(this.name + '-' + after + '.branch', { dataDirectory: this.dataDirectory });
        deletedBranch.open();
        while (data) {
            deletedBranch.write(data);
            position += this.documentWriteSize(Buffer.byteLength(data, 'utf8'));
            data = this.readFrom(position);
        }
        deletedBranch.close();

        fs.truncateSync(this.fileName, this.headerSize + after);
        this.truncateReadBuffer(after);
        this.size = after;
    }
}

module.exports = WritablePartition;
module.exports.CorruptFileError = ReadablePartition.CorruptFileError;