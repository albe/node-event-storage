const fs = require('fs');
const mkdirpSync = require('mkdirp').sync;
const ReadablePartition = require('./ReadablePartition');
const { buildMetadataHeader } = require('../util');

const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;

/**
 * @param {string} str
 * @param {number} len
 * @param {string} [char]
 * @returns {string}
 */
function pad(str, len, char = ' ') {
    return len > str.length ? char.repeat(len - str.length) + str : str;
}

/**
 * A partition is a single file where the storage will write documents to depending on some partitioning rules.
 * In the case of an event store, this is most likely the (write) streams.
 */
class WritablePartition extends ReadablePartition {

    /**
     * @param {string} name The name of the partition.
     * @param {Object} [config] An object with storage parameters.
     * @param {string} [config.dataDirectory] The path where the storage data should reside. Default '.'.
     * @param {number} [config.readBufferSize] Size of the read buffer in bytes. Default 4096.
     * @param {number} [config.writeBufferSize] Size of the write buffer in bytes. Default 16384.
     * @param {number} [config.maxWriteBufferDocuments] How many documents to have in the write buffer at max. 0 means as much as possible. Default 0.
     * @param {boolean} [config.syncOnFlush] If fsync should be called on write buffer flush. Set this if you need strict durability. Defaults to false.
     * @param {boolean} [config.dirtyReads] If dirty reads should be allowed. This means that writes that are in write buffer but not yet flushed can be read. Defaults to true.
     * @param {Object} [config.metadata] A metadata object that will be written to the file header.
     */
    constructor(name, config = {}) {
        let defaults = {
            writeBufferSize: DEFAULT_WRITE_BUFFER_SIZE,
            maxWriteBufferDocuments: 0,
            syncOnFlush: false,
            dirtyReads: true,
            metadata: {}
        };
        config = Object.assign(defaults, config);
        super(name, config);
        if (!fs.existsSync(this.dataDirectory)) {
            mkdirpSync(this.dataDirectory);
        }
        this.fileMode = 'a+';
        this.writeBufferSize = config.writeBufferSize >>> 0;
        this.maxWriteBufferDocuments = config.maxWriteBufferDocuments >>> 0;
        this.syncOnFlush = !!config.syncOnFlush;
        this.dirtyReads = !!config.dirtyReads;
        this.metadata = config.metadata;
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
            if (stat.size === 0) {
                this.writeMetadata();
                this.size = 0;
            } else {
                return false;
            }
        }

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
     * @api
     * @param {string} data The data to write to storage.
     * @param {function} [callback] A function that will be called when the document is written to disk.
     * @returns {number|boolean} The file position at which the data was written or false on error.
     */
    write(data, callback) {
        if (!this.fd) {
            return false;
        }
        let dataSize = Buffer.byteLength(data, 'utf8');
        const dataToWrite = pad(dataSize.toString(), 10) + data.toString() + "\n";
        dataSize += 11;

        this.flushIfWriteBufferTooSmall(dataSize);
        if (dataSize > this.writeBuffer.byteLength) {
            //console.log('unbuffered write!');
            fs.writeSync(this.fd, dataToWrite);
            if (typeof callback === 'function') {
                process.nextTick(callback);
            }
        } else {
            this.writeBufferCursor += this.writeBuffer.write(dataToWrite, this.writeBufferCursor, dataSize, 'utf8');
            this.writeBufferDocuments++;
            if (typeof callback === 'function') {
                this.flushCallbacks.push(callback);
            }
            this.flushIfWriteBufferDocumentsFull();
        }
        const dataPosition = this.size;
        this.size += dataSize;
        return dataPosition;
    }

    /**
     * Prepare the read buffer for reading from the specified position.
     *
     * @protected
     * @param {number} position The position in the file to prepare the read buffer for reading from.
     * @returns {Object} A reader object with properties `buffer`, `cursor` and `length`.
     */
    prepareReadBuffer(position) {
        if (position + 10 >= this.size) {
            return { buffer: null, cursor: 0, length: 0 };
        }
        let bufferPos = this.size - this.writeBufferCursor;
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
            position += Buffer.byteLength(data, 'utf8') + 11;
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