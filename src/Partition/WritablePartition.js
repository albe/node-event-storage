import fs from 'fs';
import ReadablePartition, { CorruptFileError, HEADER_MAGIC, DOCUMENT_ALIGNMENT, DOCUMENT_SEPARATOR, DOCUMENT_HEADER_SIZE, DOCUMENT_FOOTER_SIZE } from './ReadablePartition.js';
import { assert, alignTo } from '../utils/util.js';
import { buildMetadataHeader } from '../utils/metadataUtil.js';
import { ensureDirectory } from '../utils/fsUtil.js';
import Clock from '../Clock.js';

const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;
const DOCUMENT_PAD = ' '.repeat(DOCUMENT_ALIGNMENT);

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
        config.metadata = Object.assign(defaults.metadata, config.metadata);
        config = Object.assign(defaults, config);
        super(name, config);
        ensureDirectory(this.dataDirectory);
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
        if (this.opened) {
            return true;
        }

        let success = super.open();
        if (!success) {
            if (this.size + this.headerSize !== 0) {
                // If file is not empty, we can not open and initialize it
                return false;
            }
            this.writeMetadata();
            success = super.open();
        }

        this.writeBuffer = success && Buffer.allocUnsafeSlow(this.writeBufferSize);
        // Where inside the write buffer the next write is added
        this.writeBufferCursor = 0;
        // How many documents are currently in the write buffer
        this.writeBufferDocuments = 0;
        this.flushCallbacks = [];
        // Pre-allocated buffer for document header (16 bytes) + size footer (4 bytes) used in writeUnbuffered
        this.writeMetaBuffer = Buffer.allocUnsafe(DOCUMENT_HEADER_SIZE + 4);

        this.clock = new this.ClockConstructor(this.metadata.epoch);

        return success;
    }

    /**
     * Close the partition and frees up all resources.
     *
     * @api
     * @returns void
     */
    close() {
        super.close();
        if (this.writeBuffer) {
            this.writeBuffer = null;
            this.writeBufferCursor = 0;
            this.writeBufferDocuments = 0;
            this.writeMetaBuffer = null;
        }
    }

    getFileHandle() {
        return this.fileHandlePool.get(this, (fd) => this.beforeFileHandleClose(fd));
    }

    beforeFileHandleClose(fd) {
        if (!this.writeBuffer) {
            return;
        }
        this.flush();
        fs.fsyncSync(fd);
    }

    /**
     * Write the header and metadata to the file.
     *
     * @private
     * @returns void
     */
    writeMetadata() {
        const metadataBuffer = buildMetadataHeader(HEADER_MAGIC, this.metadata);
        fs.writeFileSync(this.fileName, metadataBuffer);
        this.headerSize = metadataBuffer.byteLength;
    }

    /**
     * Flush the write buffer to disk.
     * This is a sync method and will invoke all previously registered flush callbacks.
     *
     * @returns {boolean}
     */
    flush() {
        if (!this.isOpen()) {
            return false;
        }
        if (this.writeBufferCursor === 0) {
            return false;
        }

        const fd = this.getFileHandle();
        fs.writeSync(fd, this.writeBuffer, 0, this.writeBufferCursor);
        if (this.syncOnFlush) {
            fs.fsyncSync(fd);
        }

        this.writeBufferCursor = 0;
        this.writeBufferDocuments = 0;
        const callbacks = this.flushCallbacks;
        this.flushCallbacks = [];
        for (let i = 0; i < callbacks.length; i++) callbacks[i]();

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
        ({ sequenceNumber, time64 } = this.normalizeWriteMetadata(sequenceNumber, time64));
        /* c8 ignore next */
        assert(time64 >= 0, 'Time may not be negative!');

        buffer.writeUInt32BE(dataSize, offset);
        buffer.writeUInt32BE(sequenceNumber, offset + 4);
        buffer.writeDoubleBE(time64, offset + 8);
        return DOCUMENT_HEADER_SIZE;
    }

    normalizeWriteMetadata(sequenceNumber, time64) {
        return {
            sequenceNumber: sequenceNumber === null ? 0 : sequenceNumber,
            time64: time64 === null ? this.clock.time() : time64
        };
    }

    normalizeWriteArguments(sequenceNumber, callback) {
        if (typeof sequenceNumber === 'function') {
            return { sequenceNumber: null, callback: sequenceNumber };
        }
        return { sequenceNumber, callback };
    }

    shouldWriteUnbuffered(dataSize) {
        return dataSize + DOCUMENT_HEADER_SIZE >= this.writeBuffer.byteLength * 4 / 5;
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
        this.writeDocumentHeader(this.writeMetaBuffer, 0, dataSize, sequenceNumber);
        const fd = this.getFileHandle();

        let bytesWritten = 0;
        bytesWritten += fs.writeSync(fd, this.writeMetaBuffer, 0, DOCUMENT_HEADER_SIZE);
        bytesWritten += fs.writeSync(fd, data);
        const padSize = alignTo(dataSize + DOCUMENT_FOOTER_SIZE, DOCUMENT_ALIGNMENT);
        bytesWritten += fs.writeSync(fd, DOCUMENT_PAD.substring(0, padSize));
        this.writeMetaBuffer.writeUInt32BE(dataSize, 0);
        bytesWritten += fs.writeSync(fd, this.writeMetaBuffer, 0, 4);
        bytesWritten += fs.writeSync(fd, DOCUMENT_SEPARATOR);
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
        const bytesToWrite = this.documentWriteSize(dataSize);
        this.flushIfWriteBufferTooSmall(bytesToWrite);

        let bytesWritten = 0;
        bytesWritten += this.writeDocumentHeader(this.writeBuffer, this.writeBufferCursor, dataSize, sequenceNumber);
        bytesWritten += this.writeBuffer.write(data, this.writeBufferCursor + bytesWritten, 'utf8');
        const padSize = alignTo(dataSize + DOCUMENT_FOOTER_SIZE, DOCUMENT_ALIGNMENT);
        bytesWritten += this.writeBuffer.write(DOCUMENT_PAD.substring(0, padSize), this.writeBufferCursor + bytesWritten, 'utf8');
        this.writeBuffer.writeUInt32BE(dataSize, this.writeBufferCursor + bytesWritten);
        bytesWritten += 4;
        bytesWritten += this.writeBuffer.write(DOCUMENT_SEPARATOR, this.writeBufferCursor + bytesWritten, 'utf8');
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
        this.getFileHandle();
        ({ sequenceNumber, callback } = this.normalizeWriteArguments(sequenceNumber, callback));
        const dataSize = Buffer.byteLength(data, 'utf8');
        assert(dataSize <= 64 * 1024 * 1024, 'Document is too large! Maximum is 64 MB');

        const dataPosition = this.size;
        if (this.shouldWriteUnbuffered(dataSize)) {
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
     * Prepare the read buffer for reading *before* the specified position. Don't try to read *after* the returned cursor.
     *
     * @protected
     * @param {number} position The position in the file to prepare the read buffer for reading before.
     * @param {number} [size] The amount of bytes that need to be buffered before position. By default, only guarantees that the document footer can be read.
     * @returns {object} A reader object with properties `buffer`, `cursor` and `length`.
     */
    prepareReadBufferBackwards(position, size = 0) {
        const bufferPos = this.size - this.writeBufferCursor;
        // Handle the case when data that is still in write buffer is supposed to be read backwards
        if (this.dirtyReads && this.writeBufferCursor > 0 && position > bufferPos) {
            return { buffer: this.writeBuffer, cursor: position - bufferPos, length: this.writeBufferCursor };
        }
        return super.prepareReadBufferBackwards(position, size);
    }

    /**
     * Read all documents in reverse write order, ignoring any unflushed write-buffer data when
     * dirty reads are disabled.
     *
     * @api
     * @param {number} [before] The document position to start reading backward from.
     * @param {object|null} [headerOut] Optional object to populate with document header fields on each yield.
     * @returns {Generator<Buffer>} A generator that returns all documents in this partition in reverse order.
     */
    *readAllBackwards(before = -1, headerOut = null) {
        if (!this.dirtyReads && this.writeBufferCursor > 0) {
            const flushedSize = this.size - this.writeBufferCursor;
            const clampedBefore = before < 0 ? flushedSize : Math.min(before, flushedSize);
            yield* super.readAllBackwards(clampedBefore, headerOut);
            return;
        }
        yield* super.readAllBackwards(before, headerOut);
    }

    /**
     *
     * @internal
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
     * Truncate the partition, removing all documents with sequenceNumber > after.
     * Scans the partition file backwards to find the cutoff position.
     *
     * @api
     * @param {number} after Keep all documents with sequenceNumber <= after. Documents with
     *   sequenceNumber > after (and any torn write at the end) are removed.
     */
    truncateAfterSequence(after) {
        let position = this.size;
        let truncateAt = this.size; // default: nothing to truncate
        while ((position = this.findDocumentPositionBefore(position)) !== false) {
            const reader = this.prepareReadBufferBackwards(position);
            if (!reader.buffer) break;
            const { sequenceNumber } = this.readDocumentHeader(reader.buffer, reader.cursor, position);
            if (sequenceNumber > after) {
                // This document must be removed; record its start as the new cutoff.
                truncateAt = position;
            } else {
                break;
            }
        }
        this.truncate(truncateAt);
    }

    /**
     * Truncate the partition storage at the given position.
     *
     * @api
     * @param {number} after The file position after which to truncate the partition.
     */
    truncate(after) {
        if (after >= this.size) {
            return;
        }
        this.open();
        after = Math.max(0, after);
        this.flush();

        // Always save the truncated part for manual recovery, even if it contains corrupted data
        this.branchOff('truncated-' + Date.now(), after);

        try {
            this.readFrom(after);
        } catch (e) {
            assert(e instanceof CorruptFileError, 'Can only truncate on valid document boundaries.');
        }

        fs.truncateSync(this.fileName, this.headerSize + after);
        this.truncateReadBuffer(after);
        this.size = after;
        this.emit('truncated', after);
    }

    /**
     * Create a branch of this partition starting from the given position.
     *
     * @internal
     * @param {string} branchName The name that identifies the branch (will be prefixed with this partition name and suffixed with the position)
     * @param {number} position The file position from where to branch off
     * @returns {WritablePartition} The branched off partition
     */
    branchOff(branchName, position) {
        const deletedBranch = new WritablePartition(this.name + '-' + branchName + '-' + position + '.branch', { dataDirectory: this.dataDirectory, metadata: { epoch: this.metadata.epoch } });
        deletedBranch.open();
        const branchFd = deletedBranch.getFileHandle();
        do {
            const reader = this.prepareReadBuffer(position);
            fs.writeSync(branchFd, reader.buffer, reader.cursor, reader.length);
            position += reader.length;
        } while (position < this.size);
        deletedBranch.close();
        return deletedBranch;
    }
}

export default WritablePartition;
export { CorruptFileError };