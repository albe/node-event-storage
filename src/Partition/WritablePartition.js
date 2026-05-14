import fs from 'fs';
import { createRequire } from 'module';
import ReadablePartition, { CorruptFileError, HEADER_MAGIC, DOCUMENT_ALIGNMENT, DOCUMENT_SEPARATOR, DOCUMENT_HEADER_SIZE, DOCUMENT_FOOTER_SIZE } from './ReadablePartition.js';
import { assert, alignTo } from '../util.js';
import { buildMetadataHeader } from '../metadataUtil.js';
import { ensureDirectory } from '../fsUtil.js';
import Clock from '../Clock.js';

const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;
const DOCUMENT_PAD = ' '.repeat(DOCUMENT_ALIGNMENT);
const DOCUMENT_SEPARATOR_BUFFER = Buffer.from(DOCUMENT_SEPARATOR, 'ascii');
const require = createRequire(import.meta.url);

let mmapIo = null;
function loadMmapIo() {
    if (mmapIo !== null) {
        return mmapIo;
    }
    try {
        mmapIo = require('@riaskov/mmap-io');
    } catch (primaryError) {
        try {
            mmapIo = require('mmap-io');
        } catch (fallbackError) {
            mmapIo = false;
        }
    }
    return mmapIo;
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
     * @param {boolean} [config.mmapWriteBuffer] If true, allocate the write buffer with mmap.
     * @param {object} [config.metadata] A metadata object that will be written to the file header.
     * @param {typeof Clock} [config.clock] The constructor to a clock interface
     */
    constructor(name, config = {}) {
        let defaults = {
            writeBufferSize: DEFAULT_WRITE_BUFFER_SIZE,
            maxWriteBufferDocuments: 0,
            syncOnFlush: false,
            dirtyReads: true,
            mmapWriteBuffer: false,
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
        this.mmapWriteBuffer = !!config.mmapWriteBuffer;
        this.metadata = config.metadata;
        this.writeBuffer = null;
        this.writeBufferMapping = null;
        this.writeBufferMapDelta = 0;
        this.writeBufferCursor = 0;
        this.writeBufferDocuments = 0;
        this.flushCallbacks = [];
        this.writeMetaBuffer = null;
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

        let success = super.open();
        if (!success) {
            if (this.size + this.headerSize !== 0) {
                // If file is not empty, we can not open and initialize it
                return false;
            }
            this.writeMetadata();
            success = super.open();
        }

        this.writeBuffer = success && (!this.mmapWriteBuffer ? this.createWriteBuffer(this.size) : null);
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

    createWriteBuffer(filePosition = this.size) {
        if (!this.mmapWriteBuffer) {
            return Buffer.allocUnsafeSlow(this.writeBufferSize);
        }
        const mmap = loadMmapIo();
        assert(mmap, 'An mmap package is required for mmapWriteBuffer.');
        const pageSize = mmap.PAGESIZE || 4096;
        const fileOffset = this.headerSize + filePosition;
        const mapOffset = fileOffset - (fileOffset % pageSize);
        const mapDelta = fileOffset - mapOffset;
        const mapLength = mapDelta + this.writeBufferSize;
        fs.ftruncateSync(this.fd, fileOffset + this.writeBufferSize);
        this.writeBufferMapping = mmap.map(
            mapLength,
            mmap.PROT_READ | mmap.PROT_WRITE,
            mmap.MAP_SHARED,
            this.fd,
            mapOffset
        );
        this.writeBufferMapDelta = mapDelta;
        return this.writeBufferMapping.subarray(mapDelta, mapDelta + this.writeBufferSize);
    }

    ensureWriteBuffer() {
        if (!this.writeBuffer) {
            this.writeBuffer = this.createWriteBuffer(this.size - this.writeBufferCursor);
        }
    }

    getWriteBufferCapacity() {
        return this.writeBuffer ? this.writeBuffer.byteLength : this.writeBufferSize;
    }

    /**
     * Close the partition and frees up all resources.
     *
     * @api
     * @returns void
     */
    close() {
        if (this.fd) {
            if (this.writeBufferCursor > 0) {
                this.flush();
            }
            fs.fsyncSync(this.fd);

            this.writeBuffer = null;
            this.writeBufferMapping = null;
            this.writeBufferMapDelta = 0;
            this.writeBufferCursor = 0;
            this.writeBufferDocuments = 0;
            this.writeMetaBuffer = null;
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
        if (!this.fd) {
            return false;
        }
        if (!this.writeBuffer || this.writeBufferCursor === 0) {
            return false;
        }

        if (this.mmapWriteBuffer) {
            const mmap = loadMmapIo();
            mmap.sync(this.writeBufferMapping, this.syncOnFlush);
            fs.ftruncateSync(this.fd, this.headerSize + this.size);
            this.writeBuffer = null;
            this.writeBufferMapping = null;
            this.writeBufferMapDelta = 0;
        } else {
            fs.writeSync(this.fd, this.writeBuffer, 0, this.writeBufferCursor);
        }
        if (this.syncOnFlush) {
            fs.fsyncSync(this.fd);
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
        if (sequenceNumber === null) {
            sequenceNumber = 0;
        }
        if (time64 === null) {
            time64 = this.clock.time();
        }
        /* istanbul ignore if */
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
        this.writeDocumentHeader(this.writeMetaBuffer, 0, dataSize, sequenceNumber);

        let bytesWritten = 0;
        bytesWritten += fs.writeSync(this.fd, this.writeMetaBuffer, 0, DOCUMENT_HEADER_SIZE);
        if (Buffer.isBuffer(data)) {
            bytesWritten += fs.writeSync(this.fd, data, 0, dataSize);
        } else {
            bytesWritten += fs.writeSync(this.fd, data);
        }
        const padSize = alignTo(dataSize + DOCUMENT_FOOTER_SIZE, DOCUMENT_ALIGNMENT);
        bytesWritten += fs.writeSync(this.fd, DOCUMENT_PAD.substr(0, padSize));
        this.writeMetaBuffer.writeUInt32BE(dataSize, 0);
        bytesWritten += fs.writeSync(this.fd, this.writeMetaBuffer, 0, 4);
        bytesWritten += fs.writeSync(this.fd, DOCUMENT_SEPARATOR);
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
        this.ensureWriteBuffer();
        const bytesToWrite = this.documentWriteSize(dataSize);
        this.flushIfWriteBufferTooSmall(bytesToWrite);

        let bytesWritten = 0;
        bytesWritten += this.writeDocumentHeader(this.writeBuffer, this.writeBufferCursor, dataSize, sequenceNumber);
        if (Buffer.isBuffer(data)) {
            data.copy(this.writeBuffer, this.writeBufferCursor + bytesWritten, 0, dataSize);
            bytesWritten += dataSize;
        } else {
            bytesWritten += this.writeBuffer.write(data, this.writeBufferCursor + bytesWritten, 'utf8');
        }
        const padSize = alignTo(dataSize + DOCUMENT_FOOTER_SIZE, DOCUMENT_ALIGNMENT);
        this.writeBuffer.fill(0x20, this.writeBufferCursor + bytesWritten, this.writeBufferCursor + bytesWritten + padSize);
        bytesWritten += padSize;
        this.writeBuffer.writeUInt32BE(dataSize, this.writeBufferCursor + bytesWritten);
        bytesWritten += 4;
        DOCUMENT_SEPARATOR_BUFFER.copy(this.writeBuffer, this.writeBufferCursor + bytesWritten);
        bytesWritten += DOCUMENT_SEPARATOR_BUFFER.byteLength;
        this.writeBufferCursor += bytesWritten;
        this.writeBufferDocuments++;
        if (typeof callback === 'function') {
            this.flushCallbacks.push(callback);
        }
        this.flushIfWriteBufferDocumentsFull();
        return bytesWritten;
    }

    writeBufferedSerialized(dataSize, serializeToBuffer, sequenceNumber, callback) {
        this.ensureWriteBuffer();
        const bytesToWrite = this.documentWriteSize(dataSize);
        this.flushIfWriteBufferTooSmall(bytesToWrite);

        let bytesWritten = 0;
        bytesWritten += this.writeDocumentHeader(this.writeBuffer, this.writeBufferCursor, dataSize, sequenceNumber);
        const payloadOffset = this.writeBufferCursor + bytesWritten;
        const serializedSize = serializeToBuffer(this.writeBuffer, payloadOffset);
        if (serializedSize !== undefined) {
            assert(serializedSize === dataSize, `Serialized size mismatch: expected ${dataSize}, got ${serializedSize}.`);
        }
        bytesWritten += dataSize;
        const padSize = alignTo(dataSize + DOCUMENT_FOOTER_SIZE, DOCUMENT_ALIGNMENT);
        this.writeBuffer.fill(0x20, this.writeBufferCursor + bytesWritten, this.writeBufferCursor + bytesWritten + padSize);
        bytesWritten += padSize;
        this.writeBuffer.writeUInt32BE(dataSize, this.writeBufferCursor + bytesWritten);
        bytesWritten += 4;
        DOCUMENT_SEPARATOR_BUFFER.copy(this.writeBuffer, this.writeBufferCursor + bytesWritten);
        bytesWritten += DOCUMENT_SEPARATOR_BUFFER.byteLength;
        this.writeBufferCursor += bytesWritten;
        this.writeBufferDocuments++;
        if (typeof callback === 'function') {
            this.flushCallbacks.push(callback);
        }
        this.flushIfWriteBufferDocumentsFull();
        return bytesWritten;
    }

    writeSerialized(dataSize, serializeToBuffer, sequenceNumber, callback) {
        assert(this.fd, 'Partition is not opened.');
        if (typeof sequenceNumber === 'function') {
            callback = sequenceNumber;
            sequenceNumber = null;
        }
        assert(typeof serializeToBuffer === 'function', 'serializeToBuffer needs to be a function.');
        assert(dataSize <= 64 * 1024 * 1024, 'Document is too large! Maximum is 64 MB');

        const dataPosition = this.size;
        if (dataSize + DOCUMENT_HEADER_SIZE >= this.getWriteBufferCapacity() * 4 / 5) {
            const temp = Buffer.allocUnsafe(dataSize);
            const serializedSize = serializeToBuffer(temp, 0);
            if (serializedSize !== undefined) {
                assert(serializedSize === dataSize, `Serialized size mismatch: expected ${dataSize}, got ${serializedSize}.`);
            }
            this.size += this.writeUnbuffered(temp, dataSize, sequenceNumber, callback);
        } else {
            this.size += this.writeBufferedSerialized(dataSize, serializeToBuffer, sequenceNumber, callback);
        }
        assert(this.size >= dataPosition + dataSize + DOCUMENT_HEADER_SIZE, `Error while writing document at position ${dataPosition}.`);
        return dataPosition;
    }

    /**
     * @api
     * @param {string} data The data to write to storage.
     * @param {number} [sequenceNumber] The external sequence number to store with the document.
     * @param {function} [callback] A function that will be called when the document is written to disk.
     * @returns {number|boolean} The file position at which the data was written or false on error.
     */
    write(data, sequenceNumber, callback) {
        assert(this.fd, 'Partition is not opened.');
        if (typeof sequenceNumber === 'function') {
            callback = sequenceNumber;
            sequenceNumber = null;
        }
        const dataSize = Buffer.isBuffer(data) ? data.byteLength : Buffer.byteLength(data, 'utf8');
        assert(dataSize <= 64 * 1024 * 1024, 'Document is too large! Maximum is 64 MB');

        const dataPosition = this.size;
        if (dataSize + DOCUMENT_HEADER_SIZE >= this.getWriteBufferCapacity() * 4 / 5) {
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
     * @returns {object} A reader object with properties `buffer`, `cursor` and `length`.
     */
    prepareReadBufferBackwards(position) {
        const bufferPos = this.size - this.writeBufferCursor;
        // Handle the case when data that is still in write buffer is supposed to be read backwards
        if (this.dirtyReads && this.writeBufferCursor > 0 && position > bufferPos) {
            return { buffer: this.writeBuffer, cursor: position - bufferPos, length: this.writeBufferCursor };
        }
        return super.prepareReadBufferBackwards(position);
    }

    /**
     * Read all documents in reverse write order, ignoring any unflushed write-buffer data when
     * dirty reads are disabled.
     *
     * @api
     * @param {number} [before] The document position to start reading backward from.
     * @returns {Generator<string>} A generator that returns all documents in this partition in reverse order.
     */
    *readAllBackwards(before = -1) {
        if (!this.dirtyReads && this.writeBufferCursor > 0) {
            const flushedSize = this.size - this.writeBufferCursor;
            const clampedBefore = before < 0 ? flushedSize : Math.min(before, flushedSize);
            yield* super.readAllBackwards(clampedBefore);
            return;
        }
        yield* super.readAllBackwards(before);
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
            if (!(e instanceof CorruptFileError)) {
                throw new Error('Can only truncate on valid document boundaries.');
            }
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
        do {
            const reader = this.prepareReadBuffer(position);
            fs.writeSync(deletedBranch.fd, reader.buffer, reader.cursor, reader.length);
            position += reader.length;
        } while (position < this.size);
        deletedBranch.close();
        return deletedBranch;
    }
}

export default WritablePartition;
export { CorruptFileError };
