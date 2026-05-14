import WritableAppendOnlyMmapedFile from '../File/AppendOnlyMmapedFile.js';
import MmapReadablePartition, {
    CorruptFileError,
    HEADER_MAGIC,
    DOCUMENT_ALIGNMENT,
    DOCUMENT_SEPARATOR,
    DOCUMENT_HEADER_SIZE,
    DOCUMENT_FOOTER_SIZE
} from './MmapReadablePartition.js';
import { buildMetadataHeader } from '../metadataUtil.js';
import Clock from '../Clock.js';
import { assert, alignTo } from '../util.js';

const DOCUMENT_PAD = ' '.repeat(DOCUMENT_ALIGNMENT);

class MmapWritablePartition extends MmapReadablePartition {

    constructor(name, config = {}) {
        const defaults = {
            writeBufferSize: 64 * 1024,
            syncOnFlush: false,
            metadata: {
                epoch: Date.now()
            },
            clock: Clock,
            flushDelay: 0
        };
        const metadata = Object.assign({}, defaults.metadata, config.metadata);
        config = Object.assign({}, defaults, config, { metadata });
        super(name, config);

        this.syncOnFlush = !!config.syncOnFlush;
        this.flushDelay = config.flushDelay >>> 0; // jshint ignore:line
        assert(
            typeof(config.clock.prototype) === 'object' && typeof(config.clock.prototype.time) === 'function',
            'Clock constructor prototype needs to implement the method time().'
        );
        this.ClockConstructor = config.clock;
    }

    createFile(name, config) {
        const metadataHeader = buildMetadataHeader(HEADER_MAGIC, config.metadata);
        return new WritableAppendOnlyMmapedFile(name, Object.assign({}, config, { initialData: metadataHeader }));
    }

    open() {
        if (this.file.isOpen()) {
            return true;
        }
        const opened = super.open();
        if (!opened) {
            return false;
        }

        this.flushCallbacks = [];
        this.flushTimeout = null;
        this.clock = new this.ClockConstructor(this.metadata.epoch);
        return true;
    }

    close() {
        this.flush();
        super.close();
    }

    flush() {
        if (!this.file.isOpen()) {
            return false;
        }
        if (this.flushTimeout) {
            clearTimeout(this.flushTimeout);
            this.flushTimeout = null;
        }

        const flushed = this.file.flush();
        if (!flushed) {
            return false;
        }

        const callbacks = this.flushCallbacks;
        this.flushCallbacks = [];
        for (let i = 0; i < callbacks.length; i++) {
            callbacks[i]();
        }
        return true;
    }

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

    scheduleFlush() {
        if (this.flushTimeout) {
            return;
        }
        this.flushTimeout = setTimeout(() => this.flush(), this.flushDelay);
    }

    write(data, sequenceNumber, callback) {
        assert(this.fd, 'Partition is not opened.');
        if (typeof sequenceNumber === 'function') {
            callback = sequenceNumber;
            sequenceNumber = null;
        }
        const dataSize = Buffer.byteLength(data, 'utf8');
        assert(dataSize <= 64 * 1024 * 1024, 'Document is too large! Maximum is 64 MB');

        const dataPosition = this.size;
        const bytesToWrite = this.documentWriteSize(dataSize);
        const target = this.file.reserve(bytesToWrite);

        let bytesWritten = 0;
        bytesWritten += this.writeDocumentHeader(target, bytesWritten, dataSize, sequenceNumber);
        bytesWritten += target.write(data, bytesWritten, 'utf8');
        const padSize = alignTo(dataSize + DOCUMENT_FOOTER_SIZE, DOCUMENT_ALIGNMENT);
        bytesWritten += target.write(DOCUMENT_PAD.slice(0, padSize), bytesWritten, 'utf8');
        target.writeUInt32BE(dataSize, bytesWritten);
        bytesWritten += 4;
        bytesWritten += target.write(DOCUMENT_SEPARATOR, bytesWritten, 'utf8');

        assert(bytesWritten === bytesToWrite, `Error while writing document at position ${dataPosition}.`);
        this.size += bytesWritten;

        if (typeof callback === 'function') {
            this.flushCallbacks.push(callback);
        }
        if (this.syncOnFlush) {
            this.flush();
        } else {
            this.scheduleFlush();
        }
        return dataPosition;
    }

    truncateReadBuffer(after) {
        if (this.readBufferPos >= after) {
            this.readBufferPos = -1;
            this.readBufferLength = 0;
        } else if (this.readBufferPos + this.readBufferLength > after) {
            this.readBufferLength -= (this.readBufferPos + this.readBufferLength) - after;
        }
    }

    truncateAfterSequence(after) {
        let position = this.size;
        let truncateAt = this.size;
        while ((position = this.findDocumentPositionBefore(position)) !== false) {
            const reader = this.prepareReadBufferBackwards(position);
            if (!reader.buffer) break;
            const { sequenceNumber } = this.readDocumentHeader(reader.buffer, reader.cursor, position);
            if (sequenceNumber > after) {
                truncateAt = position;
            } else {
                break;
            }
        }
        this.truncate(truncateAt);
    }

    truncate(after) {
        if (after >= this.size) {
            return;
        }
        this.open();
        after = Math.max(0, after);
        this.flush();

        try {
            this.readFrom(after);
        } catch (e) {
            if (!(e instanceof CorruptFileError)) {
                throw new Error('Can only truncate on valid document boundaries.');
            }
        }

        this.file.truncate(this.headerSize + after);
        this.flush();
        this.truncateReadBuffer(after);
        this.size = after;
        this.emit('truncated', after);
    }
}

export default MmapWritablePartition;
export { CorruptFileError };
