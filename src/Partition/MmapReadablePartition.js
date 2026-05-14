import { ReadableAppendOnlyMmapedFile } from '../File/AppendOnlyMmapedFile.js';
import ReadablePartition, {
    CorruptFileError,
    InvalidDataSizeError,
    HEADER_MAGIC,
    DOCUMENT_ALIGNMENT,
    DOCUMENT_SEPARATOR,
    DOCUMENT_HEADER_SIZE,
    DOCUMENT_FOOTER_SIZE
} from './ReadablePartition.js';
import { assert } from '../util.js';

const NES_EPOCH = new Date('2020-01-01T00:00:00');

class MmapReadablePartition extends ReadablePartition {

    constructor(name, config = {}) {
        super(name, config);
        this.file = this.createFile(name, config);
    }

    createFile(name, config) {
        return new ReadableAppendOnlyMmapedFile(name, config);
    }

    isOpen() {
        return this.file.isOpen();
    }

    open() {
        if (this.file.isOpen()) {
            return true;
        }

        this.file.open();
        this.fd = this.file.fd;

        this.readBuffer = Buffer.allocUnsafeSlow(this.readBufferSize);
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

    readMetadata() {
        assert(this.size >= 16, 'Invalid file.');

        const headerBuffer = this.file.read(0, 8 + 4);
        const headerMagic = headerBuffer.toString('utf8', 0, 8);
        assert(headerMagic.substr(0, 6) === HEADER_MAGIC.substr(0, 6), `Invalid file header in partition ${this.name}.`);

        this.header = headerMagic;
        assert(
            headerMagic === HEADER_MAGIC,
            `Invalid file version. The partition ${this.name} was created with a different library version (${headerMagic.substr(6)}).`
        );

        const metadataSize = headerBuffer.readUInt32BE(8);
        assert(metadataSize > 2 && metadataSize <= 4096, 'Invalid metadata size.');
        assert(this.file.fileSize >= 12 + metadataSize, 'Invalid file.');

        const metadata = this.file.readString(12, metadataSize).trim();
        try {
            this.metadata = JSON.parse(metadata);
            this.metadata.epoch = this.metadata.epoch || NES_EPOCH.getTime();
        } catch (e) {
            throw new Error('Invalid metadata.');
        }

        this.headerSize = 12 + metadataSize;
        return this.headerSize;
    }

    readFileSize() {
        return this.file.fileSize - this.headerSize;
    }

    close() {
        this.file.close();
        this.fd = null;
        if (this.readBuffer) {
            this.readBuffer = null;
            this.readBufferPos = -1;
            this.readBufferLength = 0;
        }
    }

    fillBuffer(from = 0) {
        const available = Math.max(0, Math.min(this.readBuffer.byteLength, this.size - from));
        if (available === 0) {
            this.readBufferLength = 0;
            this.readBufferPos = from;
            return;
        }
        const source = this.file.read(this.headerSize + from, available);
        source.copy(this.readBuffer, 0, 0, available);
        this.readBufferLength = available;
        this.readBufferPos = from;
    }

    readFrom(position, size = 0, headerOut = null) {
        assert(this.fd, 'Partition is not opened.');
        assert((position % DOCUMENT_ALIGNMENT) === 0, `Invalid read position ${position}. Needs to be a multiple of ${DOCUMENT_ALIGNMENT}.`);

        const reader = this.prepareReadBuffer(position);
        if (reader.length < size + DOCUMENT_HEADER_SIZE) {
            return false;
        }

        let dataPosition = reader.cursor + DOCUMENT_HEADER_SIZE;
        const { dataSize, sequenceNumber, time64 } = this.readDocumentHeader(reader.buffer, reader.cursor, position, size);
        if (headerOut !== null) {
            headerOut.dataSize = dataSize;
            headerOut.sequenceNumber = sequenceNumber;
            headerOut.time64 = time64;
        }

        const writeSize = this.documentWriteSize(dataSize);
        if (position + writeSize > this.size) {
            throw new CorruptFileError(`Invalid document at position ${position}. This may be caused by an unfinished write.`);
        }

        if (dataSize + DOCUMENT_HEADER_SIZE > reader.buffer.byteLength) {
            const absoluteDataPosition = this.headerSize + position + DOCUMENT_HEADER_SIZE;
            return this.file.readString(absoluteDataPosition, dataSize);
        }

        if (reader.cursor > 0 && dataPosition + dataSize > reader.length) {
            this.fillBuffer(position);
            dataPosition = DOCUMENT_HEADER_SIZE;
        }

        return reader.buffer.toString('utf8', dataPosition, dataPosition + dataSize);
    }
}

export default MmapReadablePartition;
export { CorruptFileError, InvalidDataSizeError, HEADER_MAGIC, DOCUMENT_ALIGNMENT, DOCUMENT_SEPARATOR, DOCUMENT_HEADER_SIZE, DOCUMENT_FOOTER_SIZE };
