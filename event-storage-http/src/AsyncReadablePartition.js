import fs from 'fs/promises';
import ReadablePartition, {
    HEADER_MAGIC,
    DOCUMENT_ALIGNMENT,
    DOCUMENT_HEADER_SIZE
} from '../../src/Partition/ReadablePartition.js';

class AsyncReadablePartition extends ReadablePartition {
    async open() {
        if (this.fileHandle) {
            return true;
        }

        this.fileHandle = await fs.open(this.fileName, this.fileMode);
        this.fd = this.fileHandle.fd;
        this.readBuffer = Buffer.allocUnsafeSlow(this.readBufferSize);
        this.readBufferPos = -1;
        this.readBufferLength = 0;

        this.headerSize = 0;
        this.size = await this.readFileSizeAsync();
        if (this.size <= 0) {
            await this.close();
            return false;
        }

        this.size -= await this.readMetadataAsync();
        return true;
    }

    async close() {
        if (this.fileHandle) {
            await this.fileHandle.close();
            this.fileHandle = null;
            this.fd = null;
        }
        if (this.readBuffer) {
            this.readBuffer = null;
            this.readBufferPos = -1;
            this.readBufferLength = 0;
        }
    }

    async readFileSizeAsync() {
        const stat = await fs.stat(this.fileName);
        return stat.size - this.headerSize;
    }

    async readMetadataAsync() {
        const headerBuffer = Buffer.allocUnsafe(12);
        await this.fileHandle.read(headerBuffer, 0, 12, 0);
        const headerMagic = headerBuffer.toString('utf8', 0, 8);
        if (headerMagic.slice(0, 6) !== HEADER_MAGIC.slice(0, 6)) {
            throw new Error(`Invalid file header in partition ${this.name}.`);
        }
        if (headerMagic !== HEADER_MAGIC) {
            throw new Error(`Invalid file version. The partition ${this.name} was created with a different library version (${headerMagic.slice(6)}).`);
        }
        const metadataSize = headerBuffer.readUInt32BE(8);
        if (metadataSize <= 2 || metadataSize > 4096) {
            throw new Error('Invalid metadata size.');
        }

        const metadataBuffer = Buffer.allocUnsafe(metadataSize - 1);
        metadataBuffer.fill(' ');
        await this.fileHandle.read(metadataBuffer, 0, metadataSize - 1, 12);
        this.metadata = JSON.parse(metadataBuffer.toString('utf8').trim());
        this.metadata.epoch = this.metadata.epoch || new Date('2020-01-01T00:00:00').getTime();
        this.header = headerMagic;
        this.headerSize = 12 + metadataSize;
        return this.headerSize;
    }

    async fillBuffer(from = 0) {
        const { bytesRead } = await this.fileHandle.read(
            this.readBuffer,
            0,
            this.readBuffer.byteLength,
            this.headerSize + from
        );
        this.readBufferLength = bytesRead;
        this.readBufferPos = from;
    }

    async prepareReadBuffer(position) {
        if (position + DOCUMENT_HEADER_SIZE >= this.size) {
            return { buffer: null, cursor: 0, length: 0 };
        }
        let bufferCursor = position - this.readBufferPos;
        if (this.readBufferPos < 0 || bufferCursor < 0 || bufferCursor + DOCUMENT_HEADER_SIZE + DOCUMENT_ALIGNMENT > this.readBufferLength) {
            await this.fillBuffer(position);
            bufferCursor = 0;
        }
        return { buffer: this.readBuffer, cursor: bufferCursor, length: this.readBufferLength };
    }

    async readFrom(position, size = 0, headerOut = null) {
        if (!this.fileHandle) {
            throw new Error('Partition is not opened.');
        }
        if ((position % DOCUMENT_ALIGNMENT) !== 0) {
            throw new Error(`Invalid read position ${position}. Needs to be a multiple of ${DOCUMENT_ALIGNMENT}.`);
        }

        const reader = await this.prepareReadBuffer(position);
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
            throw new Error(`Invalid document at position ${position}. This may be caused by an unfinished write.`);
        }

        if (dataSize + DOCUMENT_HEADER_SIZE > reader.buffer.byteLength) {
            const tempReadBuffer = Buffer.allocUnsafe(dataSize);
            await this.fileHandle.read(tempReadBuffer, 0, dataSize, this.headerSize + position + DOCUMENT_HEADER_SIZE);
            return tempReadBuffer.toString('utf8');
        }

        if (reader.cursor > 0 && dataPosition + dataSize > reader.length) {
            await this.fillBuffer(position);
            dataPosition = DOCUMENT_HEADER_SIZE;
        }

        return reader.buffer.toString('utf8', dataPosition, dataPosition + dataSize);
    }

    async *readAll(after = 0, headerOut = null) {
        let position = after < 0 ? this.size + after + 1 : after;
        const internalHeader = headerOut !== null ? headerOut : {};
        let data;
        while ((data = await this.readFrom(position, 0, internalHeader)) !== false) {
            if (headerOut !== null) {
                headerOut.position = position;
            }
            yield data;
            position += this.documentWriteSize(internalHeader.dataSize);
        }
    }
}

export default AsyncReadablePartition;
