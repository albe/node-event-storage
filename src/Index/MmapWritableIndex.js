import fs from 'fs';
import { buildMetadataHeader } from '../metadataUtil.js';
import { ensureDirectory } from '../fsUtil.js';
import MmapReadableIndex from './MmapReadableIndex.js';
import MmapReadOnlyIndex from './MmapReadOnlyIndex.js';
import { loadMmapModule, getMmapPackageName } from './MmapModule.js';

const HEADER_MAGIC = 'nesidx01';

class MmapWritableIndex extends MmapReadableIndex {
    initialize(options) {
        super.initialize(options);
        ensureDirectory(options.dataDirectory);
        this.fileMode = 'a+';
        this.syncOnFlush = !!options.syncOnFlush;
        this.flushDelay = (options.flushDelay === undefined ? 100 : options.flushDelay) >>> 0;
        this.flushTimeout = null;
        this.hasUnflushedWrites = false;
    }

    checkFile() {
        const stat = fs.fstatSync(this.fd);
        if (stat.size === 0) {
            this.writeMetadata();
            return 0;
        }
        const dataBytes = stat.size - this.readMetadata();
        if (dataBytes < 0) {
            throw new Error('Invalid index file!');
        }
        const length = Math.floor(dataBytes / this.EntryClass.size);
        const validBytes = length * this.EntryClass.size;
        if (validBytes !== dataBytes) {
            fs.ftruncateSync(this.fd, this.headerSize + validBytes);
        }
        return length;
    }

    open() {
        if (this.flushTimeout) {
            clearTimeout(this.flushTimeout);
            this.flushTimeout = null;
        }
        this.hasUnflushedWrites = false;
        const opened = super.open();
        this.mapEntries(this.bytesForEntries(this.lengthValue), true);
        return opened;
    }

    mapEntriesForLength(length) {
        const requiredMapSize = this.bytesForEntries(length);
        if (requiredMapSize <= this.mappedSize && this.mapBuffer) {
            return;
        }
        const chunk = this.mapGrowthBytes;
        const grownSize = Math.max(requiredMapSize, this.mappedSize + chunk);
        fs.ftruncateSync(this.fd, grownSize);
        this.mapEntries(grownSize, true);
    }

    add(entry, callback) {
        const number = entry.number ?? entry[0];
        const position = entry.position ?? entry[1];
        const size = entry.size ?? entry[2] ?? 0;
        const partition = entry.partition ?? entry[3] ?? 0;
        if (this.lastNumber >= number) {
            throw new Error(`Consistency error. Tried to add entry ${number} but last entry is ${this.lastNumber}.`);
        }

        const nextLength = this.lengthValue + 1;
        this.mapEntriesForLength(nextLength);
        const offset = this.headerSize + this.lengthValue * this.EntryClass.size;
        this.mapBuffer.writeUInt32LE(number, offset);
        this.mapBuffer.writeUInt32LE(position, offset + 4);
        this.mapBuffer.writeUInt32LE(size, offset + 8);
        this.mapBuffer.writeUInt32LE(partition, offset + 12);

        this.lengthValue = nextLength;
        this.lastNumber = number;
        this.hasUnflushedWrites = true;
        if (!this.flushTimeout && this.syncOnFlush) {
            this.flushTimeout = setTimeout(() => this.flush(), this.flushDelay);
        }
        if (typeof callback === 'function') {
            callback(this.lengthValue);
        }
        return this.lengthValue;
    }

    flush() {
        if (!this.fd) {
            return false;
        }
        if (this.flushTimeout) {
            clearTimeout(this.flushTimeout);
            this.flushTimeout = null;
        }
        if (!this.hasUnflushedWrites) {
            return false;
        }
        if (this.syncOnFlush) {
            this.mmap.sync(this.mapBuffer);
            fs.fsyncSync(this.fd);
        }
        this.hasUnflushedWrites = false;
        return true;
    }

    truncate(after) {
        if (!this.fd) {
            return;
        }
        if (after < 0) {
            after = 0;
        }
        this.hasUnflushedWrites = false;
        if (this.flushTimeout) {
            clearTimeout(this.flushTimeout);
            this.flushTimeout = null;
        }

        const truncatePosition = this.bytesForEntries(after);
        const stat = fs.statSync(this.fileName);
        if (truncatePosition >= stat.size && after >= this.lengthValue) {
            return;
        }
        fs.ftruncateSync(this.fd, truncatePosition);
        this.lengthValue = after;
        this.lastNumber = after > 0 ? this.readEntryAt(this.headerSize + (after - 1) * this.EntryClass.size).number : 0;
        this.mapEntries(truncatePosition, true);
    }

    writeMetadata() {
        if (!this.metadata) {
            this.metadata = { entryClass: this.EntryClass.name, entrySize: this.EntryClass.size };
        }
        const metadataBuffer = buildMetadataHeader(HEADER_MAGIC, this.metadata);
        fs.writeSync(this.fd, metadataBuffer, 0, metadataBuffer.byteLength, 0);
        this.headerSize = metadataBuffer.byteLength;
    }

    close() {
        if (this.fd) {
            this.flush();
            const finalSize = this.bytesForEntries(this.lengthValue);
            fs.ftruncateSync(this.fd, finalSize);
        }
        super.close();
    }

    destroy() {
        this.close();
        fs.unlinkSync(this.fileName);
    }
}

MmapWritableIndex.ReadOnly = MmapReadOnlyIndex;

export default MmapWritableIndex;
export { MmapReadableIndex, MmapReadOnlyIndex, loadMmapModule, getMmapPackageName };
