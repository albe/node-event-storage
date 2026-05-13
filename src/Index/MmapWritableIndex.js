import fs from 'fs';
import { createRequire } from 'module';
import WritableIndex from './WritableIndex.js';

const require = createRequire(import.meta.url);

let mmapModule = null;
let mmapPackageName = null;
const supportedMmapPackages = ['@riaskov/mmap-io', 'mmap-io', '@fayzanx/mmap-io'];

function loadMmapModule() {
    if (mmapModule) {
        return mmapModule;
    }
    let hasError = false;
    for (const packageName of supportedMmapPackages) {
        try {
            mmapModule = require(packageName);
            mmapPackageName = packageName;
            return mmapModule;
        } catch (e) {
            hasError = true;
        }
    }
    const attempted = supportedMmapPackages.join(', ');
    if (hasError) {
        throw new Error(`No compatible mmap-io implementation installed. Tried: ${attempted}`);
    }
    throw new Error('No compatible mmap-io implementation installed.');
}

function getMmapPackageName() {
    loadMmapModule();
    return mmapPackageName;
}

class MmapWritableIndex extends WritableIndex {
    initialize(options) {
        super.initialize(options);
        this.mmap = loadMmapModule();
        this.mapBuffer = null;
        this.mappedEntries = 0;
        this.flushedLength = 0;
    }

    open() {
        const opened = super.open();
        this.flushedLength = this.length;
        this.mapEntries(this.flushedLength);
        return opened;
    }

    close() {
        if (this.fd) {
            this.flush();
            fs.ftruncateSync(this.fd, this.headerSize + this.length * this.EntryClass.size);
        }
        super.close();
        this.unmapEntries();
        this.flushedLength = 0;
        this.mappedEntries = 0;
    }

    flush() {
        if (!this.fd) {
            return false;
        }
        if (this.flushTimeout) {
            clearTimeout(this.flushTimeout);
            this.flushTimeout = null;
        }
        if (this.writeBufferCursor === 0) {
            return false;
        }

        const entrySize = this.EntryClass.size;
        const pendingEntries = this.writeBufferCursor / entrySize;
        const flushUntil = this.flushedLength + pendingEntries;
        this.mapEntries(flushUntil);

        const writeOffset = this.headerSize + this.flushedLength * entrySize;
        this.writeBuffer.copy(this.mapBuffer, writeOffset, 0, this.writeBufferCursor);
        if (this.syncOnFlush) {
            this.mmap.sync(this.mapBuffer);
        }

        this.flushedLength = flushUntil;
        this.writeBufferCursor = 0;
        const callbacks = this.flushCallbacks;
        this.flushCallbacks = [];
        for (let i = 0; i < callbacks.length; i += 2) callbacks[i](callbacks[i + 1]);
        return true;
    }

    truncate(after) {
        super.truncate(after);
        this.flushedLength = this.length;
        this.mapEntries(this.flushedLength);
    }

    read(index) {
        index = Number(index) - 1;
        if (!this.mapBuffer || index < 0 || index >= this.flushedLength) {
            return super.read(index + 1);
        }
        const offset = this.headerSize + index * this.EntryClass.size;
        if (index === this.readUntil + 1) {
            this.readUntil++;
        }
        this.data[index] = this.EntryClass.fromBuffer(this.mapBuffer, offset);
        return this.data[index];
    }

    readRange(from, until) {
        if (!this.mapBuffer || until > this.flushedLength) {
            return super.readRange(from, until);
        }
        if (until === from) {
            return [this.read(from)];
        }

        from--;
        until--;

        const readFrom = Math.max(this.readUntil + 1, from);
        for (let index = readFrom; index <= until; index++) {
            const offset = this.headerSize + index * this.EntryClass.size;
            this.data[index] = this.EntryClass.fromBuffer(this.mapBuffer, offset);
        }
        if (from <= this.readUntil + 1) {
            this.readUntil = Math.max(this.readUntil, until);
        }
        return this.data.slice(from, until + 1);
    }

    mapEntries(length) {
        if (!this.fd) {
            return;
        }
        if (length === this.mappedEntries && this.mapBuffer) {
            return;
        }
        const mapSize = this.headerSize + length * this.EntryClass.size;
        fs.ftruncateSync(this.fd, mapSize);
        this.unmapEntries();
        if (length > 0) {
            this.mapBuffer = this.mmap.map(mapSize, this.mmap.PROT_READ | this.mmap.PROT_WRITE, this.mmap.MAP_SHARED, this.fd, 0);
        }
        this.mappedEntries = length;
    }

    unmapEntries() {
        if (!this.mapBuffer) {
            return;
        }
        this.mmap.sync(this.mapBuffer);
        // @riaskov/mmap-io currently has no explicit unmap() API, so we only call it when available.
        if (typeof this.mmap.unmap === 'function') {
            this.mmap.unmap(this.mapBuffer);
        }
        this.mapBuffer = null;
    }
}

export default MmapWritableIndex;
export { loadMmapModule, getMmapPackageName };
