import fs from 'fs';
import path from 'path';
import events from 'events';
import Entry, { assertValidEntryClass } from '../IndexEntry.js';
import { assert, wrapAndCheck, binarySearch } from '../util.js';
import { loadMmapModule, unmapBuffer } from './MmapModule.js';

const HEADER_MAGIC = 'nesidx01';

class MmapReadableIndex extends events.EventEmitter {
    constructor(name = '.index', options = {}) {
        super();
        if (typeof name !== 'string') {
            options = name;
            name = '.index';
        }
        const defaults = {
            dataDirectory: '.',
            EntryClass: Entry,
            writeBufferSize: 4096,
        };
        options = Object.assign(defaults, options);
        assertValidEntryClass(options.EntryClass);

        this.name = name;
        this.initialize(options);
        this.open();
    }

    initialize(options) {
        this.EntryClass = options.EntryClass;
        this.dataDirectory = options.dataDirectory;
        this.fileName = path.resolve(options.dataDirectory, this.name);
        this.fileMode = 'r';
        this.fd = null;
        this.lengthValue = 0;
        this.headerSize = 0;
        this.mmap = loadMmapModule();
        this.mapBuffer = null;
        this.mappedSize = 0;
        this.mapGrowthBytes = Math.max(options.writeBufferSize >>> 0, this.EntryClass.size);
        this.lastNumber = 0;

        if (options.metadata) {
            this.metadata = Object.assign({ entryClass: options.EntryClass.name, entrySize: options.EntryClass.size }, options.metadata);
        }
    }

    get length() {
        return this.lengthValue;
    }

    get lastEntry() {
        if (this.lengthValue < 1) {
            return false;
        }
        return this.get(this.lengthValue);
    }

    isOpen() {
        return !!this.fd;
    }

    open() {
        if (this.fd) {
            return false;
        }
        this.fd = fs.openSync(this.fileName, this.fileMode);
        const length = this.readFileLength();
        this.lengthValue = length;
        this.mapEntries(this.bytesForEntries(length), false);
        this.lastNumber = length > 0 ? this.readEntryAt(this.headerSize + (length - 1) * this.EntryClass.size).number : 0;
        return true;
    }

    close() {
        this.lengthValue = 0;
        this.lastNumber = 0;
        if (this.fd) {
            fs.closeSync(this.fd);
            this.fd = null;
        }
        this.unmapEntries();
    }

    bytesForEntries(length) {
        return this.headerSize + length * this.EntryClass.size;
    }

    readFileLength() {
        let length;
        try {
            length = this.checkFile();
            assert(length >= 0, 'Index file was truncated to empty!');
        } catch (e) {
            this.close();
            throw e;
        }
        return length;
    }

    checkFile() {
        const stat = fs.fstatSync(this.fd);
        if (stat.size === 0) {
            return 0;
        }
        const dataBytes = stat.size - this.readMetadata();
        assert(dataBytes >= 0, 'Invalid index file!');
        const length = Math.floor(dataBytes / this.EntryClass.size);
        assert(dataBytes === length * this.EntryClass.size, 'Index file is corrupt!');
        return length;
    }

    verifyAndSetMetadata(metadata) {
        try {
            const parsedMetadata = JSON.parse(metadata);
            if (this.metadata && JSON.stringify(this.metadata) !== JSON.stringify(parsedMetadata)) {
                throw new Error('Index metadata mismatch! ' + metadata);
            }
            this.metadata = parsedMetadata;
        } catch (e) {
            if (e.message.startsWith('Index metadata mismatch!')) {
                throw e;
            }
            throw new Error('Invalid metadata.');
        }
    }

    readMetadata() {
        const headerBuffer = Buffer.allocUnsafe(12);
        fs.readSync(this.fd, headerBuffer, 0, 12, 0);
        const headerMagic = headerBuffer.toString('utf8', 0, 8);
        assert(headerMagic.slice(0, 6) === HEADER_MAGIC.slice(0, 6), 'Invalid file header.');
        assert(headerMagic === HEADER_MAGIC, `Invalid file version. The index ${this.fileName} was created with a different library version (${headerMagic.slice(6)}).`);

        const metadataSize = headerBuffer.readUInt32BE(8);
        assert(metadataSize >= 3, 'Invalid metadata size.');
        const metadataBuffer = Buffer.allocUnsafe(metadataSize - 1);
        metadataBuffer.fill(' ');
        fs.readSync(this.fd, metadataBuffer, 0, metadataSize - 1, 12);
        const metadata = metadataBuffer.toString('utf8').trim();
        this.verifyAndSetMetadata(metadata);
        this.headerSize = 12 + metadataSize;
        return this.headerSize;
    }

    mapEntries(mapSize, writable) {
        this.unmapEntries();
        if (mapSize < this.headerSize + this.EntryClass.size) {
            this.mappedSize = mapSize;
            return;
        }
        this.mapBuffer = this.mmap.map(
            mapSize,
            writable ? this.mmap.PROT_READ | this.mmap.PROT_WRITE : this.mmap.PROT_READ,
            this.mmap.MAP_SHARED,
            this.fd,
            0
        );
        this.mappedSize = mapSize;
    }

    unmapEntries() {
        if (!this.mapBuffer) {
            return;
        }
        unmapBuffer(this.mmap, this.mapBuffer);
        this.mapBuffer = null;
        this.mappedSize = 0;
    }

    read(index) {
        index = Number(index) - 1;
        if (index < 0 || index >= this.lengthValue) {
            return false;
        }
        return this.readEntryAt(this.headerSize + index * this.EntryClass.size);
    }

    get(index) {
        index = wrapAndCheck(index, this.lengthValue);
        if (index <= 0) {
            return false;
        }
        return this.read(index);
    }

    all() {
        if (this.lengthValue < 1) {
            return [];
        }
        return this.range(1, this.lengthValue);
    }

    range(from, until = -1) {
        from = wrapAndCheck(from, this.lengthValue);
        until = wrapAndCheck(until, this.lengthValue);

        if (from <= 0 || until < from) {
            return false;
        }
        const length = until - from + 1;
        const entries = new Array(length);
        for (let i = 0; i < length; i++) {
            entries[i] = this.readEntryAt(this.headerSize + (from - 1 + i) * this.EntryClass.size);
        }
        return entries;
    }

    readEntryAt(offset) {
        return {
            number: this.mapBuffer.readUInt32LE(offset),
            position: this.mapBuffer.readUInt32LE(offset + 4),
            size: this.mapBuffer.readUInt32LE(offset + 8),
            partition: this.mapBuffer.readUInt32LE(offset + 12),
        };
    }

    find(number, min = false) {
        if (this.lengthValue < 1) {
            return 0;
        }
        const [low, high] = binarySearch(number, Math.min(this.lengthValue, number), index => this.read(index).number);
        return min ? low : high;
    }
}

export default MmapReadableIndex;
