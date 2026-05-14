import fs from 'fs';
import path from 'path';
import events from 'events';
import mmap from '@riaskov/mmap-io';
import WatchesFile from '../WatchesFile.js';
import { ensureDirectory } from '../fsUtil.js';
import { alignTo, assert } from '../util.js';

const FILE_SIZE_MARKER_SIZE = 4;
const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;

class ReadableAppendOnlyMmapedFile extends events.EventEmitter {

    constructor(name, options = {}) {
        super();
        assert(typeof name === 'string' && name !== '', 'Must specify a file name.');
        const config = Object.assign({
            dataDirectory: '.',
        }, options);

        this.name = name;
        this.dataDirectory = path.resolve(config.dataDirectory);
        this.fileName = path.resolve(this.dataDirectory, name);
        this.pageSize = mmap.PAGESIZE;

        this.fileMode = 'r';
        this.fd = null;
        this.fileSize = 0;
        this.mappedSize = 0;
        this.mapBuffer = null;
    }

    isOpen() {
        return !!this.fd;
    }

    open() {
        if (this.fd) {
            return false;
        }
        this.fd = fs.openSync(this.fileName, this.fileMode);
        this.remap();
        return true;
    }

    close() {
        this.unmap();
        if (this.fd) {
            fs.closeSync(this.fd);
            this.fd = null;
        }
    }

    map(size) {
        this.mappedSize = size;
        if (size <= 0) {
            this.mapBuffer = Buffer.alloc(0);
            return;
        }
        const protection = this.fileMode === 'r'
            ? mmap.PROT_READ
            : mmap.PROT_READ | mmap.PROT_WRITE;
        this.mapBuffer = mmap.map(size, protection, mmap.MAP_SHARED, this.fd, 0);
    }

    unmap() {
        this.mapBuffer = null;
        this.mappedSize = 0;
    }

    readFileSizeMarker() {
        if (this.mappedSize < FILE_SIZE_MARKER_SIZE) {
            return -1;
        }
        const markerPos = this.mappedSize - FILE_SIZE_MARKER_SIZE;
        const backwardsOffset = this.mapBuffer.readUInt32BE(markerPos);
        const size = this.mappedSize - backwardsOffset;
        if (size < 0 || size > markerPos) {
            return -1;
        }
        return size;
    }

    detectFileSize() {
        if (this.mappedSize <= FILE_SIZE_MARKER_SIZE) {
            return 0;
        }
        const limit = this.mappedSize - FILE_SIZE_MARKER_SIZE;
        for (let position = limit - 4; position >= 0; position -= 4) {
            if (this.mapBuffer.readUInt32BE(position) !== 0) {
                return position + 4;
            }
        }
        return 0;
    }

    remap() {
        assert(this.fd, 'File is not opened.');
        const stat = fs.fstatSync(this.fd);
        this.unmap();
        this.map(stat.size);
        const markedSize = this.readFileSizeMarker();
        this.fileSize = markedSize >= 0 ? markedSize : this.detectFileSize();
    }

    read(position, length) {
        assert(this.mapBuffer !== null, 'File is not mapped.');
        assert(position >= 0, 'Position must be >= 0.');
        assert(length >= 0, 'Length must be >= 0.');
        assert(position + length <= this.fileSize, 'Read exceeds file size.');
        return this.mapBuffer.subarray(position, position + length);
    }

    readString(position, length, encoding = 'utf8') {
        return this.read(position, length).toString(encoding);
    }
}

class WritableAppendOnlyMmapedFile extends ReadableAppendOnlyMmapedFile {

    constructor(name, options = {}) {
        super(name, options);
        this.fileMode = 'r+';
        this.writeBufferSize = Math.max(this.pageSize, (options.writeBufferSize >>> 0) || DEFAULT_WRITE_BUFFER_SIZE); // jshint ignore:line
        this.initialData = options.initialData ? Buffer.from(options.initialData) : null;
    }

    writeFileSizeMarker() {
        assert(this.mapBuffer !== null, 'File is not mapped.');
        assert(this.fileSize <= this.mappedSize - FILE_SIZE_MARKER_SIZE, 'Invalid file size marker range.');
        const markerPos = this.mappedSize - FILE_SIZE_MARKER_SIZE;
        const backwardsOffset = this.mappedSize - this.fileSize;
        this.mapBuffer.writeUInt32BE(backwardsOffset >>> 0, markerPos); // jshint ignore:line
    }

    flush() {
        if (!this.fd || !this.mapBuffer) {
            return false;
        }
        mmap.sync(this.mapBuffer, true);
        const now = new Date();
        fs.futimesSync(this.fd, now, now);
        fs.fdatasyncSync(this.fd);
        return true;
    }

    truncate(size) {
        assert(this.mapBuffer !== null, 'File is not mapped.');
        size = Math.max(0, size);
        if (size >= this.fileSize) {
            return false;
        }
        this.mapBuffer.fill(0, size, this.fileSize);
        this.fileSize = size;
        this.writeFileSizeMarker();
        return true;
    }

    computeMappedSize(actualSize) {
        const requestedSize = actualSize + this.writeBufferSize + FILE_SIZE_MARKER_SIZE;
        return requestedSize + alignTo(requestedSize, this.pageSize);
    }

    initializeNewFile() {
        const initialSize = this.initialData ? this.initialData.byteLength : 0;
        this.fileSize = initialSize;
        this.mappedSize = this.computeMappedSize(initialSize);
        fs.ftruncateSync(this.fd, this.mappedSize);
        this.map(this.mappedSize);
        if (this.initialData && this.initialData.byteLength > 0) {
            this.initialData.copy(this.mapBuffer, 0);
        }
        this.writeFileSizeMarker();
        this.flush();
    }

    open() {
        if (this.fd) {
            return false;
        }
        ensureDirectory(this.dataDirectory);
        try {
            this.fd = fs.openSync(this.fileName, this.fileMode);
        } catch (e) {
            if (e.code !== 'ENOENT') {
                throw e;
            }
            this.fd = fs.openSync(this.fileName, 'w+');
        }
        const stat = fs.fstatSync(this.fd);
        if (stat.size === 0) {
            this.initializeNewFile();
            return true;
        }

        this.remap();
        const wantedMappedSize = this.computeMappedSize(this.fileSize);
        if (this.mappedSize < wantedMappedSize) {
            this.grow(this.fileSize);
        }
        this.writeFileSizeMarker();
        this.flush();
        return true;
    }

    grow(requiredFileSize) {
        const nextMappedSize = this.computeMappedSize(requiredFileSize);
        if (nextMappedSize <= this.mappedSize) {
            return false;
        }
        this.flush();
        this.unmap();
        fs.ftruncateSync(this.fd, nextMappedSize);
        this.map(nextMappedSize);
        this.fileSize = requiredFileSize;
        this.writeFileSizeMarker();
        return true;
    }

    write(data) {
        assert(this.mapBuffer !== null, 'File is not mapped.');
        const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
        const writePosition = this.fileSize;
        const endPosition = writePosition + buffer.byteLength;
        if (endPosition > this.mappedSize - FILE_SIZE_MARKER_SIZE) {
            this.grow(endPosition);
        }
        buffer.copy(this.mapBuffer, writePosition);
        this.fileSize = endPosition;
        this.writeFileSizeMarker();
        return writePosition;
    }

    close() {
        this.flush();
        super.close();
    }
}

class ReadOnlyAppendOnlyMmapedFile extends WatchesFile(ReadableAppendOnlyMmapedFile) {

    onChange() {
        if (!this.fd) {
            return;
        }
        const previousSize = this.fileSize;
        this.remap();
        if (this.fileSize > previousSize) {
            this.emit('append', previousSize, this.fileSize);
        }
        if (this.fileSize < previousSize) {
            this.emit('truncate', previousSize, this.fileSize);
        }
    }

    onRename() {
        this.close();
    }
}

WritableAppendOnlyMmapedFile.ReadOnly = ReadOnlyAppendOnlyMmapedFile;
WritableAppendOnlyMmapedFile.Readable = ReadableAppendOnlyMmapedFile;

export default WritableAppendOnlyMmapedFile;
export { ReadableAppendOnlyMmapedFile, ReadOnlyAppendOnlyMmapedFile, FILE_SIZE_MARKER_SIZE };
