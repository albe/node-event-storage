import fs from 'fs';
import WritableAppendOnlyMmapedFile from '../File/AppendOnlyMmapedFile.js';
import MmapReadableIndex, { HEADER_MAGIC } from './MmapReadableIndex.js';
import Entry from '../IndexEntry.js';
import { assertEqual } from '../util.js';
import { buildMetadataHeader } from '../metadataUtil.js';

/**
 * An append-only, memory-mapped index.
 *
 * On first creation the metadata header is written as the initial mapped content so that the
 * file is immediately readable by MmapReadableIndex after a flush.  On subsequent opens the
 * header is read back from the mapped buffer and verified.
 *
 * Entries are appended by writing directly into the mapped region, so there is no separate
 * user-space write buffer.  A delayed flush (mmap.sync + fdatasync) is scheduled after each
 * add() call and can be forced at any time with flush().
 */
class MmapWritableIndex extends MmapReadableIndex {

    constructor(name = '.index', options = {}) {
        if (typeof name !== 'string') {
            options = name;
            name = '.index';
        }
        options = Object.assign({ writeBufferSize: 64 * 1024, flushDelay: 100 }, options);
        super(name, options);
    }

    createFile(name, options) {
        const EntryClass = options.EntryClass || Entry;
        const metadata = Object.assign(
            { entryClass: EntryClass.name, entrySize: EntryClass.size },
            options.metadata
        );
        // The metadata header becomes initialData so that a newly created file already
        // contains a valid readable header after the first open().
        const initialData = buildMetadataHeader(HEADER_MAGIC, metadata);
        return new WritableAppendOnlyMmapedFile(name, Object.assign({}, options, { initialData }));
    }

    initIndex(options) {
        this.flushDelay = options.flushDelay || 100;
        this.flushTimeout = null;
        this.flushCallbacks = [];
    }

    close() {
        if (this.file.isOpen()) {
            this.flush();
        }
        this.file.close();
    }

    destroy() {
        this.close();
        fs.unlinkSync(this.file.fileName);
    }

    /**
     * Flush pending mmap data to disk and invoke any registered post-flush callbacks.
     *
     * @returns {boolean} True if a flush was actually performed.
     */
    flush() {
        if (!this.file.isOpen()) {
            return false;
        }
        if (this.flushTimeout) {
            clearTimeout(this.flushTimeout);
            this.flushTimeout = null;
        }
        const flushed = this.file.flush();
        const callbacks = this.flushCallbacks;
        this.flushCallbacks = [];
        for (let i = 0; i < callbacks.length; i += 2) {
            callbacks[i](callbacks[i + 1]);
        }
        return flushed;
    }

    onFlush(callback, position) {
        if (typeof callback !== 'function') {
            return;
        }
        this.flushCallbacks.push(callback, position);
    }

    /**
     * Append a single entry to the index.
     *
     * @api
     * @param {Entry} entry
     * @param {function} [callback] Called with the 1-based entry position after the next flush.
     * @returns {number} The 1-based position of the appended entry.
     */
    add(entry, callback) {
        assertEqual(entry.constructor.name, this.EntryClass.name, 'Wrong entry object.');
        assertEqual(entry.constructor.size, this.EntryClass.size, 'Invalid entry size.');

        const last = this.lastEntry;
        if (last && last.number >= entry.number) {
            throw new Error('Consistency error. Tried to add an index that should come before existing last entry.');
        }

        const buf = Buffer.allocUnsafe(this.EntryClass.size);
        entry.toBuffer(buf, 0);
        this.file.write(buf);

        this.onFlush(callback, this.length);
        if (!this.flushTimeout) {
            this.flushTimeout = setTimeout(() => this.flush(), this.flushDelay);
        }
        return this.length;
    }

    /**
     * Truncate the index so that it contains at most `after` entries.
     *
     * @param {number} after Number of entries to keep (0 keeps none).
     */
    truncate(after) {
        if (!this.file.isOpen()) {
            return;
        }
        if (after < 0) {
            after = 0;
        }
        this.flush();
        this.file.truncate(this.headerSize + after * this.EntryClass.size);
    }
}

export default MmapWritableIndex;
export { Entry };
