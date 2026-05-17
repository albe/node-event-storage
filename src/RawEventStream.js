import { Readable } from 'stream';

/**
 * A non-object-mode Readable that emits raw NDJSON buffers for a document range.
 * Each chunk is a complete JSON document followed by a newline byte, so the stream
 * can be piped directly into an HTTP response without further serialization.
 *
 * The iterator is created lazily on the first `_read` call, mirroring the lazy-fetch
 * pattern of {@link EventStream}.
 */
class RawEventStream extends Readable {

    /**
     * @param {import('../Storage/ReadableStorage.js').default} storage The storage to read from.
     * @param {number} [from=1] The 1-based document number (inclusive) to start reading from.
     * @param {number} [until=-1] The 1-based document number (inclusive) to read until. Defaults to index.length.
     * @param {object|null} [index=null] The index to use. Defaults to the storage's primary index.
     */
    constructor(storage, from = 1, until = -1, index = null) {
        super({ objectMode: false });
        this.storage = storage;
        this.from = from;
        this.until = until;
        this.index = index;
        this.bufferIterator = null;
    }

    _read() {
        if (!this.bufferIterator) {
            this.bufferIterator = this.storage.readRangeBuffers(this.from, this.until, this.index);
        }
        const next = this.bufferIterator.next();
        this.push(next.done ? null : next.value);
    }
}

export default RawEventStream;
