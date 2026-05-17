import { Readable } from 'stream';

/**
 * A non-object-mode Readable that emits raw NDJSON from a storage buffer generator.
 * Each yielded Buffer is already a complete JSON document followed by a newline, so the
 * stream can be piped directly into an HTTP response without further serialization.
 */
class RawEventStream extends Readable {

    /**
     * @param {Generator<Buffer>} bufferIterator A generator that yields one Buffer per document.
     *   Each buffer must already include the trailing newline (as produced by
     *   {@link ReadableStorage#readRangeBuffers}).
     */
    constructor(bufferIterator) {
        super();
        this.bufferIterator = bufferIterator;
    }

    _read() {
        const next = this.bufferIterator.next();
        this.push(next.done ? null : next.value);
    }
}

export default RawEventStream;
