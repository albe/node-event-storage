import EventStream from './EventStream.js';

/** Reusable sentinel used for missing or empty per-stream iterators. */
const emptyIterator = Object.freeze({ next() { return { done: true }; } });

/** Appended to each raw buffer chunk to form a newline-delimited JSON stream. */
const NDJSON_NEWLINE = Buffer.from('\n');

/**
 * Calculate the actual version number from a possibly relative (negative) version number.
 *
 * @param {number} version The version to normalize.
 * @param {number} length The maximum version number
 * @returns {number} The absolute version number.
 */
function normalizeVersion(version, length) {
    return version < 0 ? version + length + 1 : version;
}

/**
 * An event stream is a simple wrapper around an iterator over storage documents.
 * It implements a node readable stream interface.
 */
class JoinEventStream extends EventStream {

    /**
     * @param {string} name The name of the stream.
     * @param {Array<string>} streams The name of the streams to join together.
     * @param {EventStore} eventStore The event store to get the stream from.
     * @param {number} [minRevision] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The 1-based maximum revision to include in the events (inclusive).
     * @param {function(object, object): boolean|null} [predicate] An optional filter function
     *   `(payload, metadata) => boolean`.  Only events for which this returns truthy are yielded.
     * @param {boolean} [raw] When true, emits raw NDJSON Buffers. Ordering uses the document header's
     *   `time64` (denormalized by partition epoch) so no JSON deserialization is needed.
     */
    constructor(name, streams, eventStore, minRevision = 1, maxRevision = -1, predicate = null, raw = false) {
        super(name, eventStore, minRevision, maxRevision, predicate, raw);
        if (!(streams instanceof Array) || streams.length === 0) {
            throw new Error(`Invalid list of streams supplied to JoinStream ${name}.`);
        }

        this.streamIndex = eventStore.storage.index;
        this.serializer = eventStore.storage.serializer;
        // Translate revisions to index numbers (1-based) and wrap around negatives
        this.minRevision = normalizeVersion(minRevision, eventStore.length);
        this.maxRevision = normalizeVersion(maxRevision, eventStore.length);
        this.fetch = function() {
            this._next = new Array(streams.length).fill(undefined);
            return streams.map(streamName => {
                const streamIndex = eventStore.streams[streamName]?.index;
                if (!streamIndex || streamIndex.length === 0) {
                    return emptyIterator;
                }
                const from = streamIndex.find(this.minRevision, this.minRevision <= this.maxRevision);
                const until = streamIndex.find(this.maxRevision, this.minRevision > this.maxRevision);
                if (from === 0 || until === 0) {
                    // find() returns 0 when the requested revision is outside the stream's range
                    // (e.g. minRevision > all entries, or maxRevision < all entries).
                    return emptyIterator;
                }
                return eventStore.storage.iterateRangeWithHeaders(from, until, streamIndex);
            });
        }
        this._iterator = null;
    }

    /**
     * Returns the value of the iterator at position `index`
     * @param {number} index The iterator position for which to return the next value
     * @returns {*}
     */
    getValue(index) {
        const next = this._iterator[index].next();
        return next.done ? false : next.value;
    }

    /**
     * Returns true if `first` follows `second` in the read direction, meaning `second` should be yielded first.
     * Compares by `globalTime` (denormalized partition-epoch + time64) with `sequenceNumber` as tiebreaker.
     * @private
     * @param {{ globalTime: number, sequenceNumber: number }} first
     * @param {{ globalTime: number, sequenceNumber: number }} second
     * @returns {boolean}
     */
    follows(first, second) {
        if (this.minRevision > this.maxRevision) {
            // Descending: the event with the smaller globalTime comes later in the reverse scan.
            if (first.globalTime !== second.globalTime) return first.globalTime < second.globalTime;
            return first.sequenceNumber < second.sequenceNumber;
        }
        // Ascending: the event with the larger globalTime follows (comes after) the smaller one.
        if (first.globalTime !== second.globalTime) return first.globalTime > second.globalTime;
        return first.sequenceNumber > second.sequenceNumber;
    }

    /**
     * Returns the next event in merge order. Ordering is by `globalTime` (epoch-denormalized `time64`)
     * with `sequenceNumber` as a tiebreaker, read directly from the binary document header without
     * deserializing the JSON body.
     *
     * In object mode: returns a deserialized `{ stream, payload, metadata }` document.
     * In raw mode: returns `{ buffer, globalTime, sequenceNumber }` — no JSON deserialization.
     * @returns {object|{buffer:Buffer,globalTime:number,sequenceNumber:number}|false}
     */
    next() {
        if (!this._iterator) {
            this._iterator = this.fetch();
        }
        while (true) {
            let nextIndex = -1;
            this._next.forEach((value, index) => {
                if (typeof value === 'undefined') {
                    value = this._next[index] = this.getValue(index);
                }
                if (value === false) {
                    return;
                }
                if (nextIndex === -1 || this.follows(this._next[nextIndex], value)) {
                    nextIndex = index;
                }
            });

            if (nextIndex === -1) {
                return false;
            }
            const next = this._next[nextIndex];
            this._next[nextIndex] = undefined;

            if (this.raw) {
                // Raw mode: no deserialization. Predicates are not supported in this mode.
                return next;
            }

            // Object mode: deserialize from the raw buffer to apply predicate and return the document.
            const document = this.serializer.deserialize(next.buffer.toString('utf8'));
            if (!this.predicate || this.predicate(document.payload, document.metadata)) {
                return document;
            }
        }
    }

    /**
     * Readable stream implementation.
     * @private
     */
    _read() {
        const next = this.next();
        if (this.raw) {
            // next is { buffer, globalTime, sequenceNumber } — push raw bytes + newline, no re-serialization.
            this.push(next === false ? null : Buffer.concat([next.buffer, NDJSON_NEWLINE]));
        } else {
            this.push(next ? next.payload : null);
        }
    }

}

export default JoinEventStream;
