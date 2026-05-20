import EventStream from './EventStream.js';

/** Reusable sentinel used for missing or empty per-stream iterators. */
const emptyIterator = Object.freeze({ next() { return { done: true }; } });

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
     * @param {function(object, object): boolean|true|null} [predicate] An optional filter function, or
     *   `true` to activate raw-buffer mode (see {@link EventStream}).
     */
    constructor(name, streams, eventStore, minRevision = 1, maxRevision = -1, predicate = null) {
        super(name, eventStore, minRevision, maxRevision, predicate);
        if (!(streams instanceof Array) || streams.length === 0) {
            throw new Error(`Invalid list of streams supplied to JoinStream ${name}.`);
        }

        this.streamIndex = eventStore.storage.index;
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
                // Raw mode: get { buffer, time64, sequenceNumber } for binary-header ordering.
                // Object mode: storage deserializes for us and we order by metadata.commitId.
                return eventStore.storage.readRange(from, until, streamIndex, this.raw);
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
     *
     * In raw mode: compares by `time64` (epoch-denormalized, from binary header) with `sequenceNumber`
     * as a globally-unique tiebreaker.
     * In object mode: compares by `metadata.commitId` (the global sequence number from the JSON body).
     * @private
     * @param {object} first
     * @param {object} second
     * @returns {boolean}
     */
    follows(first, second) {
        const descending = this.minRevision > this.maxRevision;
        const follows = (a, b) => descending ? a < b : a > b;
        if (this.raw) {
            if (first.time64 !== second.time64) return follows(first.time64, second.time64);
            return follows(first.sequenceNumber, second.sequenceNumber);
        }
        return follows(first.metadata.commitId, second.metadata.commitId);
    }

    /**
     * Returns the next event in merge order.
     *
     * In raw mode: returns `{ buffer, time64, sequenceNumber }` from the binary header — no JSON
     * deserialization. In object mode: returns a deserialized `{ stream, payload, metadata }` document
     * produced by the storage layer.
     * @returns {object|false}
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

            if (this.raw || !this.predicate || this.predicate(next.payload, next.metadata)) {
                return next;
            }
        }
    }

}

export default JoinEventStream;
