import EventStream from './EventStream.js';
import { kWayMerge } from './util.js';

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
     * @param {function|object|null} [predicate] Optional matcher (see {@link EventStream}).
     * @param {boolean} [raw=false] If true, emit NDJSON Buffers.
     */
    constructor(name, streams, eventStore, minRevision = 1, maxRevision = -1, predicate = null, raw = false) {
        super(name, eventStore, minRevision, maxRevision, predicate, raw);
        if (!(streams instanceof Array) || streams.length === 0) {
            throw new Error(`Invalid list of streams supplied to JoinStream ${name}.`);
        }

        this.streamIndex = eventStore.storage.index;
        // Translate revisions to index numbers (1-based) and wrap around negatives
        this.minRevision = normalizeVersion(minRevision, eventStore.length);
        this.maxRevision = normalizeVersion(maxRevision, eventStore.length);
        this.fetch = function() {
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
     * @returns {Generator<object>}
     */
    createMergedIterator() {
        const ascending = this.minRevision <= this.maxRevision;
        const raw = this.raw;
        return kWayMerge(
            this.fetch(),
            entry => raw ? entry.sequenceNumber : entry.metadata.commitId,
            ascending
        );
    }

    /**
     * Returns the next event in merge order.
     *
     * In raw mode: returns `{ buffer, time64, sequenceNumber }` from the binary header — no JSON
     * deserialization. In object mode: returns a deserialized `{ stream, payload, metadata }` document
     * produced by the storage layer.
     * @returns {object|false} The next event, or `false` when the stream is exhausted.
     */
    next() {
        if (!this._iterator) {
            this._iterator = this.createMergedIterator();
        }
        while (true) {
            const step = this._iterator.next();
            if (step.done) {
                return false;
            }
            const next = step.value;

            if (this.matchesPredicate(next)) {
                return next;
            }
        }
    }

}

export default JoinEventStream;
