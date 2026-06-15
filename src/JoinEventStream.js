import EventStream from './EventStream.js';
import { assert, kWayMerge } from './utils/util.js';
import { normalizeRevision } from './utils/apiHelpers.js';

/** Reusable sentinel used for missing or empty per-stream iterators. */
const emptyIterator = Object.freeze({ next() { return { done: true }; } });

/**
 * An event stream is a simple wrapper around an iterator over storage documents.
 * It implements a node readable stream interface.
 */
class JoinEventStream extends EventStream {

    /**
     * @param {string} name The name of the stream.
     * @param {Array<string>} streams The name of the streams to join together.
     * @param {EventStore} eventStore The event store to get the stream from.
     * @param {number} [minSequenceNumber] The 1-based minimum global sequence number to include in the events (inclusive).
     * @param {number} [maxSequenceNumber] The 1-based maximum global sequence number to include in the events (inclusive).
     * @param {function|object|null} [predicate] Optional matcher (see {@link EventStream}).
     * @param {boolean} [raw=false] If true, emit NDJSON Buffers.
     */
    constructor(name, streams, eventStore, minSequenceNumber = 1, maxSequenceNumber = -1, predicate = null, raw = false) {
        super(name, eventStore, minSequenceNumber, maxSequenceNumber, predicate, raw);
        assert(streams instanceof Array, `Invalid list of streams supplied to JoinStream ${name}.`);

        this.streamIndex = eventStore.storage.index;
        // Translate global sequence numbers to index numbers (1-based) and wrap around negatives.
        this.minRevision = normalizeRevision(minSequenceNumber, eventStore.length);
        this.maxRevision = normalizeRevision(maxSequenceNumber, eventStore.length);
        this.fetch = function() {
            return streams.map(streamName => {
                const streamIndex = eventStore.streams[streamName]?.index;
                if (!streamIndex || streamIndex.length === 0) {
                    return emptyIterator;
                }
                const from = streamIndex.find(this.minRevision, this.minRevision <= this.maxRevision);
                const until = streamIndex.find(this.maxRevision, this.minRevision > this.maxRevision);
                if (from === 0 || until === 0) {
                    // find() returns 0 when the requested sequence range is outside this stream's entries
                    // (e.g. minRevision > all entries, or maxRevision < all entries).
                    return emptyIterator;
                }
                // Raw mode: get { buffer, time64, sequenceNumber } for binary-header ordering.
                // Object mode: storage deserializes for us and we order by metadata.commitId.
                return eventStore.storage.readRange(from, until, streamIndex, this.raw);
            });
        }
        this._iterator = streams.length === 0 ? emptyIterator : null;
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
