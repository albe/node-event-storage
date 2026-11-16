import EventStream from './EventStream.js';
import {assert} from './utils/util.js';
import {normalizeSelector, resolveSelector} from './utils/indexUtil.js';
import {normalizeRevision} from './utils/apiHelpers.js';

/**
 * JoinEventStream is a virtual stream over one or multiple physical stream indexes.
 *
 * It is the canonical implementation behind `EventStore.fromStreams(...)` and supports
 * nested selector algebra with alternating operators by depth:
 *
 * - depth 0 (top-level array): OR
 * - depth 1: AND
 * - depth 2: OR
 * - ... alternating by depth parity
 *
 * Flat arrays (e.g. `['a', 'b']`) therefore keep the legacy join semantics (OR).
 *
 * @extends EventStream
 */
class JoinEventStream extends EventStream {

    /**
     * @param {string} name The name of this virtual stream.
     * @param {Array<string|Array>} selector Stream selector as flat or nested arrays.
     * @param {EventStore} eventStore The event store instance.
     * @param {number} [minRevision=1] Global minimum revision (inclusive).
     * @param {number} [maxRevision=-1] Global maximum revision (inclusive).
     * @param {function|object|null} [predicate] Optional matcher (same semantics as EventStream).
     * @param {boolean} [raw=false] If true, emit NDJSON buffers.
     */
    constructor(name, selector, eventStore, minRevision = 1, maxRevision = -1, predicate = null, raw = false) {
        super(name, eventStore, minRevision, maxRevision, predicate, raw);
        assert(Array.isArray(selector) && selector.length > 0, `Invalid selector supplied to JoinEventStream ${name}.`);

        this.eventStore = eventStore;
        this.selector = normalizeSelector(selector);
        this.streamIndex = eventStore.storage.index;
        this.minRevision = normalizeRevision(minRevision, eventStore.length);
        this.maxRevision = normalizeRevision(maxRevision, eventStore.length);
        this.version = this.streamIndex.length;

        this.fetch = () => this.iterateDocuments();
        this._iterator = null;
    }


    /**
     * Get the range of index entries of one stream, bounded by the global revisions.
     *
     * @private
     * @param {string} streamName
     * @returns {Iterator<Entry>}
     */
    streamRange(streamName) {
        const index = this.eventStore.streams[streamName]?.index;
        const first = index ? index.find(Math.min(this.minRevision, this.maxRevision), true) : 0;
        const last = index ? index.find(Math.max(this.minRevision, this.maxRevision)) : 0;
        if (first === 0 || last === 0 || first > last) {
            return [][Symbol.iterator]();
        }
        return this.minRevision <= this.maxRevision ? index.range(first, last) : index.range(last, first);
    }

    /**
     * Iterate storage documents lazily from the matching index entries.
     *
     * @private
     * @returns {Generator<object|{ buffer: Buffer, time64: number, sequenceNumber: number }>}
     */
    * iterateDocuments() {
        const forwards = this.minRevision <= this.maxRevision;
        for (const entry of resolveSelector(this.selector, name => this.streamRange(name), forwards)) {
            const event = this.eventStore.storage.readFrom(entry.partition, entry.position, entry.size, this.raw, !forwards);
            if (event) {
                yield event;
            }
        }
    }
}

export default JoinEventStream;
