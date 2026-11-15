import EventStream from './EventStream.js';
import {assert} from './utils/util.js';
import {EXHAUSTED, IndexCursor, OrCursor, AndCursor, normalizeSelector} from './utils/indexUtil.js';
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
     * Build a cursor over the matching index entries for one selector node.
     * Selector is already normalized and optimized from constructor, so we just traverse it.
     *
     * @private
     * @param {string|Array<string|Array>} selectorNode
     * @param {number} [depth=0]
     * @returns {IndexCursor|OrCursor|AndCursor}
     */
    buildCursor(selectorNode, depth = 0) {
        if (typeof selectorNode === 'string') {
            const index = this.eventStore.streams[selectorNode]?.index;
            return this.buildIndexCursor(index);
        }

        const children = selectorNode.map(node => this.buildCursor(node, depth + 1));
        return depth % 2 === 0 ? new OrCursor(children) : new AndCursor(children);
    }

    /**
     * Build a cursor over one stream index, bounded by the global revisions.
     *
     * @private
     * @param {ReadableIndex|undefined} index
     * @returns {IndexCursor|OrCursor}
     */
    buildIndexCursor(index) {
        if (!index || index.length === 0) {
            return new OrCursor([]);
        }
        const forwards = this.minRevision <= this.maxRevision;
        const minNumber = Math.min(this.minRevision, this.maxRevision);
        const maxNumber = Math.max(this.minRevision, this.maxRevision);
        return new IndexCursor(index, minNumber, maxNumber, forwards);
    }

    /**
     * Iterate storage documents lazily, in the requested direction, by walking the selector cursor.
     *
     * @private
     * @returns {Generator<object|{ buffer: Buffer, time64: number, sequenceNumber: number }>}
     */
    * iterateDocuments() {
        const forwards = this.minRevision <= this.maxRevision;
        const cursor = this.buildCursor(this.selector);
        for (; cursor.key !== EXHAUSTED; cursor.next()) {
            const { index, position } = cursor.leaf();
            const entry = index.peek(position);
            if (!entry) {
                return;
            }
            const event = this.eventStore.storage.readFrom(entry.partition, entry.position, entry.size, this.raw, !forwards);
            if (event) {
                yield event;
            }
        }
    }
}

export default JoinEventStream;
