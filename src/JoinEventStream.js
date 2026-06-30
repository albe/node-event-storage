import EventStream from './EventStream.js';
import { assert, iterate } from './utils/util.js';
import { union, intersect } from './utils/indexUtil.js';
import { normalizeRevision } from './utils/apiHelpers.js';

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
        this.selector = this.normalizeSelector(selector);
        this.streamIndex = eventStore.storage.index;
        this.minRevision = normalizeRevision(minRevision, eventStore.length);
        this.maxRevision = normalizeRevision(maxRevision, eventStore.length);
        this.version = this.streamIndex.length;

        this._combinedRanges = null;

        this.fetch = () => this.iterateDocuments();
        this._iterator = null;
    }

    /**
     * Normalize selector shape to nested arrays and validate leaf types.
     * Missing streams are allowed here to keep direct JoinEventStream construction compatible;
     * they are treated as empty ranges.
     *
     * @private
     * @param {Array<string|Array>} selector
     * @returns {Array<string|Array>}
     */
    normalizeSelector(selector) {
        const normalized = [];
        for (const node of selector) {
            if (typeof node === 'string') {
                assert(node.length > 0, 'Stream names must be non-empty strings.');
                normalized.push(node);
                continue;
            }

            assert(Array.isArray(node) && node.length > 0, 'Each selector node must be a non-empty stream name array or string.');
            normalized.push(this.normalizeSelector(node));
        }
        return normalized;
    }

    /**
     * Resolve and cache the combined index-entry ranges for the full selector.
     *
     * @private
     * @returns {Array<Array<number>>}
     */
    resolveCombinedRanges() {
        if (this._combinedRanges) {
            return this._combinedRanges;
        }

        this._combinedRanges = this.resolveSelectorRanges(this.selector);
        return this._combinedRanges;
    }

    /**
     * Optimize a selector tree at the given depth.
     * Currently, optimizes away occurrences of _all.
     *
     * @private
     * @param {Array<string|Array>} selectorNode
     * @param {number} depth
     * @returns {Array<Array<number>>|string}
     */
    optimize(selectorNode, depth) {
        if (depth % 2 !== 0) {
            return selectorNode.filter(node => node !== '_all');
        }
        if (selectorNode.some(node => node === '_all')) {
            return '_all';
        }
        return selectorNode;
    }

    /**
     * Resolve one selector node into a sorted index-entry range.
     *
     * @private
     * @param {string|Array<string|Array>} selectorNode
     * @param {number} [depth=0]
     * @returns {Array<Array<number>>}
     */
    resolveSelectorRanges(selectorNode, depth = 0) {
        if (typeof selectorNode === 'string') {
            const index = this.eventStore.streams[selectorNode]?.index;
            return this.resolveIndexRange(index);
        }
        selectorNode = this.optimize(selectorNode, depth);

        const childRanges = selectorNode.map(node => this.resolveSelectorRanges(node, depth + 1));
        return depth % 2 === 0 ? union(...childRanges) : intersect(...childRanges);
    }

    /**
     * Resolve one stream index to the global revision-bounded entry range.
     *
     * @private
     * @param {object|undefined} index
     * @returns {Array<Array<number>>}
     */
    resolveIndexRange(index) {
        if (!index || index.length === 0) {
            return [];
        }

        const ascending = this.minRevision <= this.maxRevision;
        const from = index.find(this.minRevision, ascending);
        const until = index.find(this.maxRevision, !ascending);
        if (
            from === 0 ||
            until === 0 ||
            (ascending ? from > until : from < until)
        ) {
            return [];
        }

        const rangeFrom = ascending ? from : until;
        const rangeUntil = ascending ? until : from;
        return index.range(rangeFrom, rangeUntil) || [];
    }

    /**
     * Iterate matching index entries in requested direction.
     *
     * @private
     * @returns {Generator<Array<number>>}
     */
    *iterateEntries() {
        const entries = this.resolveCombinedRanges();
        const forwards = this.minRevision <= this.maxRevision;
        yield* iterate(entries, forwards);
    }

    /**
     * Iterate storage documents lazily from combined index entries.
     *
     * @private
     * @returns {Generator<object|{ buffer: Buffer, time64: number, sequenceNumber: number }>}
     */
    *iterateDocuments() {
        const forwards = this.minRevision <= this.maxRevision;
        for (const entry of this.iterateEntries()) {
            const event = this.eventStore.storage.readFrom(
                entry.partition,
                entry.position,
                entry.size,
                this.raw,
                !forwards
            );
            if (event) {
                yield event;
            }
        }
    }

    /**
     * Reset fetch state and cached selector ranges when stream boundaries changed.
     *
     * @returns {JoinEventStream}
     */
    reset() {
        this._combinedRanges = null;
        return super.reset();
    }
}

export default JoinEventStream;
