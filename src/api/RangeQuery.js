import { assert } from '../utils/util.js';
import { buildGlobalStream, buildRelativeStream, iterateStream } from './streamSource.js';

/**
 * Bounded, directional read descriptor. Entered via `EventStreamQuery.range()`.
 *
 * Unlike EventStreamQuery, a RangeQuery has an explicit upper bound and/or
 * direction, so it deliberately does NOT expose `subscribe()` or `query()` —
 * those are open-ended operations incompatible with a bounded range.
 *
 * Position basis:
 *   - `'global'` (default): `from`/`until` are global positions. Valid for any source.
 *   - `'stream'`: `from`/`until` are stream-relative revisions. Valid only for a
 *     single `stream(...)` source. This is the explicit escape hatch for classic
 *     aggregate-stream consistency, where the differing number system is made
 *     unambiguous by the `basis` option.
 *
 * All bounds are inclusive on both ends.
 */
class RangeQuery {
    /**
     * @param {import('../EventStore.js').default} store EventStore instance.
     * @param {import('./streamSource.js').StreamSource} source Source descriptor.
     * @param {{ from?: number, matcher?: object|function|null, basis?: 'global'|'stream' }} [options]
     */
    constructor(store, source, { from = 0, matcher = null, basis = 'global', raw = false } = {}) {
        assert(basis === 'global' || basis === 'stream', `Invalid range basis "${basis}". Use 'global' or 'stream'.`);
        if (basis === 'stream') {
            assert(source.kind === 'stream', "Stream-relative range basis is only valid for a single stream(...) source.");
        }
        this._store = store;
        this._source = source;
        this._basis = basis;
        this._matcher = matcher;
        this._raw = raw;
        this._from = from > 0 ? from : undefined;
        this._until = undefined;
        this._backwardsN = null;
        this._reverse = false;
        this._limit = null;
    }

    /**
     * Set the inclusive lower bound (or the high anchor for backwards reads).
     * @param {number} position
     * @returns {RangeQuery}
     */
    from(position) {
        this._from = position;
        return this;
    }

    /**
     * Filter events by matcher (payload/metadata fields).
     * @param {object|function} matcher
     * @returns {RangeQuery}
     */
    matching(matcher) {
        this._matcher = matcher;
        return this;
    }

    /**
     * Set the inclusive upper bound. Direction is inferred from from/until.
     * @param {number} position
     * @returns {RangeQuery}
     */
    until(position) {
        this._until = position;
        return this;
    }

    /**
     * Alias for {@link RangeQuery#until}.
     * @param {number} position
     * @returns {RangeQuery}
     */
    to(position) {
        return this.until(position);
    }

    /**
     * Limit the read to the first `n` events (forward count cap).
     * @param {number} n
     * @returns {RangeQuery}
     */
    first(n) {
        return this.limit(n);
    }

    /**
     * Cap the read to at most `n` events.
     * @param {number} n
     * @returns {RangeQuery}
     */
    limit(n) {
        assert(Number.isInteger(n) && n > 0, 'limit(n) requires a positive integer.');
        this._limit = n;
        return this;
    }

    /**
     * Read backwards. With an argument `n`, read exactly `n` events backwards
     * from `from` (or the tail when `from` is unset). With no argument, reverse
     * the direction of the configured from/until range.
     * @param {number} [n]
     * @returns {RangeQuery}
     */
    backwards(n) {
        if (n === undefined) {
            this._reverse = true;
        } else {
            assert(Number.isInteger(n) && n > 0, 'backwards(n) requires a positive integer.');
            this._backwardsN = n;
        }
        return this;
    }

    /**
     * The effective length of the addressed space (global log length, or the
     * single stream's length for stream basis).
     * @private
     * @returns {number}
     */
    _length() {
        if (this._basis === 'stream') {
            const version = this._store.getStreamVersion(this._source.name);
            return version < 0 ? 0 : version;
        }
        return this._store.length;
    }

    /**
     * Resolve the configured modifiers into concrete `from`/`until` bounds plus
     * an optional event-count limit. `from > until` denotes a backwards read,
     * which the underlying engine serves directly (descending), so no client-side
     * reversal is needed.
     *
     * For `backwards(n)` the read spans the full source backwards from the anchor
     * (so the count is taken over actual source events, not raw global positions,
     * which may be sparse for a category/join), and `limit` caps it.
     * @private
     * @returns {{ from: number, until: number, limit: number }}
     */
    _resolveBounds() {
        const length = this._length();
        if (this._backwardsN !== null) {
            return { from: this._from ?? length, until: 1, limit: this._backwardsN };
        }
        if (this._reverse) {
            const lower = this._from ?? 1;
            const upper = this._until ?? length;
            return { from: upper, until: lower, limit: this._limit ?? Infinity };
        }
        return { from: this._from ?? 1, until: this._until ?? length, limit: this._limit ?? Infinity };
    }

    /**
     * Build the underlying stream for the given bounds.
     * @private
     * @param {number} from
     * @param {number} until
     * @returns {import('../EventStream.js').default|null|false}
     */
    _build(from, until) {
        if (this._basis === 'stream') {
            return buildRelativeStream(this._store, this._source.name, this._matcher, from, until, this._raw);
        }
        return buildGlobalStream(this._store, this._source, this._matcher, from, until, this._raw);
    }

    /**
     * Iterate the resolved range, applying the optional count limit.
     * @private
     * @yields {object}
     */
    *_iterate() {
        const { from, until, limit } = this._resolveBounds();
        if (this._length() === 0) {
            return;
        }
        let count = 0;
        for (const event of iterateStream(this._build(from, until), this._store.typeAccessor, this._raw)) {
            if (count >= limit) {
                return;
            }
            count++;
            yield event;
        }
    }

    /**
     * Synchronous iteration — in-process only, zero promise overhead.
     * @returns {Iterator<object>}
     */
    [Symbol.iterator]() {
        return this._iterate();
    }

    /**
     * Asynchronous iteration.
     * @returns {AsyncIterator<object>}
     */
    async *[Symbol.asyncIterator]() {
        yield* this._iterate();
    }

    /**
     * Collect all events into an array.
     * @returns {Promise<object[]>}
     */
    async toArray() {
        return [...this._iterate()];
    }
}

export default RangeQuery;
