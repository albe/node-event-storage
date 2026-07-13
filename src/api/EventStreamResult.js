import { iterateStream } from './streamSource.js';

/**
 * Lazy result of {@link EventStreamQuery#query}.
 *
 * The {@link CommitCondition} (when available) and the effective range are
 * resolved eagerly and exposed synchronously; the events themselves are
 * iterated lazily. This mirrors the HTTP wire model where the condition lives in
 * a response header readable before the body stream begins.
 */
class EventStreamResult {
    /**
     * @param {{
     *   store: import('../EventStore.js').default,
     *   stream: import('../EventStream.js').default|null,
     *   condition: object|null,
     *   range: { from: number, until: number },
     *   source: import('./streamSource.js').StreamSource
     * }} init
     */
    constructor({ store, stream, condition, range, source, raw = false }) {
        this._store = store;
        this._stream = stream;
        this._condition = condition;
        this._range = range;
        this._source = source;
        this._raw = raw;
    }

    /**
     * Commit condition covering the types + matcher of this query, for OCC/DCB
     * commits. Null when the source does not support a condition.
     * @returns {object|null}
     */
    get condition() {
        return this._condition;
    }

    /**
     * The effective global-position range covered by this result.
     * @returns {{ from: number, until: number }}
     */
    get range() {
        return this._range;
    }

    /**
     * The source descriptor.
     * @returns {import('./streamSource.js').StreamSource}
     */
    get source() {
        return this._source;
    }

    /**
     * Lazy event delivery.
     * @returns {AsyncIterator<object>}
     */
    async *[Symbol.asyncIterator]() {
        yield* iterateStream(this._stream, this._store.typeAccessor, this._raw);
    }

    /**
     * Collect all matching events into an array.
     * @returns {Promise<object[]>}
     */
    async toArray() {
        return [...iterateStream(this._stream, this._store.typeAccessor, this._raw)];
    }
}

export default EventStreamResult;
