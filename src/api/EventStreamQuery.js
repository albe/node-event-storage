import { CommitCondition } from '../EventStore.js';
import { assert } from '../utils/util.js';
import { normalizeSelector } from '../utils/indexUtil.js';
import { buildGlobalStream, conditionSelector, iterateStream } from './streamSource.js';
import RangeQuery from './RangeQuery.js';
import EventStreamResult from './EventStreamResult.js';

/**
 * Base fluent query descriptor over an unbounded, global-position-ordered event
 * stream. Returned by `EventStore.category()/join()`; `EventStore.stream()`
 * returns the {@link StreamQuery} subclass.
 *
 * Pre-mode modifiers (`from`, `matching`) are valid for every consumption mode.
 * Consumption modes:
 *   - default async/sync iteration: snapshot from `from` to the current tail, then end
 *   - `subscribe()`: continuous follow of the tail; never ends until aborted
 *   - `query()`: DCB-style read returning a CommitCondition + lazy events
 *   - `range()`: fork into a bounded {@link RangeQuery}
 *
 * `until()` / `backwards()` live only on RangeQuery, so they cannot be combined
 * with `subscribe()`/`query()`.
 */
class EventStreamQuery {
    /**
     * @param {import('../EventStore.js').default} store EventStore instance.
     * @param {import('./streamSource.js').StreamSource} source Source descriptor.
     */
    constructor(store, source) {
        this._store = store;
        this._source = source;
        this._from = 0;
        this._matcher = null;
        this._raw = false;
    }

    /**
     * Start reading from this global position (inclusive). Default 0 (beginning).
     * @param {number} position
     * @returns {this}
     */
    from(position) {
        this._from = position;
        return this;
    }

    /**
     * Filter events by matcher (payload/metadata fields).
     * @param {object|function} matcher
     * @returns {this}
     */
    matching(matcher) {
        this._matcher = matcher;
        return this;
    }

    /**
     * Yield raw NDJSON buffers instead of StoredEvents — skips JSON
     * deserialization for throughput-critical bulk reads. Not available on
     * `subscribe()` (which needs positions to advance its cursor).
     * @returns {this}
     */
    raw() {
        this._raw = true;
        return this;
    }

    /**
     * The inclusive global position to read from, with the `from(0)` default
     * mapped to 1.
     * @private
     * @returns {number}
     */
    _fromPosition() {
        return this._from > 0 ? this._from : 1;
    }

    /**
     * Snapshot iteration: from `from` to the current tail, then end.
     * @returns {Iterator<object>}
     */
    [Symbol.iterator]() {
        const until = this._store.length;
        const from = this._fromPosition();
        const stream = until < from ? null : buildGlobalStream(this._store, this._source, this._matcher, from, until, this._raw);
        return iterateStream(stream, this._store.typeAccessor, this._raw);
    }

    /**
     * Snapshot iteration (async).
     * @returns {AsyncIterator<object>}
     */
    async *[Symbol.asyncIterator]() {
        yield* this[Symbol.iterator]();
    }

    /**
     * Collect the snapshot into an array.
     * @returns {Promise<object[]>}
     */
    async toArray() {
        return [...this[Symbol.iterator]()];
    }

    /**
     * Fork into bounded-read mode, addressed by global position.
     * @returns {RangeQuery}
     */
    range() {
        return new RangeQuery(this._store, this._source, {
            from: this._from,
            matcher: this._matcher,
            basis: 'global',
            raw: this._raw
        });
    }

    /**
     * DCB-style lazy read. Always returns an {@link EventStreamResult} carrying a
     * {@link CommitCondition} — a join over the selection streams captured at the
     * current global position — regardless of source kind.
     * @returns {Promise<EventStreamResult>}
     */
    async query() {
        const until = this._store.length;
        const from = this._fromPosition();
        const stream = until < from ? null : buildGlobalStream(this._store, this._source, this._matcher, from, until, this._raw);
        const condition = new CommitCondition(conditionSelector(this._store, this._source), this._matcher, until);
        return new EventStreamResult({
            store: this._store,
            stream,
            condition,
            range: { from, until },
            source: this._source,
            raw: this._raw
        });
    }

    /**
     * Arm a one-shot waiter that resolves on the next commit (or new stream),
     * or when the signal aborts.
     * @private
     * @param {AbortSignal} [signal]
     * @returns {Promise<void>}
     */
    _nextCommit(signal) {
        return new Promise(resolve => {
            const cleanup = () => {
                this._store.removeListener('commit', onChange);
                this._store.removeListener('stream-created', onChange);
                this._store.removeListener('stream-available', onChange);
                signal?.removeEventListener('abort', onChange);
            };
            const onChange = () => {
                cleanup();
                resolve();
            };
            this._store.on('commit', onChange);
            this._store.on('stream-created', onChange);
            this._store.on('stream-available', onChange);
            signal?.addEventListener('abort', onChange, { once: true });
        });
    }

    /**
     * Continuous subscription. Delivers the backlog from `from` to the current
     * tail, then yields newly committed matching events as they arrive. Never
     * ends until `signal` is aborted.
     *
     * Each yielded event carries `globalPosition`; persist it as your checkpoint.
     * Resume with `.from(checkpoint)` for at-least-once, or `.from(checkpoint + 1)`
     * for exactly-once delivery.
     *
     * @param {{ signal?: AbortSignal }} [options]
     * @yields {object} StoredEvent
     * @returns {AsyncGenerator<object, void, undefined>}
     */
    async *subscribe({ signal } = {}) {
        assert(!this._raw, 'raw() is not supported with subscribe(); raw buffers carry no position to advance the cursor.');
        let from = this._fromPosition();
        while (true) {
            if (signal?.aborted) {
                return;
            }
            const waitNext = this._nextCommit(signal);
            const until = this._store.length;
            if (until >= from) {
                const stream = buildGlobalStream(this._store, this._source, this._matcher, from, until);
                for (const event of iterateStream(stream, this._store.typeAccessor)) {
                    yield event;
                    from = event.globalPosition + 1;
                    if (signal?.aborted) {
                        return;
                    }
                }
            }
            await waitNext;
        }
    }
}

/**
 * Fluent query for a single named stream. Adds {@link StreamQuery#streamRange} —
 * the stream-relative, version-addressed bounded read. This basis is only valid
 * for a single stream, and reading by stream version is faster than a global
 * position range, so it doubles as an explicit performance opt-in.
 */
class StreamQuery extends EventStreamQuery {
    /**
     * Fork into a bounded read addressed by stream-relative revision (1-based)
     * rather than global position. Use for classic aggregate-stream consistency
     * and for faster single-stream range reads.
     * @returns {RangeQuery}
     */
    streamRange() {
        return new RangeQuery(this._store, this._source, {
            from: this._from,
            matcher: this._matcher,
            basis: 'stream'
        });
    }
}

export default EventStreamQuery;
export { EventStreamQuery, StreamQuery };
