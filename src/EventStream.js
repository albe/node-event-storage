import stream from 'stream';
import { assert } from './utils/util.js';
import { buildRawBufferMatcher, matches } from './utils/metadataUtil.js';
import { normalizeRevision, normalizeMaxRevision } from './utils/apiHelpers.js';
import { emitDeprecationWarningOnce } from './utils/deprecations.js';

const NDJSON_NEWLINE = Buffer.from('\n');
const FILTER_MATCHER_DEPRECATION_CODE = 'EVENTSTREAM_FILTER_MATCHER_DEPRECATED';

/**
 * An event stream is a simple wrapper around an iterator over storage documents.
 * It implements a node readable stream interface.
 */
class EventStream extends stream.Readable {

    /**
     * @param {string} name The name of the stream.
     * @param {EventStore} eventStore The event store to get the stream from.
     * @param {number} [minRevision] The minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The maximum revision to include in the events (inclusive).
     * @param {function|object|null} [predicate] Optional matcher:
     *   - object mode: function `(payload, metadata) => boolean` or object matcher against `{ stream, payload, metadata }`
     *   - raw mode: function `(buffer) => boolean` or object matcher against compact NDJSON bytes.
     * @param {boolean} [raw=false] If true, emit NDJSON Buffers instead of event payload objects.
     */
    constructor(name, eventStore, minRevision = 1, maxRevision = -1, predicate = null, raw = false) {
        if (typeof predicate === 'boolean' && raw === false) {
            raw = predicate;
            predicate = null;
        }
        super({ objectMode: !raw });
        assert(typeof name === 'string' && name !== '', 'Need to specify a stream name.');
        assert(typeof eventStore === 'object' && eventStore !== null, `Need to provide EventStore instance to create EventStream ${name}.`);

        this.name = name;
        this.raw = raw;
        this.predicate = predicate || null;
        this.rawMatcher = null;
        if (eventStore.streams[name]) {
            this.streamIndex = eventStore.streams[name].index;
            this.minRevision = normalizeRevision(minRevision, this.streamIndex.length);
            this.maxRevision = normalizeRevision(maxRevision, this.streamIndex.length);
            this.version = normalizeMaxRevision(this.streamIndex.length, maxRevision);
            this._iterator = null;
            this.fetch = () => eventStore.storage.readRange(this.minRevision, this.maxRevision, this.streamIndex, raw);
        } else {
            this.streamIndex = { length: 0 };
            this.version = -1;
            this._iterator = { next() { return { done: true }; } };
        }
    }

    /**
     * @api
     * @param {number} revision The event revision to start reading from (inclusive).
     * @returns {EventStream}
     */
    from(revision) {
        this.minRevision = normalizeRevision(revision, this.streamIndex.length);
        return this;
    }

    /**
     * @api
     * @param {number} revision The event revision to read until (inclusive).
     * @returns {EventStream}
     */
    until(revision) {
        this.maxRevision = normalizeRevision(revision, this.streamIndex.length);
        this.version = normalizeMaxRevision(this.streamIndex.length, this.maxRevision);
        return this;
    }

    /**
     * @api
     * @param {number} amount The amount of events at the start of the stream to return in chronological order.
     * @returns {EventStream}
     */
    first(amount) {
        return this.fromStart().following(amount);
    }

    /**
     * @api
     * @param {number} amount The amount of events at the end of the stream to return in chronological order.
     * @returns {EventStream}
     */
    last(amount) {
        return this.fromEnd().previous(amount).forwards();
    }

    /**
     * @api
     * @returns {EventStream}
     */
    fromStart() {
        this.minRevision = 1;
        return this;
    }

    /**
     * @api
     * @returns {EventStream}
     */
    fromEnd() {
        this.minRevision = this.streamIndex.length;
        return this;
    }

    /**
     * @param {number} amount The amount of events to return in reverse chronological order.
     * @returns {EventStream}
     */
    previous(amount) {
        this.maxRevision = Math.max(1, this.minRevision - amount + 1);
        return this;
    }

    /**
     * @param {number} amount The amount of events to return in chronological order.
     * @returns {EventStream}
     */
    following(amount) {
        this.maxRevision = Math.min(this.streamIndex.length, this.minRevision + amount - 1);
        return this;
    }

    /**
     * @api
     * @returns {EventStream}
     */
    toEnd() {
        this.maxRevision = this.version = this.streamIndex.length;
        return this;
    }

    /**
     * @api
     * @returns {EventStream}
     */
    toStart() {
        this.maxRevision = 1;
        return this;
    }

    /**
     * Reverse the current range of events, no matter which direction it currently has.
     * @returns {EventStream}
     */
    reverse() {
        let tmp = this.maxRevision;
        this.maxRevision = this.minRevision;
        this.minRevision = tmp;
        this.version = normalizeMaxRevision(this.streamIndex.length, this.maxRevision);
        return this;
    }

    /**
     * Make the current range of events read in forward chronological order.
     * @api
     * @param {number} [amount] Amount of events to read forward. If not specified, will read forward until the previously set limit.
     * @returns {EventStream}
     */
    forwards(amount = 0) {
        if (amount > 0) {
            this.following(amount);
        }
        if (this.maxRevision < this.minRevision) {
            this.reverse();
        }
        return this;
    }

    /**
     * Make the current range of events read in backward chronological order.
     * @api
     * @param {number} [amount] Amount of events to read backward. If not specified, will read backward until the previously set limit.
     * @returns {EventStream}
     */
    backwards(amount = 0) {
        if (amount > 0) {
            this.previous(amount);
        }
        if (this.maxRevision > this.minRevision) {
            this.reverse();
        }
        return this;
    }

    /**
     * Will iterate over all events in this stream and return an array of the events.
     *
     * @returns {Array<object>}
     */
    get events() {
        if (this.cachedEvents instanceof Array) {
            return this.cachedEvents;
        }
        this.cachedEvents = [];
        let next;
        while ((next = this.next()) !== false) {
            this.cachedEvents.push(next.payload);
        }
        return this.cachedEvents;
    }

    /**
     * Iterate over the events in this stream with a callback.
     * This method is useful to gain access to the event metadata.
     *
     * @api
     * @param {function(object, object, string)} callback A callback function that will receive the event, the storage metadata and the original stream name for every event in this stream.
     */
    forEach(callback) {
        let next;
        while ((next = this.next()) !== false) {
            callback(next.payload, next.metadata, next.stream);
        }
    }

    /**
     * Iterator implementation. Iterate over the stream in a `for ... of` loop.
     */
    *[Symbol.iterator]() {
        let next;
        while ((next = this.next()) !== false) {
            yield this.raw ? this.toRawBuffer(next) : next.payload;
        }
    }

    /**
     * @param {{ buffer: Buffer }} entry
     * @returns {Buffer}
     */
    toRawBuffer(entry) {
        return Buffer.concat([entry.buffer, NDJSON_NEWLINE]);
    }

    /**
     * Reset this stream to the start so it can be iterated again.
     * @returns {EventStream}
     */
    reset() {
        this._iterator = null;
        this.cachedEvents = null;
        return this;
    }

    /**
     * Apply matcher semantics to this EventStream instance (legacy `filter()` behavior).
     *
     * @api
     * @param {(function(object, object): boolean)|(function(Buffer): boolean)|object|null} [predicate]
     *   Matcher function/object for object mode, or buffer matcher function/object for raw mode.
     *   Omit to clear the current predicate.
     * @returns {EventStream} `this`
     */
    where(predicate) {
        this.predicate = predicate || null;
        this.rawMatcher = null;
        this._iterator = null;
        this.cachedEvents = null;
        return this;
    }

    /**
     * Readable-compatible filter entry point.
     *
     * - `filter(callback, options)` delegates to Node's `Readable.filter(...)`.
     * - `filter(matcher)` keeps legacy EventStream matcher behavior and is deprecated.
     *
     * @api
     * @param {function|object|null} [predicate]
     * @param {{concurrency?: number, signal?: AbortSignal}|undefined} [options]
     * @returns {stream.Readable|EventStream}
     */
    filter(predicate, options) {
        if (options !== undefined) {
            return super.filter(predicate, options);
        }
        emitDeprecationWarningOnce(
            FILTER_MATCHER_DEPRECATION_CODE,
            'EventStream.filter() with matcher semantics is deprecated. Use EventStream.where() for matcher/object predicates or pass options to filter() to use Readable.filter().'
        );
        return this.where(predicate);
    }

    matchesPredicate(entry) {
        if (!this.predicate) {
            return true;
        }
        if (this.raw) {
            if (typeof this.predicate === 'function') {
                return this.predicate(entry.buffer);
            }
            if (!this.rawMatcher) {
                this.rawMatcher = buildRawBufferMatcher(this.predicate);
            }
            return this.rawMatcher(entry.buffer);
        }

        if (typeof this.predicate === 'function') {
            return this.predicate(entry.payload, entry.metadata);
        }
        return matches(entry, this.predicate);
    }

    /**
     * @returns {object|boolean} The next event or false if no more events in the stream.
     */
    next() {
        if (!this._iterator) {
            this._iterator = this.fetch();
        }
        try {
            while (true) {
                const result = this._iterator.next();
                if (result.done) return false;
                if (this.matchesPredicate(result.value)) {
                    return result.value;
                }
            }
        } catch(e) {
            return false;
        }
    }

    // noinspection JSUnusedGlobalSymbols
    /**
     * Readable stream implementation.
     * @private
     */
    _read() {
        const next = this.next();
        this.push(next ? (this.raw ? this.toRawBuffer(next) : next.payload) : null);
    }

}

export default EventStream;
