const stream = require('stream');
const { assert } = require('./util');

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
 * Return the lower absolute version given a version and a maxVersion constraint.
 * @param {number} version
 * @param {number} maxVersion
 * @returns {number}
 */
function minVersion(version, maxVersion) {
    return Math.min(version, maxVersion < 0 ? version + maxVersion + 1 : maxVersion);
}

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
     */
    constructor(name, eventStore, minRevision = 1, maxRevision = -1) {
        super({ objectMode: true });
        assert(typeof name === 'string' && name !== '', 'Need to specify a stream name.');
        assert(typeof eventStore === 'object' && eventStore !== null, `Need to provide EventStore instance to create EventStream ${name}.`);

        this.name = name;
        if (eventStore.streams[name]) {
            this.streamIndex = eventStore.streams[name].index;
            this.minRevision = normalizeVersion(minRevision, this.streamIndex.length);
            this.maxRevision = normalizeVersion(maxRevision, this.streamIndex.length);
            this.version = minVersion(this.streamIndex.length, maxRevision);
            this._iterator = null;
            this.fetch = function() {
                return eventStore.storage.readRange(this.minRevision, this.maxRevision, this.streamIndex);
            }
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
        this.minRevision = normalizeVersion(revision, this.streamIndex.length);
        return this;
    }

    /**
     * @api
     * @param {number} revision The event revision to read until (inclusive).
     * @returns {EventStream}
     */
    until(revision) {
        this.maxRevision = normalizeVersion(revision, this.streamIndex.length);
        this.version = minVersion(this.streamIndex.length, this.maxRevision);
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
        this.version = minVersion(this.streamIndex.length, this.maxRevision);
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
        if (this._events instanceof Array) {
            return this._events;
        }
        this._events = [];
        let next;
        while ((next = this.next()) !== false) {
            this._events.push(next.payload);
        }
        return this._events;
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
            yield next.payload;
        }
    }

    /**
     * Reset this stream to the start so it can be iterated again.
     * @returns {EventStream}
     */
    reset() {
        this._iterator = null;
        this._events = null;
        return this;
    }

    /**
     * @returns {object|boolean} The next event or false if no more events in the stream.
     */
    next() {
        if (!this._iterator) {
            this._iterator = this.fetch();
        }
        let next;
        try {
            next = this._iterator.next();
        } catch(e) {
            return false;
        }
        return next.done ? false : next.value;
    }

    // noinspection JSUnusedGlobalSymbols
    /**
     * Readable stream implementation.
     * @private
     */
    _read() {
        const next = this.next();
        this.push(next ? next.payload : null);
    }

}

module.exports = EventStream;
