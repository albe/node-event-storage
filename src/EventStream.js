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
            this.storage = eventStore.storage;
        } else {
            this.version = -1;
            this.iterator = { next() { return { done: true }; } };
        }
    }

    /**
     * @param {number} revision
     * @returns {EventStream}
     */
    from(revision) {
        this.minRevision = normalizeVersion(revision, this.streamIndex.length);
        return this;
    }

    /**
     * @param {number} revision
     * @returns {EventStream}
     */
    until(revision) {
        this.maxRevision = normalizeVersion(revision, this.streamIndex.length);
        this.version = minVersion(this.streamIndex.length, this.maxRevision);
        return this;
    }

    /**
     * @param {number} amount
     * @returns {EventStream}
     */
    first(amount) {
        return this.fromStart().following(amount);
    }

    /**
     * @param {number} amount
     * @returns {EventStream}
     */
    last(amount) {
        return this.fromEnd().previous(amount).forwards();
    }

    /**
     * @returns {EventStream}
     */
    fromStart() {
        this.minRevision = 1;
        return this;
    }

    /**
     * @returns {EventStream}
     */
    fromEnd() {
        this.minRevision = this.streamIndex.length;
        return this;
    }

    /**
     * @param {number} amount
     * @returns {EventStream}
     */
    previous(amount) {
        this.maxRevision = Math.max(1, this.minRevision - amount + 1);
        return this;
    }

    /**
     * @param {number} amount
     * @returns {EventStream}
     */
    following(amount) {
        this.maxRevision = Math.min(this.streamIndex.length, this.minRevision + amount - 1);
        return this;
    }

    /**
     * @returns {EventStream}
     */
    toEnd() {
        this.maxRevision = this.version = this.streamIndex.length;
        return this;
    }

    /**
     * @returns {EventStream}
     */
    toStart() {
        this.maxRevision = 1;
        return this;
    }

    /**
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
     * @returns {EventStream}
     */
    forwards() {
        if (this.maxRevision < this.minRevision) {
            this.reverse();
        }
        return this;
    }

    /**
     * @returns {EventStream}
     */
    backwards() {
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
     * @returns {object|boolean} The next event or false if no more events in the stream.
     */
    next() {
        if (!this.iterator) {
            this.iterator = this.storage.readRange(this.minRevision, this.maxRevision, this.streamIndex);
        }
        let next;
        try {
            next = this.iterator.next();
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
