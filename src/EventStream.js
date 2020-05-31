const stream = require('stream');
const { assert } = require('./util');

/**
 * Adjusts a revision number from the EventStore/EventStream interface range into the underlying storage item number.
 *
 * @param {number} rev A zero-based revision number, or a negative number to denote a "from the end" position
 * @returns {number} A one-based storage item number
 */
function adjustedRevision(rev) {
    if (rev >= 0) {
        return rev + 1;
    }
    return rev;
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
    constructor(name, eventStore, minRevision = 0, maxRevision = -1) {
        super({ objectMode: true });
        assert(typeof name === 'string' && name !== '', 'Need to specify a stream name.');
        assert(typeof eventStore === 'object' && eventStore !== null, `Need to provide EventStore instance to create EventStream ${name}.`);

        this.name = name;
        if (eventStore.streams[name]) {
            const streamIndex = eventStore.streams[name].index;
            this.version = minVersion(streamIndex.length, maxRevision);
            minRevision = adjustedRevision(minRevision);
            maxRevision = adjustedRevision(maxRevision);
            this.iterator = eventStore.storage.readRange(minRevision, maxRevision, streamIndex);
        } else {
            this.version = -1;
            this.iterator = { next() { return { done: true }; } };
        }
    }

    /**
     * Will iterate over all events in this stream and return an array of the events.
     *
     * @returns {Array<Object>}
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
     * @param {function(Object, Object, string)} callback A callback function that will receive the event, the storage metadata and the original stream name for every event in this stream.
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
     * @returns {Object|boolean} The next event or false if no more events in the stream.
     */
    next() {
        let next;
        try {
            next = this.iterator.next();
        } catch(e) {
            return false;
        }
        return next.done ? false : next.value;
    }

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
