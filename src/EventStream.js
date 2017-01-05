const Readable = require('stream').Readable;

/**
 * An event stream is a simple wrapper around an iterator over storage documents.
 * It implements a node readable stream interface.
 */
class EventStream extends Readable {

    /**
     * @param {string} name The name of the stream.
     * @param {Iterator<Object>|Generator<Object>} iterator The iterator to use for iterating over events.
     */
    constructor(name, iterator) {
        super({ objectMode: true });
        this.name = name;
        this.iterator = iterator || { next() { return { done: true } } };
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
     * @private
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
        let next = this.next();
        this.push(next ? next.payload : null);
    }
}

module.exports = EventStream;
