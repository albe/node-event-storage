const Readable = require('stream').Readable;

/**
 * An event stream is a simple wrapper around an iterator over storage documents.
 * It implements a node readable stream interface.
 */
class EventStream extends Readable {

    /**
     * @param {string} name The name of the stream.
     * @param {Iterable<Object>} iterator The iterator to use for iterating over events.
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
        if (this._events) {
            return this._events;
        }
        this._events = [];
        let next;
        while ((next = this.next()) !== false) {
            this._events.push(next);
        }
        return this._events;
    }

    /**
     * Iterator implementation. Iterate over the stream in a `for ... of` loop.
     */
    *[Symbol.iterator]() {
        let next;
        while ((next = this.next()) !== false) {
            yield next;
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
        return next.done ? false : next.value.payload;
    }

    /**
     * Readable stream implementation.
     * @private
     */
    _read() {
        let next = this.next();
        this.push(next ? next : null);
    }
}

module.exports = EventStream;
