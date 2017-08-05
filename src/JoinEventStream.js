const EventStream = require('./EventStream');

/**
 * An event stream is a simple wrapper around an iterator over storage documents.
 * It implements a node readable stream interface.
 */
class JoinEventStream extends EventStream {

    /**
     * @param {string} name The name of the stream.
     * @param {Array<string>} streams The name of the streams to join together.
     * @param {EventStore} eventStore The event store to get the stream from.
     * @param {number} [minRevision] The minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The maximum revision to include in the events (inclusive).
     */
    constructor(name, streams, eventStore, minRevision = 0, maxRevision = -1) {
        super(name, eventStore, minRevision = 0, maxRevision = -1);
        this._next = new Array(streams.length);
        this.iterator = streams.map(streamName => {
            let streamIndex = eventStore.streams[streamName].index;
            let from = minRevision > 0 ? streamIndex.find(minRevision) : 1;
            let until = maxRevision > 0 ? streamIndex.find(maxRevision) : 0;
            return eventStore.storage.readRange(from, until, streamIndex);
        });
    }

    /**
     * @private
     * @returns {Object|boolean} The next event or false if no more events in the stream.
     */
    next() {
        let nextIndex = -1;
        this._next.forEach((value, index) => {
            if (typeof value === 'undefined') {
                let next = this.iterator[index].next();
                value = this._next[index] = next.done ? false : next.value;
            }
            if (value === false) {
                return;
            }
            if (nextIndex === -1 || this._next[nextIndex].metadata.committedAt > value.metadata.committedAt) {
                nextIndex = index;
            }
        });
        if (nextIndex === -1) {
            return false;
        }
        let next = this._next[nextIndex];
        delete this._next[nextIndex];
        return next;
    }

}

module.exports = JoinEventStream;
