const EventStream = require('./EventStream');

/**
 * Translates an EventStore revision number to an index sequence number and wraps it around the given length, if it's < 0
 *
 * @param {number} rev The zero-based EventStore revision
 * @param {number} length The length of the store
 * @returns {number} The 1-based index sequence number
 */
function wrapRevision(rev, length) {
    rev++;
    if (rev <= 0) {
        rev += length;
    }
    return rev;
}

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
        super(name, eventStore, minRevision, maxRevision);
        if (!(streams instanceof Array) || streams.length === 0) {
            throw new Error(`Invalid list of streams supplied to JoinStream ${name}.`);
        }
        this._next = new Array(streams.length).fill(undefined);

        // Translate revisions to index numbers (1-based) and wrap around negatives
        minRevision = wrapRevision(minRevision, eventStore.length);
        maxRevision = wrapRevision(maxRevision, eventStore.length);

        this.iterator = streams.map(streamName => {
            if (!eventStore.streams[streamName]) {
                return { next() { return { done: true }; } };
            }
            const streamIndex = eventStore.streams[streamName].index;
            const from = streamIndex.find(minRevision, true);
            const until = streamIndex.find(maxRevision);
            return eventStore.storage.readRange(from || 1, until, streamIndex);
        });
    }

    /**
     * Returns the value of the iterator at position `index`
     * @param {number} index The iterator position for which to return the next value
     * @returns {*}
     */
    getValue(index) {
        const next = this.iterator[index].next();
        return next.done ? false : next.value;
    }

    /**
     * @private
     * @returns {Object|boolean} The next event or false if no more events in the stream.
     */
    next() {
        let nextIndex = -1;
        this._next.forEach((value, index) => {
            if (typeof value === 'undefined') {
                value = this._next[index] = this.getValue(index);
            }
            if (value === false) {
                return;
            }
            if (nextIndex === -1 || this._next[nextIndex].metadata.commitId > value.metadata.commitId) {
                nextIndex = index;
            }
        });

        if (nextIndex === -1) {
            return false;
        }
        const next = this._next[nextIndex];
        this._next[nextIndex] = undefined;
        return next;
    }

}

module.exports = JoinEventStream;
