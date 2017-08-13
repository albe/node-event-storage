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
        super(name, eventStore, minRevision, maxRevision);
        if (!(streams instanceof Array) || streams.length === 0) {
            throw new Error(`Invalid list of streams supplied to JoinStream ${name}.`);
        }
        this._next = new Array(streams.length).fill(undefined);

        // Translate revisions to index numbers (1-based) and wrap around negatives
        minRevision++;
        if (minRevision <= 0) minRevision += eventStore.length;
        maxRevision++;
        if (maxRevision <= 0) maxRevision += eventStore.length;

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
     * @private
     * @returns {Object|boolean} The next event or false if no more events in the stream.
     */
    next() {
        let nextIndex = -1;
        this._next.forEach((value, index) => {
            if (typeof value === 'undefined') {
                const next = this.iterator[index].next();
                value = this._next[index] = next.done ? false : next.value;
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
