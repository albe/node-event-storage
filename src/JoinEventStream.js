const EventStream = require('./EventStream');
const { wrapAndCheck } = require('./util');

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
 * An event stream is a simple wrapper around an iterator over storage documents.
 * It implements a node readable stream interface.
 */
class JoinEventStream extends EventStream {

    /**
     * @param {string} name The name of the stream.
     * @param {Array<string>} streams The name of the streams to join together.
     * @param {EventStore} eventStore The event store to get the stream from.
     * @param {number} [minRevision] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The 1-based maximum revision to include in the events (inclusive).
     */
    constructor(name, streams, eventStore, minRevision = 1, maxRevision = -1) {
        super(name, eventStore, minRevision, maxRevision);
        if (!(streams instanceof Array) || streams.length === 0) {
            throw new Error(`Invalid list of streams supplied to JoinStream ${name}.`);
        }

        this.streamIndex = eventStore.storage.index;
        // Translate revisions to index numbers (1-based) and wrap around negatives
        this.minRevision = normalizeVersion(minRevision, eventStore.length);
        this.maxRevision = normalizeVersion(maxRevision, eventStore.length);
        this.fetch = function() {
            this._next = new Array(streams.length).fill(undefined);
            return streams.map(streamName => {
                if (!eventStore.streams[streamName]) {
                    return { next() { return { done: true }; } };
                }
                const streamIndex = eventStore.streams[streamName].index;
                const from = streamIndex.find(this.minRevision, this.minRevision <= this.maxRevision);
                const until = streamIndex.find(this.maxRevision, this.minRevision > this.maxRevision);
                return eventStore.storage.readRange(from, until, streamIndex);
            });
        }
        this.iterator = null;
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
     * @param {number} first
     * @param {number} second
     * @returns {boolean} If the first item follows after the second in the given read order determined by this.minRevision and this.maxRevision.
     */
    follows(first, second) {
        return (this.minRevision > this.maxRevision ? first < second : first > second);
    }

    /**
     * @private
     * @returns {object|boolean} The next event or false if no more events in the stream.
     */
    next() {
        if (!this.iterator) {
            this.iterator = this.fetch();
        }
        let nextIndex = -1;
        this._next.forEach((value, index) => {
            if (typeof value === 'undefined') {
                value = this._next[index] = this.getValue(index);
            }
            if (value === false) {
                return;
            }
            if (nextIndex === -1 || this.follows(this._next[nextIndex].metadata.commitId, value.metadata.commitId)) {
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
