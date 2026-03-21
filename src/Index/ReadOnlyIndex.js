const ReadableIndex = require('./ReadableIndex');
const watchesFile = require('../WatchesFile');

/**
 * A read-only index is a readable index instance that reacts on file changes and triggers events.
 * If the underlying index was written to, an 'append' event is emitted, with the previous index length and the new index length.
 * If the underlying index was truncated, a 'truncate' event is emitted, with the previous index length and the new index length.
 */
class ReadOnlyIndex extends watchesFile(ReadableIndex) {

    /**
     * @inheritDoc
     */
    constructor(name, options) {
        super(name, options);
    }

    /**
     * @private
     * @param {string} filename
     */
    onChange(filename) {
        /* istanbul ignore if */
        if (!this.fd) {
            return;
        }
        const prevLength = this._length;
        const newLength = this.readFileLength();
        if (newLength < prevLength) {
            // Clear ring buffer slots for the removed entries to avoid stale reads
            const oldCacheStart = Math.max(0, prevLength - this.cacheSize);
            for (let i = Math.max(oldCacheStart, newLength); i < prevLength; i++) {
                this.cache[i % this.cacheSize] = null;
            }
        }
        this._length = newLength;
        if (newLength > prevLength) {
            this.emit('append', prevLength, newLength);
        }
        if (newLength < prevLength) {
            this.emit('truncate', prevLength, newLength);
        }
    }

    /**
     * @private
     * @param {string} filename
     */
    onRename(filename) {
        this.close();
    }

}

module.exports = ReadOnlyIndex;
