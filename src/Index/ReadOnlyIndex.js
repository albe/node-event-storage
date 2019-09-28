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
        const prevLength = this.data.length;
        const newLength = this.readFileLength();
        this.data.length = newLength;
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
