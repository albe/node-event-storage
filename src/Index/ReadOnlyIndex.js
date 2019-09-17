const path = require('path');
const ReadableIndex = require('./ReadableIndex');
const WatchesFile = require('../WatchesFile');

/**
 * A read-only index is a readable index instance that reacts on file changes and triggers events.
 * If the underlying index was written to, an 'append' event is emitted, with the previous index length and the new index length.
 * If the underlying index was truncated, an 'truncate' event is emitted, with the previous index length and the new index length.
 */
class ReadOnlyIndex extends WatchesFile(ReadableIndex) {

    /**
     * @inheritDoc
     */
    constructor(name, options) {
        super(name, options);
    }

    /**
     * @private
     * @param {string} eventType
     * @param {string} filename
     */
    onChange(eventType, filename) {
        if (!this.fd) {
            return;
        }
        if (eventType === 'change') {
            const prevLength = this.data.length;
            const length = this.readFileLength();
            this.data.length = length;
            this.emitFileChange(prevLength, length);
        } else if (eventType === 'rename' && filename) {
            this.close();
        }
    }

    /**
     * @private
     * @param {number} prevLength
     * @param {number} newLength
     */
    emitFileChange(prevLength, newLength) {
        if (newLength > prevLength) {
            this.emit('append', prevLength, newLength);
        }
        if (newLength < prevLength) {
            this.emit('truncate', prevLength, newLength);
        }
    }

}

module.exports = ReadOnlyIndex;
