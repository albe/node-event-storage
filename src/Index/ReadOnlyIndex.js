const ReadableIndex = require('./ReadableIndex');
const WatchesFile = require('../WatchesFile');

/**
 * A read-only index is a readable index instance that reacts on file changes and triggers events.
 * If the underlying index was written to, an 'append' event is emitted, with the previous index length and the new index length.
 * If the underlying index was truncated, an 'truncate' event is emitted, with the previous index length and the new index length.
 */
class ReadOnlyIndex extends WatchesFile(ReadableIndex) {

    /**
     * @private
     * @param {string} eventType
     * @param {string} filename
     */
    onChange(eventType, filename) {
        if (eventType === 'change') {
            const prevLength = this.data.length;
            const length = this.readFileLength();
            this.data.length = length;
            if (length > prevLength) {
                this.emit('append', prevLength, length);
            }
            if (length < prevLength) {
                this.emit('truncate', prevLength, length);
            }
        }
        if (eventType === 'rename' && filename) {
            // Closes current watcher and creates a new watcher on the new filename
            this.close();
            this.fileName = filename;
            this.open();
        }
    }

}

module.exports = ReadOnlyIndex;
