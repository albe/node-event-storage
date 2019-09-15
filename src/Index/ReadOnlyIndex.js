const fs = require('fs');
const ReadableIndex = require('./ReadableIndex');

/**
 * A read-only index is a readable index instance that reacts on file changes and triggers events.
 * If the underlying index was written to, an 'append' event is emitted, with the previous index length and the new index length.
 * If the underlying index was truncated, an 'truncate' event is emitted, with the previous index length and the new index length.
 */
class ReadOnlyIndex extends ReadableIndex {

    /**
     * @private
     */
    watchFile() {
        this.stopWatching();
        this.watcher = fs.watch(this.fileName, { persistent: false }, this.onChange.bind(this));
    }

    /**
     * @private
     */
    stopWatching() {
        if (this.watcher) {
            this.watcher.close();
            this.watcher = null;
        }
    }

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

    /**
     * Open the index if it is not already open.
     * This will open a file handle and either throw an error if the file is empty or read back the metadata and verify
     * it against the metadata provided in the constructor options.
     * It also creates a file watcher on the file to keep the internal buffer match the file size.
     *
     * @api
     * @returns {boolean} True if the index was opened or false if it was already open.
     * @throws {Error} if the file can not be opened.
     */
    open() {
        if (this.fd) {
            return false;
        }

        this.watchFile();
        return super.open();
    }

    /**
     * Close the index and release the file handle.
     * @api
     */
    close() {
        this.stopWatching();
        super.close();
    }
}

module.exports = ReadOnlyIndex;
