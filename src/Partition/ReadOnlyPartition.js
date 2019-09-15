const fs = require('fs');
const ReadablePartition = require('./ReadablePartition');

/**
 * A read-only partition is a readable partition that keeps it's size in sync with the underlying file.
 */
class ReadOnlyPartition extends ReadablePartition {

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
            const prevSize = this.size;
            this.size = this.readFileSize();
            if (this.size > prevSize) {
                this.emit('append', prevSize, this.size);
            }
            if (this.size < prevSize) {
                this.emit('truncate', prevSize, this.size);
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
     * Open the partition storage and create read and write buffers.
     *
     * @api
     * @returns {boolean}
     */
    open() {
        if (this.fd) {
            return true;
        }

        this.watchFile();
        return super.open();
    }

    /**
     * Close the partition and frees up all resources.
     *
     * @api
     * @returns void
     */
    close() {
        this.stopWatching();
        super.close();
    }

}

module.exports = ReadOnlyPartition;