const ReadablePartition = require('./ReadablePartition');
const WatchesFile = require('../WatchesFile');

/**
 * A read-only partition is a readable partition that keeps it's size in sync with the underlying file.
 */
class ReadOnlyPartition extends WatchesFile(ReadablePartition) {

    /**
     * @inheritDoc
     */
    constructor(name, options = {}) {
        super(name, options);
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
        }
    }

}

module.exports = ReadOnlyPartition;