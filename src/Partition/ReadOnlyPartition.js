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
     * @param {string} filename
     */
    onChange(filename) {
        if (!this.fd) {
            return;
        }
        const prevSize = this.size;
        this.size = this.readFileSize();
        if (this.size > prevSize) {
            this.emit('append', prevSize, this.size);
        }
        if (this.size < prevSize) {
            this.emit('truncate', prevSize, this.size);
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

module.exports = ReadOnlyPartition;