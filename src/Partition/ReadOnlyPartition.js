import ReadablePartition from './ReadablePartition.js';
import WatchesFile from '../WatchesFile.js';

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
        /* c8 ignore next 3 */
        if (!this.isOpen()) {
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

export default ReadOnlyPartition;