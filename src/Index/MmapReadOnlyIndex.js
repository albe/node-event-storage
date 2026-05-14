import fs from 'fs';
import watchesFile from '../WatchesFile.js';
import MmapReadableIndex from './MmapReadableIndex.js';

const REMAP_SHRINK_THRESHOLD_RATIO = 4;

/**
 * A read-only index is a readable index instance that reacts on file changes and triggers events.
 * If the underlying index was written to, an 'append' event is emitted, with the previous index length and the new index length.
 * If the underlying index was truncated, a 'truncate' event is emitted, with the previous index length and the new index length.
 */
class MmapReadOnlyIndex extends watchesFile(MmapReadableIndex) {

    /**
     * @private
     * @param {string} filename
     */
    onChange(filename) {
        /* istanbul ignore if */
        if (!this.fd) {
            return;
        }

        const previousLength = this.lengthValue;
        const nextLength = this.readFileLength();
        const requiredMapSize = this.bytesForEntries(nextLength);

        let nextMapSize = this.mappedSize;
        if (requiredMapSize > this.mappedSize) {
            nextMapSize = requiredMapSize + this.mapGrowthBytes;
        } else if (requiredMapSize < this.mappedSize && requiredMapSize * REMAP_SHRINK_THRESHOLD_RATIO < this.mappedSize) {
            // Shrink only after a significant size drop to avoid frequent remap thrashing.
            nextMapSize = requiredMapSize + this.mapGrowthBytes;
        }

        if (nextMapSize !== this.mappedSize) {
            const stat = fs.fstatSync(this.fd);
            const maxMapSize = Math.max(requiredMapSize, stat.size);
            this.mapEntries(Math.min(nextMapSize, maxMapSize), false);
        }

        this.lengthValue = nextLength;

        if (nextLength > previousLength) {
            this.emit('append', previousLength, nextLength);
        }
        if (nextLength < previousLength) {
            this.emit('truncate', previousLength, nextLength);
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

export default MmapReadOnlyIndex;
