import watchesFile from '../WatchesFile.js';
import MmapReadableIndex from './MmapReadableIndex.js';

class MmapReadOnlyIndex extends watchesFile(MmapReadableIndex) {
    onChange() {
        if (!this.fd) {
            return;
        }
        const prevLength = this.lengthValue;
        const length = this.readFileLength();
        const requiredMapSize = this.bytesForEntries(length);
        if (requiredMapSize !== this.mappedSize) {
            this.mapEntries(requiredMapSize, false);
        }
        this.lengthValue = length;
        this.lastNumber = length > 0 ? this.read(length).number : 0;
        if (length > prevLength) {
            this.emit('append', prevLength, length);
        }
        if (length < prevLength) {
            this.emit('truncate', prevLength, length);
        }
    }

    onRename() {
        this.close();
    }
}

export default MmapReadOnlyIndex;
