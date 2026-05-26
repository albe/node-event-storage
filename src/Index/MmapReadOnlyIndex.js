import { ReadOnlyAppendOnlyMmapedFile } from '../File/AppendOnlyMmapedFile.js';
import MmapReadableIndex from './MmapReadableIndex.js';

/**
 * A read-only, memory-mapped index that watches the underlying file for changes and
 * re-maps it automatically.
 *
 * When the writer appends entries and flushes, the file's logical size increases.
 * The ReadOnlyAppendOnlyMmapedFile detects the size change via its file watcher,
 * remaps, and emits 'append' / 'truncate' with the previous and new file sizes.
 * MmapReadOnlyIndex translates those byte-level deltas into entry-count deltas and
 * re-emits them at the index level so callers can react without knowing about the
 * underlying file format.
 */
class MmapReadOnlyIndex extends MmapReadableIndex {

    createFile(name, options) {
        const file = new ReadOnlyAppendOnlyMmapedFile(name, options);

        file.on('append', (prevFileSize, newFileSize) => {
            const prevLength = Math.max(0, Math.floor((prevFileSize - this.headerSize) / this.EntryClass.size));
            const newLength = Math.max(0, Math.floor((newFileSize - this.headerSize) / this.EntryClass.size));
            if (newLength > prevLength) {
                this.emit('append', prevLength, newLength);
            }
        });

        file.on('truncate', (prevFileSize, newFileSize) => {
            const prevLength = Math.max(0, Math.floor((prevFileSize - this.headerSize) / this.EntryClass.size));
            const newLength = Math.max(0, Math.floor((newFileSize - this.headerSize) / this.EntryClass.size));
            if (newLength < prevLength) {
                this.emit('truncate', prevLength, newLength);
            }
        });

        return file;
    }
}

export default MmapReadOnlyIndex;
