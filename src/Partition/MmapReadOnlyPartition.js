import { ReadOnlyAppendOnlyMmapedFile } from '../File/AppendOnlyMmapedFile.js';
import MmapReadablePartition from './MmapReadablePartition.js';

class MmapReadOnlyPartition extends MmapReadablePartition {

    createFile(name, config) {
        const file = new ReadOnlyAppendOnlyMmapedFile(name, config);

        file.on('append', (prevFileSize, newFileSize) => {
            const prevSize = Math.max(0, prevFileSize - this.headerSize);
            const nextSize = Math.max(0, newFileSize - this.headerSize);
            if (nextSize > prevSize) {
                this.size = nextSize;
                this.emit('append', prevSize, nextSize);
            }
        });

        file.on('truncate', (prevFileSize, newFileSize) => {
            const prevSize = Math.max(0, prevFileSize - this.headerSize);
            const nextSize = Math.max(0, newFileSize - this.headerSize);
            if (nextSize < prevSize) {
                this.size = nextSize;
                this.emit('truncate', prevSize, nextSize);
            }
        });

        file.on('rename', () => this.close());

        return file;
    }
}

export default MmapReadOnlyPartition;
