import fs from 'fs';
import FileHandlePool from '../src/FileHandlePool.js';

/**
 * Record the files that are synced: through the descriptor of their pool if open there, otherwise by opening them
 * with `fs.promises`. Call `restore()` on the returned array when done.
 *
 * @returns {Array<string> & { restore: function(): void }}
 */
export default function spyOnSyncs() {
    const synced = [];
    const originalOpen = fs.promises.open;
    const originalSync = FileHandlePool.prototype.sync;
    // Recorded when the sync starts, so that the order is that of the calls, not of their completion.
    fs.promises.open = (fileName, flags) => {
        synced.push(fileName);
        return originalOpen(fileName, flags);
    };
    FileHandlePool.prototype.sync = function(target) {
        if (this.has(target)) synced.push(target.fileName);
        return originalSync.call(this, target);
    };
    Object.defineProperty(synced, 'restore', { value: () => {
        fs.promises.open = originalOpen;
        FileHandlePool.prototype.sync = originalSync;
    } });
    return synced;
}
