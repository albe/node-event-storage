import fs from 'fs';
import FileHandlePool from '../src/FileHandlePool.js';

/**
 * Record the files that are synced in the background: through the descriptor of their pool if open there, otherwise
 * by opening them with `fs.open`. Call `restore()` on the returned array when done.
 *
 * @returns {Array<string> & { restore: function(): void }}
 */
export default function spyOnSyncs() {
    const synced = [];
    const originalOpen = fs.open;
    const originalSync = FileHandlePool.prototype.sync;
    // Recorded when the sync starts, so that the order is that of the calls, not of their completion.
    fs.open = (fileName, flags, callback) => {
        synced.push(fileName);
        return originalOpen(fileName, flags, callback);
    };
    FileHandlePool.prototype.sync = function(target, callback) {
        if (callback && this.has(target)) synced.push(target.fileName);
        return originalSync.call(this, target, callback);
    };
    Object.defineProperty(synced, 'restore', { value: () => {
        fs.open = originalOpen;
        FileHandlePool.prototype.sync = originalSync;
    } });
    return synced;
}
