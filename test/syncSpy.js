import fs from 'fs';

/**
 * Record the files that are synced: the sources only open files with `fs.promises` to sync them.
 * Call `restore()` on the returned array when done.
 *
 * @returns {Array<string> & { restore: function(): void }}
 */
export default function spyOnSyncs() {
    const synced = [];
    const originalOpen = fs.promises.open;
    // Recorded when the sync starts, so that the order is that of the calls, not of their completion.
    fs.promises.open = (fileName, flags) => {
        synced.push(fileName);
        return originalOpen(fileName, flags);
    };
    Object.defineProperty(synced, 'restore', { value: () => { fs.promises.open = originalOpen; } });
    return synced;
}
