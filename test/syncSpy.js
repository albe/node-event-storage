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

/**
 * Wait until the condition holds, e.g. until a sync in the background started or acknowledged its writes. Unlike a
 * fixed delay this does not depend on how fast the machine runs the timers and the thread pool.
 *
 * @param {function(): boolean} condition
 * @param {number} [timeout] Fail after this many ms, below the test timeout so that the reason is reported.
 * @returns {Promise<void>}
 */
export function waitFor(condition, timeout = 1500) {
    const start = Date.now();
    return new Promise((resolve, reject) => {
        (function check() {
            if (condition()) {
                resolve();
            } else if (Date.now() - start > timeout) {
                reject(new Error(`Timed out waiting for ${condition}`));
            } else {
                setTimeout(check, 1);
            }
        })();
    });
}
