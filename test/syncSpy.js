import fs from 'fs';
import FileHandlePool from '../src/FileHandlePool.js';

/**
 * Record the files that are synced: through the descriptor of their pool if open there, otherwise through a descriptor
 * opened with `fs.open` just for the sync. Call `restore()` on the returned array when done.
 *
 * @returns {Array<string> & { restore: function(): void }}
 */
export default function spyOnSyncs() {
    const synced = [];
    const originalOpen = fs.open;
    const originalFsync = fs.fsync;
    const originalSync = FileHandlePool.prototype.sync;
    const opened = new Map();
    // Recorded when the fsync starts, so that the order is that of the calls, not of their completion.
    fs.open = (fileName, ...args) => {
        const callback = args.pop();
        return originalOpen(fileName, ...args, (error, fd) => {
            if (!error) opened.set(fd, fileName);
            callback(error, fd);
        });
    };
    fs.fsync = (fd, callback) => {
        if (opened.has(fd)) synced.push(opened.get(fd));
        return originalFsync(fd, callback);
    };
    FileHandlePool.prototype.sync = function(target, callback) {
        if (this.has(target)) synced.push(target.fileName);
        return originalSync.call(this, target, callback);
    };
    Object.defineProperty(synced, 'restore', { value: () => {
        fs.open = originalOpen;
        fs.fsync = originalFsync;
        FileHandlePool.prototype.sync = originalSync;
    } });
    return synced;
}

/**
 * @param {SyncScheduler} scheduler
 * @returns {Promise<void>} Resolves once the scheduler synced everything flushed so far.
 */
export function drained(scheduler) {
    return new Promise((resolve, reject) => scheduler.drain(error => error ? reject(error) : resolve()));
}

/**
 * @param {WritableStorage} storage
 * @returns {Promise<void>} Resolves once the checkpoints queued so far are done.
 */
export function checkpointed(storage) {
    return new Promise((resolve, reject) => storage.queueCheckpoint(done => done(null), error => error ? reject(error) : resolve()));
}

/**
 * @param {WritableStorage} storage
 * @returns {Promise<void>} Resolves once a new checkpoint is recorded.
 */
export function checkpoint(storage) {
    return new Promise((resolve, reject) => storage.checkpoint(error => error ? reject(error) : resolve()));
}
