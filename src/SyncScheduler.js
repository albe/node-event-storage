import path from 'path';
import { syncDirectory, parallel } from './utils/fsUtil.js';
/** @typedef {import('./FileHandlePool.js').default} FileHandlePool */

/**
 * Decides when flushed data reaches the disk, for all files of a storage:
 * - `syncInterval`: all files flushed within the interval are synced together by one run, once each, and only then
 *   are their write callbacks invoked (group commit). Commits to different partitions thus share one run.
 * - `syncOnFlush`: like an interval of 0, a run starts as soon as the current tick is done.
 * - neither: nothing is synced, write callbacks are invoked right away.
 * A run syncs its files in parallel in the background, so the process goes on writing meanwhile. Runs follow each
 * other in a loop and never overlap, so write callbacks are invoked in the order of the writes. Writes during a run
 * belong to the next one.
 * A new file is only guaranteed to survive a power loss once its directory is synced as well, which follows the same rule.
 */
class SyncScheduler {

    /**
     * @param {object} [options]
     * @param {boolean} [options.syncOnFlush] Sync every flush as soon as possible. Defaults to false.
     * @param {number} [options.syncInterval] Sync all flushes at most this many ms later in one run. Ignored when `syncOnFlush` is set. Defaults to 0 (off).
     */
    constructor({ syncOnFlush = false, syncInterval = 0 } = {}) {
        this.enabled = !!syncOnFlush || syncInterval > 0;
        this.interval = syncOnFlush ? 0 : syncInterval;
        this.files = new Set();
        this.directories = new Set();
        this.callbacks = [];
        this.running = false;
        this.timer = null;
        this.drained = [];
    }

    /**
     * @param {{fileName: string, fileHandlePool: FileHandlePool}} file The partition or index that was flushed. It is
     *   synced through its pool, which uses the pooled descriptor if the file is still open.
     * @param {Array<function>} [callbacks] The write callbacks to invoke once the flushed data is as durable as configured.
     */
    flushed(file, callbacks = []) {
        if (!this.enabled) {
            invoke(callbacks);
            return;
        }
        this.files.add(file);
        for (let i = 0; i < callbacks.length; i++) this.callbacks.push(callbacks[i]);
        this.start();
    }

    /**
     * Sync a new file with its header and its directory entry.
     *
     * @param {{fileName: string, fileHandlePool: FileHandlePool}} file The partition or index that was created.
     */
    created(file) {
        if (!this.enabled) {
            return;
        }
        this.files.add(file);
        this.directories.add(path.dirname(file.fileName));
        this.start();
    }

    /**
     * Start the next run right away instead of waiting for the interval, e.g. when closing the storage.
     *
     * @param {function(Error|null)} callback Called once everything flushed so far is synced and its write callbacks
     *   were invoked, or with the error of a failed sync.
     */
    drain(callback) {
        if (!this.running) {
            callback(null);
            return;
        }
        this.drained.push(callback);
        if (this.timer !== null) {
            clearTimeout(this.timer);
            clearImmediate(this.timer);
            this.run();
        }
    }

    /**
     * @private
     */
    start() {
        if (!this.running) {
            this.running = true;
            this.wait();
        }
    }

    /**
     * Run after the interval, after the current tick for an interval of 0, or right away while draining.
     * @private
     */
    wait() {
        if (this.drained.length > 0) {
            this.run();
        } else {
            this.timer = this.interval > 0 ? setTimeout(() => this.run(), this.interval) : setImmediate(() => this.run());
        }
    }

    /**
     * Sync everything flushed since the previous run in parallel and invoke its callbacks, then wait for the next run
     * while something is waiting. A failed sync ends the loop: it is passed to those draining, or thrown as an uncaught
     * exception, because acknowledged writes may be lost.
     * @private
     */
    run() {
        this.timer = null;
        const { files, directories, callbacks } = this;
        this.files = new Set();
        this.directories = new Set();
        this.callbacks = [];
        const syncs = [];
        for (const file of files) syncs.push(next => file.fileHandlePool.sync(file, next));
        for (const directory of directories) syncs.push(next => syncDirectory(directory, next));
        parallel(syncs, error => {
            if (!error) {
                invoke(callbacks);
                if (this.files.size > 0 || this.callbacks.length > 0) {
                    this.wait();
                    return;
                }
            }
            this.running = false;
            const drained = this.drained.splice(0);
            if (error && drained.length === 0) {
                process.nextTick(() => { throw error; });
            }
            for (const callback of drained) callback(error);
        });
    }
}

/**
 * @param {Array<function>} callbacks
 */
function invoke(callbacks) {
    for (let i = 0; i < callbacks.length; i++) callbacks[i]();
}

export default SyncScheduler;
