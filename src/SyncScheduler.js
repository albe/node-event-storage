import path from 'path';
import { syncDirectory, parallel } from './utils/fsUtil.js';
/** @typedef {import('./FileHandlePool.js').default} FileHandlePool */

/**
 * Decides when flushed data reaches the disk, for all files of a storage:
 * - `syncOnFlush`: every flush is synced immediately.
 * - `syncInterval`: all files flushed within the interval are synced together by one run, once each, and only then
 *   are their write callbacks invoked (group commit). Commits to different partitions thus share one run. A run syncs
 *   its files in parallel in the background, so that it takes about as long as a single fsync and the process can go
 *   on writing meanwhile. Runs never overlap, so write callbacks are invoked in the order of the writes.
 * - neither: nothing is synced.
 * A new file is only guaranteed to survive a power loss once its directory is synced as well, which follows the same rule.
 */
class SyncScheduler {

    /**
     * @param {object} [options]
     * @param {boolean} [options.syncOnFlush] Sync every flush immediately. Defaults to false.
     * @param {number} [options.syncInterval] Sync all flushes at most this many ms later in one run. Ignored when `syncOnFlush` is set. Defaults to 0 (off).
     */
    constructor({ syncOnFlush = false, syncInterval = 0 } = {}) {
        this.immediate = !!syncOnFlush;
        this.interval = this.immediate ? 0 : syncInterval;
        this.timeout = null;
        this.files = new Set();
        this.directories = new Set();
        this.callbacks = [];
        this.running = null;
        this.due = false;
    }

    /**
     * @param {{fileName: string, fileHandlePool: FileHandlePool}} file The partition or index that was flushed. It is
     *   synced through its pool, which uses the pooled descriptor if the file is still open.
     * @param {Array<function>} [callbacks] The write callbacks to invoke once the flushed data is as durable as configured.
     */
    flushed(file, callbacks = []) {
        if (this.immediate) {
            file.fileHandlePool.sync(file);
        } else if (this.interval > 0) {
            this.files.add(file);
            for (let i = 0; i < callbacks.length; i++) this.callbacks.push(callbacks[i]);
            this.schedule();
            return;
        }
        for (let i = 0; i < callbacks.length; i++) callbacks[i]();
    }

    /**
     * Sync a new file with its header and its directory entry. Nothing is acknowledged before a write to the new
     * file, so with an interval this waits for the next run.
     *
     * @param {{fileName: string, fileHandlePool: FileHandlePool}} file The partition or index that was created.
     */
    created(file) {
        if (this.immediate) {
            file.fileHandlePool.sync(file);
            syncDirectory(path.dirname(file.fileName));
        } else if (this.interval > 0) {
            this.files.add(file);
            this.directories.add(path.dirname(file.fileName));
        }
    }

    /**
     * @private
     */
    schedule() {
        if (this.timeout === null && !this.due) {
            this.timeout = setTimeout(() => this.elapsed(), this.interval);
        }
    }

    /**
     * The interval of the oldest waiting flush has elapsed. If a run is still syncing, the next one starts right after it.
     * A failed sync is thrown as an uncaught exception: the data may be lost, and no write after it must be acknowledged.
     * @private
     */
    elapsed() {
        this.timeout = null;
        if (this.running) {
            this.due = true;
            return;
        }
        this.run(error => {
            if (error) {
                process.nextTick(() => { throw error; });
            } else if (this.due) {
                this.elapsed();
            }
        });
    }

    /**
     * Take everything waiting for a sync, leaving the scheduler empty.
     * @private
     * @returns {{files: Set<object>, directories: Set<string>, callbacks: Array<function>}}
     */
    take() {
        const { files, directories, callbacks } = this;
        this.files = new Set();
        this.directories = new Set();
        this.callbacks = [];
        if (this.timeout !== null) {
            clearTimeout(this.timeout);
            this.timeout = null;
        }
        this.due = false;
        return { files, directories, callbacks };
    }

    /**
     * Sync every waiting file and the directory of every new file, then invoke their write callbacks in the order of
     * the writes. Writes from within the callbacks belong to the next run.
     * Without a callback this blocks until everything is on disk, and takes over a run still syncing in the background,
     * as when closing the storage. With a callback it syncs everything in parallel in the background.
     *
     * @param {function(Error|null)} [callback] Called once the run is done, or with the error of a failed sync.
     */
    run(callback) {
        const background = typeof callback === 'function';
        const batches = !background && this.running ? [this.running, this.take()] : [this.take()];
        const batch = batches[batches.length - 1];
        this.running = background ? batch : null;
        const done = error => {
            // A run that a blocking run took over was synced and acknowledged by it already.
            if (background && this.running !== batch) return;
            this.running = null;
            if (!error) batches.forEach(({ callbacks }) => invoke(callbacks));
            callback?.(error);
        };
        const syncs = [];
        for (const { files, directories } of batches) {
            for (const file of files) syncs.push(next => file.fileHandlePool.sync(file, next));
            for (const directory of directories) syncs.push(next => syncDirectory(directory, next));
        }
        if (background) {
            parallel(syncs, done);
        } else {
            syncs.forEach(sync => sync());
            done(null);
        }
    }
}

/**
 * Invoke and remove all callbacks, so that a callback is never invoked twice when `run()` already completed a
 * background run.
 * @param {Array<function>} callbacks
 */
function invoke(callbacks) {
    const waiting = callbacks.splice(0);
    for (let i = 0; i < waiting.length; i++) waiting[i]();
}

export default SyncScheduler;
