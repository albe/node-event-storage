import path from 'path';
import { syncDirectory, syncDirectoryBlocking } from './utils/fsUtil.js';
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
            file.fileHandlePool.syncBlocking(file);
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
            file.fileHandlePool.syncBlocking(file);
            syncDirectoryBlocking(path.dirname(file.fileName));
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
     * @private
     */
    elapsed() {
        this.timeout = null;
        if (this.running) {
            this.due = true;
        } else {
            this.runInBackground();
        }
    }

    /**
     * Take everything waiting for a sync, leaving the scheduler empty.
     * @private
     * @returns {{files: Set<string>, directories: Set<string>, callbacks: Array<function>}}
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
     * Sync every waiting file and the directory of every new file in parallel in the background, then invoke the
     * waiting callbacks. A failed sync is thrown as an uncaught exception: the data may be lost, and no write after it
     * must be acknowledged.
     * @private
     */
    runInBackground() {
        const batch = this.take();
        const syncs = [];
        for (const file of batch.files) syncs.push(file.fileHandlePool.sync(file));
        for (const directory of batch.directories) syncs.push(syncDirectory(directory));
        this.running = batch;
        Promise.all(syncs).then(() => {
            if (this.running !== batch) return;
            this.running = null;
            invoke(batch.callbacks);
            if (this.due) {
                this.runInBackground();
            }
        }, error => {
            // A run that `run()` took over was synced again by it already.
            if (this.running !== batch) return;
            process.nextTick(() => { throw error; });
        });
    }

    /**
     * Sync everything waiting, and everything a background run is still syncing, right now, then invoke all their
     * callbacks in the order of the writes. Used when closing the storage, so that nothing remains unsynced.
     * Writes from within the callbacks belong to the next run.
     */
    run() {
        const running = this.running;
        const batch = this.take();
        this.running = null;
        if (running) {
            for (const file of running.files) file.fileHandlePool.syncBlocking(file);
            for (const directory of running.directories) syncDirectoryBlocking(directory);
        }
        for (const file of batch.files) file.fileHandlePool.syncBlocking(file);
        for (const directory of batch.directories) syncDirectoryBlocking(directory);
        if (running) invoke(running.callbacks);
        invoke(batch.callbacks);
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
