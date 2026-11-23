import path from 'path';
import { syncFile, syncDirectory, ignoreDeletedFile } from './utils/fsUtil.js';

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
        this.loop = null;
        this.wakeUp = null;
        this.draining = false;
    }

    /**
     * @param {string} fileName The file that was flushed.
     * @param {Array<function>} [callbacks] The write callbacks to invoke once the flushed data is as durable as configured.
     */
    flushed(fileName, callbacks = []) {
        if (!this.enabled) {
            invoke(callbacks);
            return;
        }
        this.files.add(fileName);
        for (let i = 0; i < callbacks.length; i++) this.callbacks.push(callbacks[i]);
        this.start();
    }

    /**
     * Sync a new file with its header and its directory entry.
     *
     * @param {string} fileName The file that was created.
     */
    created(fileName) {
        if (!this.enabled) {
            return;
        }
        this.files.add(fileName);
        this.directories.add(path.dirname(fileName));
        this.start();
    }

    /**
     * Start the next run right away instead of waiting for the interval, e.g. when closing the storage.
     *
     * @returns {Promise<void>} Resolves once everything flushed so far is synced and its callbacks were invoked.
     *   Rejects if a sync failed.
     */
    drain() {
        if (this.loop === null) {
            return Promise.resolve();
        }
        this.draining = true;
        this.wakeUp?.();
        return this.loop;
    }

    /**
     * @private
     */
    start() {
        if (this.loop === null) {
            this.loop = this.run();
            // A failed sync is reported through `drain()`; without a caller waiting for it, it crashes the process
            // as an unhandled rejection, because acknowledged writes may be lost.
        }
    }

    /**
     * @private
     * @returns {Promise<void>} Resolves after the interval, after the current tick for an interval of 0, or right
     *   away while draining.
     */
    wait() {
        if (this.draining) {
            return Promise.resolve();
        }
        return new Promise(resolve => {
            const timer = this.interval > 0 ? setTimeout(resolve, this.interval) : setImmediate(resolve);
            this.wakeUp = () => {
                clearTimeout(timer);
                clearImmediate(timer);
                resolve();
            };
        }).finally(() => {
            this.wakeUp = null;
        });
    }

    /**
     * Wait for the interval, then sync everything flushed meanwhile in parallel and invoke its callbacks. Repeat until
     * nothing is waiting anymore.
     * @private
     * @returns {Promise<void>}
     */
    async run() {
        try {
            while (this.files.size > 0 || this.callbacks.length > 0) {
                await this.wait();
                const { files, directories, callbacks } = this;
                this.files = new Set();
                this.directories = new Set();
                this.callbacks = [];
                const syncs = [];
                for (const fileName of files) syncs.push(syncFile(fileName).catch(ignoreDeletedFile));
                for (const directory of directories) syncs.push(syncDirectory(directory));
                await Promise.all(syncs);
                invoke(callbacks);
            }
        } finally {
            this.loop = null;
            this.draining = false;
        }
    }
}

/**
 * @param {Array<function>} callbacks
 */
function invoke(callbacks) {
    for (let i = 0; i < callbacks.length; i++) callbacks[i]();
}

export default SyncScheduler;
