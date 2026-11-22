import path from 'path';
import { syncFile, syncDirectory } from './utils/fsUtil.js';

/**
 * Decides when flushed data reaches the disk, for all files of a storage:
 * - `syncOnFlush`: every flush is synced immediately.
 * - `syncInterval`: all files flushed within the interval are synced together by one run, once each, and only then
 *   are their write callbacks invoked (group commit). Commits to different partitions thus share one run.
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
        this.changedFiles = new Set();
        this.files = new Set();
        this.directories = new Set();
        this.callbacks = [];
    }

    /**
     * @param {{ flush: function(): boolean }} file A file with written data that a run needs to flush first.
     */
    changed(file) {
        if (this.interval > 0) {
            this.changedFiles.add(file);
            this.schedule();
        }
    }

    /**
     * @param {string} fileName The file that was flushed.
     * @param {Array<function>} [callbacks] The write callbacks to invoke once the flushed data is as durable as configured.
     */
    flushed(fileName, callbacks = []) {
        if (this.immediate) {
            syncFile(fileName);
        } else if (this.interval > 0) {
            this.files.add(fileName);
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
     * @param {string} fileName The file that was created.
     */
    created(fileName) {
        if (this.immediate) {
            syncFile(fileName);
            syncDirectory(path.dirname(fileName));
        } else if (this.interval > 0) {
            this.files.add(fileName);
            this.directories.add(path.dirname(fileName));
        }
    }

    /**
     * @private
     */
    schedule() {
        if (this.timeout === null) {
            this.timeout = setTimeout(() => this.run(), this.interval);
        }
    }

    /**
     * Flush all changed files, sync every flushed file and the directory of every new file once, then invoke the
     * waiting callbacks. Writes from within the callbacks belong to the next run.
     */
    run() {
        for (const file of this.changedFiles) file.flush();
        const { files, directories, callbacks } = this;
        this.changedFiles = new Set();
        this.files = new Set();
        this.directories = new Set();
        this.callbacks = [];
        if (this.timeout !== null) {
            clearTimeout(this.timeout);
            this.timeout = null;
        }

        for (const fileName of files) syncFile(fileName);
        for (const directory of directories) syncDirectory(directory);
        for (let i = 0; i < callbacks.length; i++) callbacks[i]();
    }
}

export default SyncScheduler;
