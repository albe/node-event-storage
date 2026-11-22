import path from 'path';
import { syncDirectory } from './utils/fsUtil.js';

/**
 * Group commit over many files: all files written within one interval are made durable by a single run, which
 * fsyncs each changed file and each directory that received a new file once, and only then acknowledges the writes.
 * One cycle is shared by all partitions of a storage, so commits to different partitions share the same run.
 *
 * Files are expected to implement `flush()` and `fsync()`, the latter only syncing when the file changed.
 */
class SyncCycle {

    /**
     * @param {number} interval The maximum time in ms between a write and the run that makes it durable.
     */
    constructor(interval) {
        this.interval = interval;
        this.timeout = null;
        this.files = new Set();
        this.directories = new Set();
        this.callbacks = [];
    }

    /**
     * @param {{ flush: function(): boolean, fsync: function(): void }} file A file that has writes which are not synced yet.
     */
    add(file) {
        this.files.add(file);
        this.schedule();
    }

    /**
     * A new file is only guaranteed to survive a power loss once its directory entry is synced as well.
     * Creating a file does not schedule a run, because nothing is acknowledged before a write to it.
     *
     * @param {string} fileName
     */
    addNewFile(fileName) {
        this.directories.add(path.dirname(fileName));
    }

    /**
     * @param {Array<function>} callbacks Callbacks to invoke once everything written so far is durable.
     */
    whenSynced(callbacks) {
        for (let i = 0; i < callbacks.length; i++) this.callbacks.push(callbacks[i]);
        this.schedule();
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
     * Flush and fsync all changed files, sync the directories of all new files, then invoke the waiting callbacks.
     * Writes from within the callbacks belong to the next run.
     */
    run() {
        for (const file of this.files) file.flush();
        const { files, directories, callbacks } = this;
        this.files = new Set();
        this.directories = new Set();
        this.callbacks = [];
        if (this.timeout !== null) {
            clearTimeout(this.timeout);
            this.timeout = null;
        }

        for (const file of files) file.fsync();
        for (const directory of directories) syncDirectory(directory);
        for (let i = 0; i < callbacks.length; i++) callbacks[i]();
    }
}

export default SyncCycle;
