import fs from 'fs';
import { syncFile, ignoreDeletedFile } from './utils/fsUtil.js';

/**
 * LRU pool for file descriptors shared across multiple file-backed objects.
 *
 * Targets are expected to expose `fileName` and `fileMode` and may implement
 * `onBeforeClose(fd, evicted)` to flush state before their descriptor closes.
 *
 * A descriptor that is being synced is pinned: evicting it removes it from the pool right away, but only closes it once
 * the sync is done. Otherwise the OS could reuse the number for another file meanwhile, which the sync would then hit.
 */
class FileHandlePool {

    /**
     * @param {number} [maxOpen=0] Maximum number of simultaneously open handles. 0 disables eviction.
     */
    constructor(maxOpen = 0) {
        this.maxOpen = maxOpen;
        this.handles = new Map();
    }

    /**
     * @param {object} target
     * @returns {number}
     */
    get(target) {
        const handle = this.handles.get(target);
        if (handle) {
            this.touch(target);
            return handle.fd;
        }

        this.evictLeastRecentlyUsedIfNeeded(target);
        const fd = fs.openSync(target.fileName, target.fileMode);
        this.handles.set(target, { fd, pins: 0, removed: false });
        return fd;
    }

    /**
     * @param {object} target
     * @returns {boolean}
     */
    has(target) {
        return this.handles.has(target);
    }

    /**
     * @param {object} target
     * @param {boolean} [evicted=true]
     * @returns {boolean}
     */
    evict(target, evicted = true) {
        const handle = this.handles.get(target);
        if (!handle) {
            return false;
        }
        try {
            target.onBeforeClose?.(handle.fd, evicted);
        } finally {
            this.handles.delete(target);
            handle.removed = true;
            if (handle.pins === 0) {
                fs.closeSync(handle.fd);
            }
        }
        return true;
    }

    /**
     * Sync the target's file to disk. An fsync covers all data written to the file through any descriptor, so the
     * pooled one is used if the file is open, which saves opening and closing one just for the sync. A file that is not
     * open in the pool is synced through its own descriptor, without displacing another one. A file deleted meanwhile
     * needs no sync.
     * Without a callback this blocks until the file is on disk. With a callback it syncs in the background and pins the
     * descriptor until then.
     *
     * @param {object} target
     * @param {function(Error|null)} [callback]
     */
    sync(target, callback) {
        const handle = this.handles.get(target);
        if (typeof callback !== 'function') {
            try {
                if (handle) fs.fsyncSync(handle.fd);
                else syncFile(target.fileName);
            } catch (error) {
                ignoreDeletedFile(error);
            }
            return;
        }
        if (!handle) {
            syncFile(target.fileName, error => callback(error && error.code !== 'ENOENT' ? error : null));
            return;
        }
        handle.pins++;
        fs.fsync(handle.fd, error => {
            handle.pins--;
            if (handle.removed && handle.pins === 0) {
                fs.closeSync(handle.fd);
            }
            callback(error || null);
        });
    }

    /**
     * @param {object} target
     * @returns {void}
     */
    touch(target) {
        const handle = this.handles.get(target);
        if (!handle) {
            return;
        }
        this.handles.delete(target);
        this.handles.set(target, handle);
    }

    /**
     * Evict the least-recently-used descriptor before opening another one.
     *
     * @param {object} target
     */
    evictLeastRecentlyUsedIfNeeded(target) {
        if (this.maxOpen <= 0) {
            return;
        }
        this.handles.delete(target);
        if (this.handles.size < this.maxOpen) {
            return;
        }
        const lruTarget = this.handles.keys().next().value;
        if (!lruTarget) {
            return;
        }
        this.evict(lruTarget);
    }

    /**
     * @returns {number}
     */
    get openCount() {
        return this.handles.size;
    }

}

export default FileHandlePool;
