import fs from 'fs';

/**
 * LRU pool for file descriptors shared across multiple file-backed objects.
 *
 * Targets are expected to expose `fileName` and `fileMode` and may implement
 * `onBeforeClose(fd, evicted)` to flush state before their descriptor closes.
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
        this.handles.set(target, { fd });
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
            fs.closeSync(handle.fd);
        }
        return true;
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
