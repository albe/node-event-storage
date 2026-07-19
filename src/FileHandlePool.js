import fs from 'fs';

/**
 * LRU pool for file descriptors shared across multiple file-backed objects.
 *
 * Targets are expected to expose `fileName`, `fileMode`, `fd` and may register
 * an `onBeforeClose` callback through `get()`.
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
     * @param {function(boolean): void} [onBeforeClose]
     * @returns {number}
     */
    get(target, onBeforeClose) {
        if (target.fd) {
            this.registerBeforeClose(target, onBeforeClose);
            this.touch(target);
            return target.fd;
        }

        this.evictLeastRecentlyUsedIfNeeded(target);
        const fd = fs.openSync(target.fileName, target.fileMode);
        target.fd = fd;
        this.handles.set(target, { fd, onBeforeClose });
        return fd;
    }

    /**
     * @param {object} target
     * @returns {boolean}
     */
    has(target) {
        return this.handles.has(target);
    }

    registerBeforeClose(target, onBeforeClose) {
        const handle = this.handles.get(target);
        if (!handle) {
            return;
        }
        handle.onBeforeClose = typeof onBeforeClose === 'function' ? onBeforeClose : null;
    }

    /**
     * @param {object} target
     * @param {boolean} [evicted=true]
     * @returns {boolean}
     */
    evict(target, evicted = true) {
        const handle = this.handles.get(target);
        const fd = handle?.fd ?? target.fd;
        this.handles.delete(target);
        if (!fd) {
            return false;
        }
        try {
            handle?.onBeforeClose?.(evicted);
        } finally {
            target.fd = null;
            fs.closeSync(fd);
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

    get openCount() {
        return this.handles.size;
    }

}

export default FileHandlePool;
