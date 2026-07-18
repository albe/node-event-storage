/**
 * A fixed-capacity registry of resources with LRU eviction of open file handles.
 *
 * Resources are keyed by an identifier and are expected to implement
 * `open()`, `close()`, and `isOpen()`.
 *
 * Setting `maxOpen` to 0 disables eviction (unbounded open handles).
 */
class FileHandlePool {

    /**
     * @param {number} [maxOpen=0] Maximum number of simultaneously open file
     *   handles. 0 disables the limit.
     */
    constructor(maxOpen = 0) {
        this.maxOpen = maxOpen;
        this.registry = Object.create(null);
        this.handles = new Map();
    }

    /**
     * @param {number|string} id
     * @param {object} resource
     */
    add(id, resource) {
        this.registry[id] = resource;
    }

    /**
     * @param {number|string} id
     * @returns {object|undefined}
     */
    get(id) {
        return this.registry[id];
    }

    /**
     * @param {number|string} id
     * @returns {boolean}
     */
    has(id) {
        return id in this.registry;
    }

    /**
     * Remove a resource from the pool and close it if open.
     *
     * @param {number|string} id
     */
    remove(id) {
        const resource = this.registry[id];
        if (resource && resource.isOpen()) {
            resource.close();
        }
        delete this.registry[id];
        this.handles.delete(id);
    }

    /**
     * Drop only the open-handle tracking entry for a resource.
     *
     * @param {number|string} id
     */
    forgetOpenHandle(id) {
        this.handles.delete(id);
    }

    /**
     * Open the resource with LRU eviction if needed.
     *
     * @param {number|string} id
     * @returns {object}
     */
    open(id) {
        const resource = this.registry[id];

        if (this.maxOpen > 0) {
            this.handles.delete(id);
            if (this.handles.size >= this.maxOpen) {
                for (const [lruId] of this.handles) {
                    this.handles.delete(lruId);
                    const lruResource = this.registry[lruId];
                    if (lruResource && lruResource.isOpen()) {
                        lruResource.close();
                        break;
                    }
                }
            }
            this.handles.set(id, true);
        }

        resource.open();
        return resource;
    }

    /**
     * @param {function(object): void} callback
     */
    forEach(callback) {
        for (const id of Object.keys(this.registry)) {
            callback(this.registry[id]);
        }
    }

    /**
     * @returns {Generator<object>}
     */
    *values() {
        for (const id of Object.keys(this.registry)) {
            yield this.registry[id];
        }
    }

    /**
     * @returns {number}
     */
    get count() {
        return Object.keys(this.registry).length;
    }

    /**
     * @returns {number}
     */
    get openCount() {
        return this.handles.size;
    }

    /**
     * Reset open-handle tracking without closing resources.
     */
    clearOpenHandles() {
        this.handles.clear();
    }
}

export default FileHandlePool;
