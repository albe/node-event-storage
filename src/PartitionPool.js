/**
 * A fixed-capacity registry of partitions with LRU eviction of open file handles.
 *
 * All partitions are stored by their numeric id and may be queried at any time.
 * The pool additionally tracks which partitions currently have an open file descriptor
 * in LRU (least-recently-used) order.  When the pool is asked to open a partition and
 * doing so would exceed the configured cap, the least-recently-used open partition is
 * closed first to stay within the limit.
 *
 * Setting the cap to 0 disables eviction: all partitions are allowed to remain open
 * simultaneously, which matches the uncapped behaviour of the original implementation.
 */
class PartitionPool {

    /**
     * @param {number} [maxOpen=0] Maximum number of simultaneously open partition file
     *   handles.  0 disables the limit (no eviction).
     */
    constructor(maxOpen = 0) {
        this.maxOpen = maxOpen;
        /** Registry of all known partitions keyed by id. */
        this.registry = Object.create(null);
        /**
         * Insertion-order map used for LRU tracking of open file handles.
         * Key = partition id, value = true.
         * Oldest (least-recently-used) entry is first; newest (most-recently-used) is last.
         */
        this.handles = new Map();
    }

    /**
     * Register a partition under the given id.
     *
     * @param {number|string} id
     * @param {object} partition
     */
    add(id, partition) {
        this.registry[id] = partition;
    }

    /**
     * Retrieve a registered partition without opening it.
     *
     * @param {number|string} id
     * @returns {object|undefined}
     */
    get(id) {
        return this.registry[id];
    }

    /**
     * Check whether a partition with the given id is registered in the pool.
     *
     * @param {number|string} id
     * @returns {boolean}
     */
    has(id) {
        return id in this.registry;
    }

    /**
     * Open the partition with the given id, applying LRU eviction if necessary.
     *
     * If the partition is not yet open and adding it would exceed `maxOpen`, the
     * least-recently-used open partition is closed first.  Stale entries (partitions
     * that were closed externally) are discarded from the LRU map as they are
     * encountered; if all tracked entries turn out to be stale the loop exits without
     * closing any partition — the handle count stays temporarily inflated (bounded by
     * the number of external closes since the last `open()` call) but correctness is
     * preserved.
     *
     * @param {number|string} id
     * @returns {object} The opened partition.
     */
    open(id) {
        const partition = this.registry[id];

        if (this.maxOpen > 0) {
            // Remove id first — this may already bring the handle count below the cap.
            this.handles.delete(id);
            if (this.handles.size >= this.maxOpen) {
                for (const [lruId] of this.handles) {
                    this.handles.delete(lruId);
                    const lruPartition = this.registry[lruId];
                    if (lruPartition && lruPartition.isOpen()) {
                        lruPartition.close();
                        break;
                    }
                }
            }
            // (Re-)add id at the MRU end of the map.
            this.handles.set(id, true);
        }

        partition.open();
        return partition;
    }

    /**
     * Invoke `callback` for every registered partition.
     *
     * @param {function(object): void} callback
     */
    forEach(callback) {
        for (const id of Object.keys(this.registry)) {
            callback(this.registry[id]);
        }
    }

    /**
     * Yield every registered partition object.
     *
     * @returns {Generator<object>}
     */
    *values() {
        for (const id of Object.keys(this.registry)) {
            yield this.registry[id];
        }
    }

    /**
     * The total number of registered partitions.
     * @returns {number}
     */
    get count() {
        return Object.keys(this.registry).length;
    }

    /**
     * The number of open partition file handles currently tracked by the pool.
     * @returns {number}
     */
    get openCount() {
        return this.handles.size;
    }

    /**
     * Reset the open-handle tracking without closing any partitions.
     *
     * Call this after externally closing all partitions (e.g. after
     * `checkTornWrites`) to keep the pool's LRU state consistent with reality.
     */
    clearOpenHandles() {
        this.handles.clear();
    }

}

export default PartitionPool;
