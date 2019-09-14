const fs = require('fs');
const ReadableStorage = require('./ReadableStorage');

/**
 * An append-only storage with highly performant positional range scans.
 * It's highly optimized for an event-store and hence does not support compaction or data-rewrite, nor any querying
 */
class ReadOnlyStorage extends ReadableStorage {

    /**
     * Open the storage and indexes and create read and write buffers eagerly.
     * Will emit an 'opened' event if finished.
     *
     * @api
     * @returns {boolean}
     */
    open() {
        this.index.open();

        this.forEachSecondaryIndex(index => index.open());
        // TODO: Add directory watcher for new indexes and partitions and emit 'index-created'(name)
        //this.watcher = fs.watch();

        this.emit('opened');
        return true;
    }

    /**
     * Close the storage and frees up all resources.
     * Will emit a 'closed' event when finished.
     *
     * @api
     * @returns void
     */
    close() {
        if (this.watcher) {
            this.watcher.close();
        }
        super.close();
    }

    /**
     * @protected
     * @param {string} name
     * @param {object} [options]
     * @returns {ReadableIndex}
     */
    createIndex(name, options = {}) {
        const index = super.createIndex(name, options);
        index.on('append', (prevLength, newLength) => {
            const entries = index.range(prevLength + 1, newLength);
            if (entries === false) {
                return;
            }
            for (let entry of entries) {
                const document = this.readFrom(entry.partition, entry.position, entry.size);
                this.emit('index-add', name, entry.number, document);
            }
        });
        return index;
    }

    /**
     * @protected
     * @param args
     * @returns {Partition}
     */
    createPartition(...args) {
        const partition = super.createPartition(...args);
        partition.on('append', (prevSize, newSize) => {
            this.emit('append', partition.id, prevSize, newSize);
        });
        partition.on('truncate', (prevSize, newSize) => {
            this.emit('truncate', partition.id, prevSize, newSize);
        });
        return partition;
    }
}

module.exports = ReadOnlyStorage;
