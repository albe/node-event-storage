import FileHandlePool from './FileHandlePool.js';

/**
 * LRU file-handle pool for secondary index files.
 */
class IndexPool extends FileHandlePool {

    /**
     * @param {string} id
     * @param {{ index: object|null, matcher?: object|function, closed?: boolean }} descriptor
     */
    add(id, descriptor) {
        super.add(id, descriptor);
    }

    /**
     * @param {string} id
     * @returns {{ index: object|null, matcher?: object|function, closed?: boolean }|undefined}
     */
    open(id) {
        const descriptor = this.registry[id];
        if (!descriptor || !descriptor.index) {
            return descriptor;
        }

        if (this.maxOpen > 0) {
            this.handles.delete(id);
            if (this.handles.size >= this.maxOpen) {
                for (const [lruId] of this.handles) {
                    this.handles.delete(lruId);
                    const lruDescriptor = this.registry[lruId];
                    if (lruDescriptor?.index?.isOpen()) {
                        lruDescriptor.index.close();
                        break;
                    }
                }
            }
            this.handles.set(id, true);
        }

        descriptor.index.open();
        return descriptor;
    }

    /**
     * @param {string} id
     */
    remove(id) {
        const descriptor = this.registry[id];
        if (descriptor?.index?.isOpen()) {
            descriptor.index.close();
        }
        delete this.registry[id];
        this.handles.delete(id);
    }
}

export default IndexPool;
