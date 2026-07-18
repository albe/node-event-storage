import FileHandlePool from './FileHandlePool.js';

/**
 * LRU file-handle pool for secondary index files.
 */
class IndexPool extends FileHandlePool {}

export default IndexPool;
