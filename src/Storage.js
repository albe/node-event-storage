import WritableStorage, { StorageLockedError, LOCK_THROW, LOCK_RECLAIM } from './Storage/WritableStorage.js';
import ReadOnlyStorage from './Storage/ReadOnlyStorage.js';

export default WritableStorage;
export { ReadOnlyStorage as ReadOnly, StorageLockedError, LOCK_THROW, LOCK_RECLAIM };
