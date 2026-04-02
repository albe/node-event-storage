import WritableStorage, { StorageLockedError, LOCK_THROW, LOCK_RECLAIM } from './Storage/WritableStorage.js';
import ReadOnlyStorage from './Storage/ReadOnlyStorage.js';

WritableStorage.ReadOnly = ReadOnlyStorage;
WritableStorage.StorageLockedError = StorageLockedError;
WritableStorage.LOCK_THROW = LOCK_THROW;
WritableStorage.LOCK_RECLAIM = LOCK_RECLAIM;

export default WritableStorage;
export { ReadOnlyStorage as ReadOnly, StorageLockedError, LOCK_THROW, LOCK_RECLAIM };
