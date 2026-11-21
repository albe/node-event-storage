import WritableStorage, { StorageLockedError, LOCK_THROW, LOCK_RECLAIM, LOCK_AUTO } from './Storage/WritableStorage.js';
import ReadOnlyStorage from './Storage/ReadOnlyStorage.js';

WritableStorage.ReadOnly = ReadOnlyStorage;
WritableStorage.StorageLockedError = StorageLockedError;
WritableStorage.LOCK_THROW = LOCK_THROW;
WritableStorage.LOCK_RECLAIM = LOCK_RECLAIM;
WritableStorage.LOCK_AUTO = LOCK_AUTO;

export default WritableStorage;
export { ReadOnlyStorage as ReadOnly, StorageLockedError, LOCK_THROW, LOCK_RECLAIM, LOCK_AUTO };
