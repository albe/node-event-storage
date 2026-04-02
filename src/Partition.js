import WritablePartition, { CorruptFileError } from './Partition/WritablePartition.js';
import ReadOnlyPartition from './Partition/ReadOnlyPartition.js';

WritablePartition.ReadOnly = ReadOnlyPartition;
WritablePartition.CorruptFileError = CorruptFileError;

export default WritablePartition;
export { ReadOnlyPartition as ReadOnly, CorruptFileError };
