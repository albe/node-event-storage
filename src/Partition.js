import WritablePartition, { CorruptFileError } from './Partition/WritablePartition.js';
import ReadOnlyPartition from './Partition/ReadOnlyPartition.js';

export default WritablePartition;
export { ReadOnlyPartition as ReadOnly, CorruptFileError };
