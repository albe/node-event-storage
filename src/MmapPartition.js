import MmapWritablePartition, { CorruptFileError } from './Partition/MmapWritablePartition.js';
import MmapReadOnlyPartition from './Partition/MmapReadOnlyPartition.js';

MmapWritablePartition.ReadOnly = MmapReadOnlyPartition;
MmapWritablePartition.CorruptFileError = CorruptFileError;

export default MmapWritablePartition;
export { MmapReadOnlyPartition as ReadOnly, CorruptFileError };
