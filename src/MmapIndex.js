import MmapWritableIndex, { Entry } from './Index/MmapWritableIndex.js';
import MmapReadOnlyIndex from './Index/MmapReadOnlyIndex.js';

MmapWritableIndex.ReadOnly = MmapReadOnlyIndex;
MmapWritableIndex.Entry = Entry;

export default MmapWritableIndex;
export { MmapReadOnlyIndex as ReadOnly, Entry };
