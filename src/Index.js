import WritableIndex, { Entry } from './Index/WritableIndex.js';
import ReadOnlyIndex from './Index/ReadOnlyIndex.js';

WritableIndex.ReadOnly = ReadOnlyIndex;
WritableIndex.Entry = Entry;

export default WritableIndex;
export { ReadOnlyIndex as ReadOnly, Entry };
