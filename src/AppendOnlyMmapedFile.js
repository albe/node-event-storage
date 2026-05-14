import WritableAppendOnlyMmapedFile, { ReadableAppendOnlyMmapedFile, ReadOnlyAppendOnlyMmapedFile, FILE_SIZE_MARKER_SIZE } from './File/AppendOnlyMmapedFile.js';

WritableAppendOnlyMmapedFile.ReadOnly = ReadOnlyAppendOnlyMmapedFile;
WritableAppendOnlyMmapedFile.Readable = ReadableAppendOnlyMmapedFile;
WritableAppendOnlyMmapedFile.FILE_SIZE_MARKER_SIZE = FILE_SIZE_MARKER_SIZE;

export default WritableAppendOnlyMmapedFile;
export { ReadableAppendOnlyMmapedFile as Readable, ReadOnlyAppendOnlyMmapedFile as ReadOnly, FILE_SIZE_MARKER_SIZE };
