import fs from 'fs';
import path from 'path';
import { ensureDirectory } from './utils/fsUtil.js';

/**
 * Move the type streams of stores written before type streams were namespaced (named like the type, e.g.
 * `CourseCreated`) to `types/<type>`, and emit `stream-migrated` with the old and the new name for each.
 *
 * A stream counts as a type stream when its stored matcher is exactly the `typeAccessor` matcher for its name, so a
 * regular stream that only shares the name of a type is left alone. An index file holds the entries and the matcher,
 * neither of which depends on the stream name, so the file is renamed instead of copied.
 *
 * @param {import('./EventStore.js').default} store A writable store whose streams have been registered.
 */
function migrateTypeStreams(store) {
    if (store.typeSource === null) {
        return;
    }
    const typeMatcher = store.typeSource.matcherFn(undefined);
    for (const streamName of Object.keys(store.streams)) {
        if (streamName === '_all' || streamName.includes('/') || store.streams[streamName].closed) {
            continue;
        }
        const matcher = store.storage.secondaryIndexes['stream-' + streamName]?.matcher;
        if (!matcher || typeof matcher !== 'object' || JSON.stringify(matcher) !== JSON.stringify(typeMatcher(streamName))) {
            continue;
        }
        const target = store.typeSource.nameBuilder(streamName);
        if (target in store.streams) {
            // Already indexed under the new name, e.g. by a version that wrote both: the old index is redundant.
            store.deleteEventStream(streamName);
        } else {
            renameStream(store, streamName, target, matcher);
        }
        store.emit('stream-migrated', streamName, target);
    }
}

/**
 * Close the index of a stream, rename its file to the index file of the new name and open it under that name.
 *
 * @param {import('./EventStore.js').default} store
 * @param {string} from
 * @param {string} to
 * @param {object} matcher The matcher stored in the index.
 */
function renameStream(store, from, to, matcher) {
    const { storage } = store;
    const { index } = store.streams[from];
    const fileName = index.fileName;
    storage.removeSecondaryIndex('stream-' + from);
    index.close();
    delete store.streams[from];

    const targetFileName = path.join(storage.indexDirectory, `${storage.storageFile}.stream-${to}.index`);
    ensureDirectory(path.dirname(targetFileName));
    fs.renameSync(fileName, targetFileName);
    store.streams[to] = { index: storage.openIndex('stream-' + to, matcher) };
}

export { migrateTypeStreams };
