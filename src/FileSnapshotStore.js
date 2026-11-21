import fs from 'fs';
import path from 'path';
import { assert } from './utils/util.js';
import { ensureDirectory, isSameOrParentDirectory, writeFileAtomic } from './utils/fsUtil.js';

/**
 * A snapshot store for read models that keeps one JSON file per read model name in a directory.
 *
 * @param {string} directory
 * @returns {import('./ReadModel.js').SnapshotStore}
 */
function fileSnapshotStore(directory) {
    assert(typeof directory === 'string' && directory !== '', 'fileSnapshotStore() needs a directory.');
    const fileName = (name) => {
        const file = path.join(directory, `${name}.json`);
        assert(isSameOrParentDirectory(directory, path.dirname(file)), `Read model name "${name}" leaves the snapshot directory.`);
        return file;
    };
    return Object.freeze({
        load(name) {
            const file = fileName(name);
            return fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : null;
        },
        save(name, snapshot) {
            const file = fileName(name);
            ensureDirectory(path.dirname(file));
            writeFileAtomic(file, JSON.stringify(snapshot));
        }
    });
}

export { fileSnapshotStore };
