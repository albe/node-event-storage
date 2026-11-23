import fs from 'fs';
import os from "os";
import path from 'path';
import { mkdirpSync } from 'mkdirp';

/**
 * Resolve a file or directory path, supporting `~/` for the user's home directory and joining with additional path segments.
 * @param {string} fileOrDirectory
 * @param {string} [directories]
 * @returns {string} The normalized path, with resolved relative and home directory references.
 */
function resolvePath(fileOrDirectory, ...directories) {
    fileOrDirectory = fileOrDirectory.replace(/^~\//, os.homedir() + '/');
    return path.resolve(fileOrDirectory, ...directories);
}

/**
 * Unlink a file, ignoring the error if it does not exist.
 * @param {string} fileName
 * @throws {Error} For any error other than ENOENT.
 */
function safeUnlink(fileName) {
    try {
        fs.unlinkSync(fileName);
    } catch (e) {
        if (e.code !== 'ENOENT') {
            throw e;
        }
    }
}

/**
 * Replace a file atomically: write to a temporary file, then rename it over the target.
 * A failed or interrupted write never leaves a partially written target file behind.
 *
 * @param {string} fileName Target file.
 * @param {string|Buffer} data
 * @param {string} [tmpFileName] Temporary file to write first. Defaults to `fileName + '.tmp'`.
 * @throws {Error} When writing or renaming fails; the temporary file is removed in that case.
 */
function writeFileAtomic(fileName, data, tmpFileName = fileName + '.tmp') {
    try {
        fs.writeFileSync(tmpFileName, data);
        fs.renameSync(tmpFileName, fileName);
    } catch (e) {
        safeUnlink(tmpFileName);
        throw e;
    }
}

/**
 * Sync a file to disk through its own descriptor, independent of whether and where the file is open otherwise.
 *
 * @param {string} fileName
 * @param {string} [flags] The flags to open the file with. Windows needs write access to sync a file.
 */
function syncFile(fileName, flags = 'r+') {
    const fd = fs.openSync(fileName, flags);
    try {
        fs.fsyncSync(fd);
    } finally {
        fs.closeSync(fd);
    }
}

/**
 * A file deleted or renamed after it was written needs no sync anymore. Rethrows every other error.
 *
 * @param {Error} error
 */
function ignoreDeletedFile(error) {
    if (error.code !== 'ENOENT') {
        throw error;
    }
}

/**
 * Like `syncFile()`, but in the background on the libuv thread pool, so that the event loop goes on in the meantime
 * and several files can be synced in parallel.
 *
 * @param {string} fileName
 * @param {string} [flags] The flags to open the file with. Windows needs write access to sync a file.
 * @returns {Promise<void>}
 */
async function syncFileInBackground(fileName, flags = 'r+') {
    const handle = await fs.promises.open(fileName, flags);
    try {
        await handle.sync();
    } finally {
        await handle.close();
    }
}

/**
 * Platforms that can not open a directory for syncing (Windows) are skipped silently.
 *
 * @param {string} dirName
 */
function syncDirectory(dirName) {
    try {
        syncFile(dirName, 'r');
    } catch (e) {
    }
}

/**
 * Like `syncDirectory()`, but in the background.
 *
 * @param {string} dirName
 * @returns {Promise<void>}
 */
async function syncDirectoryInBackground(dirName) {
    try {
        await syncFileInBackground(dirName, 'r');
    } catch (e) {
    }
}

/**
 * Ensure that the given directory exists.
 * @param {string} dirName Target directory.
 * @returns {boolean} True when the directory already existed.
 */
function ensureDirectory(dirName) {
    if (!fs.existsSync(dirName)) {
        try {
            mkdirpSync(dirName);
        } catch (e) {
        }
        return false;
    }
    return true;
}

/**
 * Invoke `onEach` if `relativePath` matches `regexPattern`, passing the first capture group or the full match.
 *
 * @param {string} relativePath Relative file path.
 * @param {RegExp} regexPattern Regex used for path matching.
 * @param {function(string): void} onEach Callback invoked per match.
 * @returns {void}
 */
function visitMatchingPath(relativePath, regexPattern, onEach) {
    const match = relativePath.match(regexPattern);
    if (match !== null) {
        onEach(match[1] !== undefined ? match[1] : match[0]);
    }
}

/**
 * Classify `entries` into matching files (visited via `onEach`) and subdirectory names (returned).
 *
 * @param {fs.Dirent[]} entries Directory entries from one level.
 * @param {string} relativePrefix Relative prefix for child entries.
 * @param {RegExp} regexPattern Regex for file paths.
 * @param {function(string): void} onEach Callback for matching files.
 * @returns {string[]} Names of subdirectory entries.
 */
function classifyEntries(entries, relativePrefix, regexPattern, onEach) {
    const subdirs = [];
    for (let entry of entries) {
        if (entry.isDirectory()) {
            subdirs.push(entry.name);
        } else {
            visitMatchingPath(relativePrefix + entry.name, regexPattern, onEach);
        }
    }
    return subdirs;
}

/**
 * Sequentially scan each name in `subdirs`, calling `done` when all are complete or on first error.
 *
 * @param {string[]} subdirs Subdirectory names.
 * @param {string} dir Absolute parent path.
 * @param {string} relativePrefix Relative prefix used during recursion.
 * @param {RegExp} regexPattern Regex for file paths.
 * @param {function(string): void} onEach Callback for matching files.
 * @param {function(Error=): void} done Completion callback.
 * @returns {void}
 */
function scanSubdirs(subdirs, dir, relativePrefix, regexPattern, onEach, done) {
    let i = 0;
    function next() {
        if (i >= subdirs.length) return done(null);
        const name = subdirs[i++];
        scanDir(path.join(dir, name), relativePrefix + name + '/', false, regexPattern, onEach, (err) => {
            if (err) return done(err);
            next();
        });
    }
    next();
}

/**
 * Asynchronously scan one directory level, then recurse into subdirectories sequentially.
 *
 * @param {string} dir Absolute directory path.
 * @param {string} relativePrefix Relative prefix for match paths.
 * @param {boolean} isRoot True for the initial call.
 * @param {RegExp} regexPattern Regex for file paths.
 * @param {function(string): void} onEach Callback for matching files.
 * @param {function(Error=): void} done Completion callback.
 * @returns {void}
 */
function scanDir(dir, relativePrefix, isRoot, regexPattern, onEach, done) {
    fs.readdir(dir, { withFileTypes: true }, (err, entries) => {
        if (err) {
            /* c8 ignore next */
            if (!isRoot && err.code === 'ENOENT') return done(null);
            return done(err);
        }
        const subdirs = classifyEntries(entries, relativePrefix, regexPattern, onEach);
        scanSubdirs(subdirs, dir, relativePrefix, regexPattern, onEach, done);
    });
}

/**
 * Scan a directory (and its subdirectories) for files whose relative paths match a regex pattern,
 * calling a callback for each match.
 *
 * The regex is matched against the **relative path from `directory`** (e.g. `eventstore.stream-x/foo.index`),
 * so patterns that capture a path prefix work transparently for both flat and nested layouts.
 *
 * The `onEach` callback receives the first capturing group of the match (`match[1]`), or the full
 * match (`match[0]`) when no capturing group is defined in the pattern.
 *
 * @param {string} directory The root directory to scan.
 * @param {RegExp} regexPattern The pattern to match relative file paths against.
 * @param {function(string)} onEach Called with the first capturing group (or full match) for each matching path.
 * @param {function(Error?)} onDone Called when the scan is complete, or with an error if one occurred.
 * @returns {void}
 */
function scanForFiles(directory, regexPattern, onEach, onDone) {
    scanDir(directory, '', true, regexPattern, onEach, onDone);
}

/**
 * Return true when both paths are equal or `child` is nested inside `parent`.
 *
 * @private
 * @param {string} parent
 * @param {string} child
 * @returns {boolean}
 */
function isSameOrParentDirectory(parent, child) {
    const relative = path.relative(parent, child);
    return !relative || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

export {
    resolvePath,
    ensureDirectory,
    scanForFiles,
    isSameOrParentDirectory,
    safeUnlink,
    ignoreDeletedFile,
    syncDirectory,
    syncFile,
    syncFileInBackground,
    syncDirectoryInBackground,
    writeFileAtomic
};
