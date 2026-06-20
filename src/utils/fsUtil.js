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
 * Synchronously scan one directory level, then recurse into subdirectories.
 *
 * @param {string} dir Absolute directory path.
 * @param {string} relativePrefix Relative prefix for match paths.
 * @param {boolean} isRoot True for the initial call.
 * @param {RegExp} regexPattern Regex for file paths.
 * @param {function(string): void} onEach Callback for matching files.
 * @returns {void}
 */
function scanDirSync(dir, relativePrefix, isRoot, regexPattern, onEach) {
    let entries;
    try {
        entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch (err) {
        if (!isRoot && err.code === 'ENOENT') return;
        throw err;
    }
    const subdirs = classifyEntries(entries, relativePrefix, regexPattern, onEach);
    for (const name of subdirs) {
        scanDirSync(path.join(dir, name), relativePrefix + name + '/', false, regexPattern, onEach);
    }
}

/**
 * Synchronous counterpart to {@link scanForFiles}. Used on cold paths where a blocking scan is
 * acceptable and the result is needed immediately (e.g. lazily registering a partition referenced
 * by a freshly appended index entry during live watching).
 *
 * @param {string} directory The root directory to scan.
 * @param {RegExp} regexPattern The pattern to match relative file paths against.
 * @param {function(string)} onEach Called with the first capturing group (or full match) for each matching path.
 * @returns {void}
 */
function scanForFilesSync(directory, regexPattern, onEach) {
    scanDirSync(directory, '', true, regexPattern, onEach);
}

export {
    resolvePath,
    ensureDirectory,
    scanForFiles,
    scanForFilesSync,
};
