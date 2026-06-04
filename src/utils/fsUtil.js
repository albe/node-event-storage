import fs from 'fs';
import path from 'path';
import { mkdirpSync } from 'mkdirp';

const SAFE_RELATIVE_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_]*(?:[\/:@~+=\-#.][A-Za-z0-9_]+)*$/;

// Best-effort cleanup for temporary files after interrupted/failed writes.
function safeUnlink(fileName) {
    try {
        fs.unlinkSync(fileName);
    } catch (e) {
        if (e.code !== 'ENOENT') {
            throw e;
        }
    }
}

// Prevent partially written persistence files from replacing the last valid state.
function writeFileAtomic(fileName, data, options = {}, onSuccess = null) {
    const tmpFileName = options.tmpFileName || `${fileName}.tmp`;
    const writeOptions = options.encoding ? { encoding: options.encoding } : undefined;
    try {
        fs.writeFileSync(tmpFileName, data, writeOptions);
        fs.renameSync(tmpFileName, fileName);
        if (typeof onSuccess === 'function') {
            onSuccess();
        }
    } catch (e) {
        safeUnlink(tmpFileName);
        throw e;
    }
    return fileName;
}

function isSafeRelativeName(name) {
    return typeof name === 'string'
        && name !== ''
        && SAFE_RELATIVE_NAME_PATTERN.test(name);
}

function resolvePathWithinRoot(rootDirectory, relativePath) {
    const root = path.resolve(rootDirectory);
    const resolvedPath = path.resolve(root, relativePath);
    const rootRelativePath = path.relative(root, resolvedPath);
    if (rootRelativePath.startsWith('..') || path.isAbsolute(rootRelativePath)) {
        throw new Error(`Invalid relative path "${relativePath}".`);
    }
    return resolvedPath;
}

/**
 * Ensure that the given directory exists.
 * @param {string} dirName
 * @return {boolean} true if the directory existed already
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
 * @param {string} relativePath
 * @param {RegExp} regexPattern
 * @param {function(string)} onEach
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
 * @param {fs.Dirent[]} entries
 * @param {string} relativePrefix
 * @param {RegExp} regexPattern
 * @param {function(string)} onEach
 * @returns {string[]} names of subdirectory entries
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
 * @param {string[]} subdirs
 * @param {string} dir
 * @param {string} relativePrefix
 * @param {RegExp} regexPattern
 * @param {function(string)} onEach
 * @param {function(Error?)} done
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
 * @param {string} dir
 * @param {string} relativePrefix
 * @param {boolean} isRoot
 * @param {RegExp} regexPattern
 * @param {function(string)} onEach
 * @param {function(Error?)} done
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
 */
function scanForFiles(directory, regexPattern, onEach, onDone) {
    scanDir(directory, '', true, regexPattern, onEach, onDone);
}

export {
    ensureDirectory,
    safeUnlink,
    writeFileAtomic,
    scanForFiles,
    isSafeRelativeName,
    resolvePathWithinRoot,
};
