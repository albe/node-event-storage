import fs from 'fs';
import path from 'path';
import events from 'events';
import { assert } from './utils/util.js';
import { isSameOrParentDirectory } from "./utils/fsUtil.js";

/** @type {Map<string, DirectoryWatcher>} */
const directoryWatchers = new Map();

/**
 * A reference counting singleton nodejs watcher for directories.
 * Emits events 'change' and 'rename' with the file name as argument.
 */
class DirectoryWatcher extends events.EventEmitter {

    /**
     * Normalize fs.watch filenames to be relative to the watched directory.
     * On Windows recursive watches may yield absolute `\\?\` names or directory names.
     *
     * @private
     * @param {string|Buffer|null} filename
     * @returns {string}
     */
    normalizeFilename(filename) {
        if (!filename) return '';
        if (Buffer.isBuffer(filename)) {
            filename = filename.toString();
        }
        if (typeof filename !== 'string' || filename.length === 0) {
            return '';
        }

        const stripped = filename
            .replace(/^\\\\\?\\UNC\\/i, '\\\\')
            .replace(/^\\\\\?\\/, '');
        const normalized = path.normalize(stripped);

        if (path.isAbsolute(normalized)) {
            const relative = path.relative(this.directory, normalized);
            return relative === '.' ? '' : relative;
        }

        return normalized === '.' ? '' : normalized;
    }

    /**
     * Forward fs.watch events with normalized filenames.
     *
     * @private
     * @param {string} eventType
     * @param {string|Buffer|null} filename
     */
    onFsEvent(eventType, filename) {
        const normalized = this.normalizeFilename(filename);
        if (eventType === 'rename' && !filename) {
            // Close if the watched directory was removed
            this.close();
            return;
        }
        this.emit(eventType, normalized);
    }

    /**
     * @param {string} directory
     * @param {object} [options] The options to pass to the fs.watch call. See https://nodejs.org/api/fs.html#fs_fs_watch_filename_options_listener
     * @returns {DirectoryWatcher}
     */
    constructor(directory, options = {}) {
        directory = path.normalize(directory);
        const watchOptions = Object.assign({ persistent: false, recursive: true, encoding: 'utf8' }, options);
        const watcherKey = `${directory}|${JSON.stringify(watchOptions)}`;

        if (directoryWatchers.has(watcherKey)) {
            const watcher = directoryWatchers.get(watcherKey);
            watcher.references++;
            return watcher;
        }
        assert(fs.existsSync(directory), `Can not watch a non-existing directory "${directory}".`);
        assert(fs.statSync(directory).isDirectory(), `Can only watch directories, but "${directory}" is none.`);
        super();
        this.setMaxListeners(0);
        directoryWatchers.set(watcherKey, this);
        this.watcherKey = watcherKey;
        this.directory = directory;
        this.onFsEvent = this.onFsEvent.bind(this);
        this.watcher = fs.watch(directory, watchOptions, this.onFsEvent);
        this.references = 1;
    }

    /**
     * Close this watcher.
     * @returns void
     */
    close() {
        this.references--;
        if (this.references === 0 && this.watcher) {
            this.watcher.close();
            this.watcher = null;
            directoryWatchers.delete(this.watcherKey);
        }
    }

}

/**
 * A watcher for a single file or a directory, with the possibility to provide a filter method for file names to watch.
 */
class Watcher extends events.EventEmitter {

    /**
     * Remove redundant nested directories so each tree is watched only once.
     *
     * @private
     * @param {string[]} directories
     * @param {boolean} recursive
     * @returns {[string[], string[]]} Tuple of [watchedDirectories, removedRelativePrefixes]
     */
    static deduplicateDirectories(directories, recursive = true) {
        const normalized = [...new Set(directories.map(dir => path.resolve(path.normalize(dir))))];
        if (!recursive) {
            return [normalized, []];
        }
        normalized.sort((a, b) => a.length - b.length);
        const deduplicated = [];
        const removedRelativePrefixes = new Set();
        for (const directory of normalized) {
            const parent = deduplicated.find(current => isSameOrParentDirectory(current, directory));
            if (parent) {
                const relative = path.normalize(path.relative(parent, directory));
                if (relative && relative !== '.') {
                    removedRelativePrefixes.add(relative);
                }
                continue;
            }
            deduplicated.push(directory);
        }
        return [deduplicated, [...removedRelativePrefixes]];
    }

    /**
     * Strip the longest matching removed relative prefix from an observed filename.
     *
     * @private
     * @param {string} filename
     * @returns {string}
     */
    normalizeSubDirectoryFilename(filename) {
        if (!filename || !this.subDirectoryPrefixes || this.subDirectoryPrefixes.length === 0) {
            return filename;
        }

        filename = path.normalize(filename);
        for (const subDirectoryPrefix of this.subDirectoryPrefixes) {
            if (filename === subDirectoryPrefix) {
                return '';
            }
            const prefix = subDirectoryPrefix + path.sep;
            if (filename.startsWith(prefix)) {
                return filename.slice(prefix.length);
            }
        }
        return filename;
    }

    /**
     * @param {string|string[]} fileOrDirectory The filename or directory or list of directories to watch
     * @param {function(string): boolean} [fileFilter] A filter that will receive a filename and needs to return true if this watcher should be invoked. Will be ignored if the first argument is a file.
     * @param {object|null} [watchOptions] Options forwarded to fs.watch for each directory watcher.
     * @returns {Watcher}
     */
    constructor(fileOrDirectory, fileFilter = null, watchOptions = null) {
        super();
        this.validEventTypes = new Set(['change', 'rename']);
        let directories;
        const options = Object.assign({ recursive: true }, watchOptions);
        this.subDirectoryPrefixes = [];
        if (typeof fileOrDirectory === 'string') {
            directories = [fileOrDirectory];
            if (!fs.statSync(fileOrDirectory).isDirectory()) {
                directories = [path.dirname(fileOrDirectory)];
                const filename = path.basename(fileOrDirectory);
                this.fixedFilename = filename;
                fileFilter = changedFilename => changedFilename === filename;
            }
        } else {
            this.fixedFilename = null;
            const [deduplicatedDirectories, removedRelativePrefixes] = Watcher.deduplicateDirectories(fileOrDirectory, options.recursive);
            directories = deduplicatedDirectories;
            this.subDirectoryPrefixes = removedRelativePrefixes.sort((a, b) => b.length - a.length);
        }

        this.watchers = directories.map(dir => new DirectoryWatcher(dir, options));

        if (fileFilter === null) {
            fileFilter = () => true;
        }

        this.fileFilter = fileFilter;
        this.onChange = this.onChange.bind(this);
        this.onRename = this.onRename.bind(this);
        this.watchers.forEach(watcher => {
            watcher.on('change', this.onChange);
            watcher.on('rename', this.onRename);
        });
    }

    /**
     * Override to validate against allowed event types.
     * @override
     * @param {string} eventType
     * @param {function} listener
     * @returns {Watcher}
     */
    on(eventType, listener) {
        assert(this.validEventTypes.has(eventType), `Event type ${eventType} is unknown. Only 'change' and 'rename' are supported.`);
        return super.on(eventType, listener);
    }

    /**
     * @private
     * @param {string} filename
     */
    onChange(filename) {
        if (!filename && this.fixedFilename) {
            filename = this.fixedFilename;
        }
        filename = this.normalizeSubDirectoryFilename(filename);
        if (!filename || !this.fileFilter(filename)) {
            return;
        }
        this.emit('change', filename);
    }

    /**
     * @private
     * @param {string} filename
     */
    onRename(filename) {
        if (!filename && this.fixedFilename) {
            filename = this.fixedFilename;
        }
        filename = this.normalizeSubDirectoryFilename(filename);
        if (!filename || !this.fileFilter(filename)) {
            return;
        }
        this.emit('rename', filename);
    }

    /**
     * Close this watcher and release all handlers.
     * @api
     */
    close() {
        this.watchers.forEach(watcher => {
            watcher.removeListener('change', this.onChange);
            watcher.removeListener('rename', this.onRename);
            watcher.close();
        });
        this.watchers = [];
        this.removeAllListeners();
    }

}

export default Watcher;