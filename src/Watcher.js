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
class DirectoryWatcher {

    /**
     * Register a handler with optional filename resolver.
     *
     * @param {function(string, string): void} handler
     * @param {function(string, string): string|null} [filenameResolver]
     */
    registerHandler(handler, filenameResolver = null) {
        this.subscriptions.set(handler, typeof filenameResolver === 'function' ? filenameResolver : null);
    }

    /**
     * Unregister a previously added handler.
     *
     * @param {function(string, string): void} handler
     */
    unregisterHandler(handler) {
        this.subscriptions.delete(handler);
    }

    /**
     * Dispatch an fs event to matching subscribers.
     *
     * @private
     * @param {'change'|'rename'} eventType
     * @param {string} filename
     */
    dispatch(eventType, filename) {
        for (const [handler, filenameResolver] of this.subscriptions) {
            const resolvedFilename = filenameResolver ? filenameResolver(filename, eventType) : filename;
            if (resolvedFilename === null || resolvedFilename === undefined) {
                continue;
            }
            handler(eventType, resolvedFilename);
        }
    }

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
        filename = path.normalize(stripped);

        if (path.isAbsolute(filename)) {
            filename = path.relative(this.directory, filename);
        }
        if (!filename || filename === '.') {
            return '';
        }
        return filename.replace(/\\/g, '/');
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
        this.dispatch(eventType, normalized);
    }

    /**
     * @param {string} directory
     * @param {object} [options] The options to pass to the fs.watch call. See https://nodejs.org/api/fs.html#fs_fs_watch_filename_options_listener
     * @returns {DirectoryWatcher}
     */
    constructor(directory, options = {}) {
        const watchOptions = Object.assign({ recursive: true }, options);
        const watcherKey = `${directory}|r:${watchOptions.recursive}`;

        if (directoryWatchers.has(watcherKey)) {
            const watcher = directoryWatchers.get(watcherKey);
            watcher.references++;
            return watcher;
        }
        assert(fs.existsSync(directory), `Can not watch a non-existing directory "${directory}".`);
        assert(fs.statSync(directory).isDirectory(), `Can only watch directories, but "${directory}" is none.`);
        directoryWatchers.set(watcherKey, this);
        this.watcherKey = watcherKey;
        this.directory = directory;
        this.subscriptions = new Map();
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

const VALID_WATCH_EVENT_TYPES = ['change', 'rename'];

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
        const normalized = [...new Set(directories)];
        if (!recursive) {
            return [normalized, []];
        }
        normalized.sort((a, b) => a.length - b.length);
        const deduplicated = [];
        const removedRelativePrefixes = new Set();
        for (const directory of normalized) {
            const parent = deduplicated.find(current => isSameOrParentDirectory(current, directory));
            if (parent) {
                const relative = path.relative(parent, directory).replace(/\\/g, '/');
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
     * Resolve a filename to a path relative to the longest matching watched prefix.
     *
     * @private
     * @param {string} filename
     * @returns {string}
     */
    resolveRelativeFilename(filename) {
        filename = path.normalize(filename).replace(/\\/g, '/');
        for (const prefix of this.relativePrefixes) {
            if (filename === prefix) {
                return '';
            }
            const withSeparator = prefix + '/';
            if (filename.startsWith(withSeparator)) {
                return filename.slice(withSeparator.length);
            }
        }
        return filename;
    }

    /**
     * Resolve the final filename used for matching and emitting.
     * Returns `null` when the filename is filtered out.
     *
     * @private
     * @param {string} filename
     * @returns {string|null}
     */
    resolveFilename(filename) {
        if (!filename) {
            if (!this.fileFilter(filename)) {
                return null;
            }
            return this.fixedFilename || '';
        }

        filename = this.resolveRelativeFilename(filename);

        if (!this.fileFilter(filename)) {
            return null;
        }
        return filename;
    }

    /**
     * @param {string|string[]} fileOrDirectory The filename or directory or list of directories to watch
     * @param {function(string): boolean} [fileFilter] A filter that will receive a filename and needs to return true if this watcher should be invoked.
     * @param {object|null} [options] Internal watcher options.
     * @param {boolean} [options.recursive=true] Whether directories are watched recursively.
     * @param {string} [options.rootDirectory] Root directory used to relativize single-file watcher events.
     * @returns {Watcher}
     */
    constructor(fileOrDirectory, fileFilter = null, options = null) {
        super();
        const watchOptions = Object.assign({ recursive: true }, options);
        delete watchOptions.rootDirectory;
        this.relativePrefixes = [];
        let directories;
        if (typeof fileOrDirectory === 'string') {
            directories = [fileOrDirectory];
            if (!fs.statSync(fileOrDirectory).isDirectory()) {
                const rootDirectory = options?.rootDirectory || path.dirname(fileOrDirectory);

                directories = [rootDirectory];
                this.fixedFilename = path.relative(rootDirectory, fileOrDirectory).replace(/\\/g, '/');
                if (fileFilter === null) {
                    fileFilter = changedFilename => changedFilename === this.fixedFilename;
                }
            }
        } else {
            this.fixedFilename = null;
            const [deduplicatedDirectories, removedRelativePrefixes] = Watcher.deduplicateDirectories(fileOrDirectory, watchOptions.recursive);
            directories = deduplicatedDirectories;
            this.relativePrefixes = removedRelativePrefixes.sort((a, b) => b.length - a.length);
        }

        this.watchers = directories.map(dir => new DirectoryWatcher(dir, watchOptions));

        if (fileFilter === null) {
            fileFilter = () => true;
        }

        this.fileFilter = fileFilter;
        this.resolveFilename = this.resolveFilename.bind(this);
        this.emitHandler = this.emit.bind(this);
        this.watchers.forEach(watcher => {
            watcher.registerHandler(this.emitHandler, this.resolveFilename);
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
        assert(VALID_WATCH_EVENT_TYPES.includes(eventType), `Event type ${eventType} is unknown. Only 'change' and 'rename' are supported.`);
        return super.on(eventType, listener);
    }

    /**
     * Close this watcher and release all handlers.
     * @api
     */
    close() {
        this.watchers.forEach(watcher => {
            watcher.unregisterHandler(this.emitHandler);
            watcher.close();
        });
        this.watchers = [];
        this.removeAllListeners();
    }

}

export default Watcher;