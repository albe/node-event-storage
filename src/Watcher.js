const fs = require('fs');
const path = require('path');
const events = require('events');
const { assert } = require('./util');

/** @type {Map<string, DirectoryWatcher>} */
const directoryWatchers = new Map();

/**
 * A reference counting singleton nodejs watcher for directories.
 * Emits events 'change' and 'rename' with the file name as argument.
 */
class DirectoryWatcher extends events.EventEmitter {

    /**
     * @param {string} directory
     * @param {object} [options] The options to pass to the fs.watch call. See https://nodejs.org/api/fs.html#fs_fs_watch_filename_options_listener
     * @returns {DirectoryWatcher}
     */
    constructor(directory, options = {}) {
        directory = path.normalize(directory);

        if (directoryWatchers.has(directory)) {
            const watcher = directoryWatchers.get(directory);
            watcher.references++;
            return watcher;
        }
        assert(fs.existsSync(directory), `Can not watch a non-existing directory "${directory}".`);
        assert(fs.statSync(directory).isDirectory(), `Can only watch directories, but "${directory}" is none.`);
        super();
        directoryWatchers.set(directory, this);
        this.directory = directory;
        this.watcher = fs.watch(directory, Object.assign({ persistent: false }, options), this.emit.bind(this));
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
            directoryWatchers.delete(this.directory);
        }
    }

}

/**
 * A watcher for a single file or a directory, with the possibility to provide a filter method for file names to watch.
 */
class Watcher {

    /**
     * @param {string|string[]} fileOrDirectory The filename or directory or list of directories to watch
     * @param {function(string): boolean} [fileFilter] A filter that will receive a filename and needs to return true if this watcher should be invoked. Will be ignored if the first argument is a file.
     * @returns {Watcher}
     */
    constructor(fileOrDirectory, fileFilter = null) {
        let directories;
        if (typeof fileOrDirectory === 'string') {
            if (!fs.statSync(fileOrDirectory).isDirectory()) {
                directories = [path.dirname(fileOrDirectory)];
                const filename = path.basename(fileOrDirectory);
                fileFilter = changedFilename => changedFilename === filename;
            } else {
                directories = [fileOrDirectory];
            }
        } else {
            directories = [...new Set(fileOrDirectory.map(path.normalize))];
        }

        this.watchers = directories.map(dir => new DirectoryWatcher(dir));

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
        this.handlers = { change: [], rename: [] };
    }

    /**
     * Register a new handler that is triggered if the fileFilter matches.
     * @param {string} eventType
     * @param {function(string): void} handler A handler method that should be invoked with the filename as argument
     * @api
     */
    on(eventType, handler) {
        assert(eventType in this.handlers, `Event type ${eventType} is unknown. Only 'change' and 'rename' are supported.`);

        this.handlers[eventType].push(handler);
    }

    /**
     * @private
     * @param {string} filename
     */
    onChange(filename) {
        if (this.handlers.change.length === 0) {
            return;
        }
        if (!filename || !this.fileFilter(filename)) {
            return;
        }
        this.handlers.change
            .forEach((handler) => handler(filename));
    }

    /**
     * @private
     * @param {string} filename
     */
    onRename(filename) {
        if (this.handlers.rename.length === 0) {
            return;
        }
        if (!filename || !this.fileFilter(filename)) {
            return;
        }
        this.handlers.rename
            .forEach((handler) => handler(filename));
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
        this.handlers = { change: [], rename: [] };
    }

}

module.exports = Watcher;