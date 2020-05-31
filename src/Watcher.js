const fs = require('fs');
const path = require('path');
const EventEmitter = require('events');
const { assert } = require('./util');

const directoryWatchers = {};

/**
 * A reference counting singleton nodejs watcher for directories.
 * Emits events 'change' and 'rename' with the file name as argument.
 */
class DirectoryWatcher extends EventEmitter {

    /**
     * @param {string} directory
     * @param {object} [options] The options to pass to the fs.watch call. See https://nodejs.org/api/fs.html#fs_fs_watch_filename_options_listener
     * @returns {DirectoryWatcher}
     */
    constructor(directory, options = {}) {
        directory = path.normalize(directory);
        assert(fs.existsSync(directory), 'Can not watch a non-existing directory.');

        if (directoryWatchers[directory]) {
            directoryWatchers[directory].references++;
            return directoryWatchers[directory];
        }
        super();
        directoryWatchers[directory] = this;
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
            directoryWatchers[this.directory] = null;
        }
    }

}

/**
 * A watcher for a single file or a directory, with the possibility to provide a filter method for file names to watch.
 */
class Watcher {

    /**
     * @param {string} fileOrDirectory
     * @param {function(string): boolean} [fileFilter] A filter that will receive a filename and needs to return true if this watcher should be invoked.
     * @returns {Watcher}
     */
    constructor(fileOrDirectory, fileFilter = null) {
        const isDirectory = fs.statSync(fileOrDirectory).isDirectory();
        let directory = fileOrDirectory;
        if (!isDirectory) {
            directory = path.dirname(fileOrDirectory);
        }
        this.watcher = new DirectoryWatcher(directory);

        if (!isDirectory) {
            const filename = path.basename(fileOrDirectory);
            fileFilter = changedFilename => changedFilename === filename;
        } else if (fileFilter === null) {
            fileFilter = () => true;
        }

        this.fileFilter = fileFilter;
        this.onChange = this.onChange.bind(this);
        this.onRename = this.onRename.bind(this);
        this.watcher.on('change', this.onChange);
        this.watcher.on('rename', this.onRename);
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
        if (!(this.watcher instanceof EventEmitter)) {
            return;
        }
        this.watcher.removeListener('change', this.onChange);
        this.watcher.removeListener('rename', this.onRename);
        this.watcher.close();
        this.watcher = null;
        this.handlers = { change: [], rename: [] };
    }

}

module.exports = Watcher;