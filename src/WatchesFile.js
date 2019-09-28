const Watcher = require('./Watcher');

/**
 * A mixin that provides a file watcher for this.fileName which triggers a method `onChange` on the class, that needs to be implemented.
 *
 * @param {constructor} Base
 * @returns {{new(): {watchFile(): void, close(): void, open(): boolean, stopWatching(): void}, prototype: {watchFile(): void, close(): void, open(): boolean, stopWatching(): void}}}
 * @constructor
 */
const WatchesFile = Base => class extends Base {

    /**
     * @private
     */
    watchFile() {
        this.stopWatching();
        this.watcher = new Watcher(this.fileName);
        this.watcher.on('change', this.onChange.bind(this));
        this.watcher.on('rename', this.onRename.bind(this));
    }

    /**
     * @private
     */
    stopWatching() {
        if (this.watcher) {
            this.watcher.close();
            this.watcher = null;
        }
    }

    /**
     * @api
     * @returns {boolean}
     */
    open() {
        if (this.fd) {
            return false;
        }

        this.watchFile();
        return super.open();
    }

    /**
     * @api
     */
    close() {
        this.stopWatching();
        super.close();
    }

};

module.exports = WatchesFile;