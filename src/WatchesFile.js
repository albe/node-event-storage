const fs = require('fs');

/**
 * A mixin that provides a file watcher for this.fileName which triggers a method `onChange` on the class, that needs to be implemented.
 *
 * @param {constructor} Base
 * @returns {{new(): {watchFile(): void, close(): void, open(): boolean, stopWatching(): void}, prototype: {watchFile(): void, close(): void, open(): boolean, stopWatching(): void}}}
 * @constructor
 */
const WatchesFile = Base => class extends Base {

    watcher = null;

    /**
     * @private
     */
    watchFile() {
        this.stopWatching();
        this.watcher = fs.watch(this.fileName, { persistent: false }, this.onChange.bind(this));
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