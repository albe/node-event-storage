const Watcher = require('./Watcher');

/**
 * A mixin that provides a file watcher for this.fileName which triggers a method `onChange` on the class, that needs to be implemented.
 *
 * @template T
 * @param {T} Base
 * @returns {T} An anonymous class that extends Base
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
        if (super.open()) {
            this.watchFile();
            return true;
        }
        return false;
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