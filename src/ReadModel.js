import events from 'events';
import fs from 'fs';
import path from 'path';
import { assert } from './utils/util.js';
import { ensureDirectory, isSameOrParentDirectory, writeFileAtomic } from './utils/fsUtil.js';
import { applyEvent, resolveProjection, DecisionModel } from './Projection.js';
import { ReadOnly as ReadOnlyStorage } from './Storage.js';

/**
 * A projection that follows the store: it catches up through the stream indexes and then applies every new matching
 * event as it is written. Optionally its state and position are persisted, so a restart only reads the events
 * committed since.
 *
 * Emits `'change'` with `(state, position)` whenever the state changed.
 */
class ReadModel extends events.EventEmitter {

    /**
     * @param {import('./EventStore.js').default} eventStore
     * @param {import('./Projection.js').Projection|Object<string, import('./Projection.js').Projection>} projectionOrMap
     * @param {{ name?: string, persist?: boolean, version?: string|number }} [options]
     *   `persist` stores state and position under `name` (required then); `version` invalidates a persisted
     *   state when the handlers changed.
     */
    constructor(eventStore, projectionOrMap, { name = null, persist = false, version = null } = {}) {
        super();
        assert(!persist || (typeof name === 'string' && name !== ''), 'A persisted read model needs a name.');
        this.eventStore = eventStore;
        this.projection = resolveProjection(eventStore, projectionOrMap);
        this.name = name;
        this.version = version;
        this.fileName = persist ? this.resolveFileName(name) : null;
        this.persisting = null;
        this.selector = eventStore.compileDcbQuery(this.projection.query);
        this.state = this.projection.initialState;
        this.position = 0;
        this.restore();
        this.update();

        this.storage = eventStore.storage;
        this.isReadOnly = this.storage instanceof ReadOnlyStorage;
        this.onWrite = this.isReadOnly
            ? (document, entry) => this.follow(entry.number, document)
            : (indexName, number, document) => indexName === '_all' && this.follow(number, document);
        this.storage.on(this.isReadOnly ? 'wrote' : 'index-add', this.onWrite);
    }

    /**
     * @private
     * @param {string} name
     * @returns {string}
     */
    resolveFileName(name) {
        const directory = path.join(this.eventStore.storage.indexDirectory, 'readmodels');
        const fileName = path.join(directory, `${this.eventStore.storage.storageFile}.${name}.json`);
        assert(isSameOrParentDirectory(directory, path.dirname(fileName)), `Read model name "${name}" leaves the readmodels directory.`);
        return fileName;
    }

    /**
     * Load the persisted state unless it was built by another version or from another query.
     * @private
     */
    restore() {
        if (!this.fileName || !fs.existsSync(this.fileName)) {
            return;
        }
        const snapshot = JSON.parse(fs.readFileSync(this.fileName, 'utf8'));
        if (snapshot.version !== this.version || JSON.stringify(snapshot.query) !== JSON.stringify(this.projection.query)) {
            return;
        }
        if (snapshot.position > this.eventStore.length) {
            return; // the store was truncated or replaced; rebuild
        }
        this.state = snapshot.state;
        this.position = snapshot.position;
    }

    /**
     * Synchronously apply all matching events committed since the current position.
     *
     * @api
     * @param {number} [until] The last global position to read (inclusive). Defaults to the store length.
     * @returns {ReadModel} This read model.
     */
    update(until = this.eventStore.length) {
        if (until <= this.position) {
            return this;
        }
        const previous = this.state;
        const stream = this.eventStore.fromStreams('_readmodel_' + (this.name || ''), this.selector, this.position + 1, until);
        stream.forEach((event, metadata) => {
            this.state = applyEvent(this.eventStore, this.projection, this.state, event, metadata);
        });
        this.position = until;
        this.changed(previous);
        return this;
    }

    /**
     * Apply a document that was just written at the given global position.
     * @private
     * @param {number} number
     * @param {{ payload: object, metadata: object }} document
     */
    follow(number, document) {
        if (number <= this.position) {
            return;
        }
        // Catch up on anything missed before, without the current document whose stream indexes may not be written yet.
        this.update(number - 1);
        const previous = this.state;
        this.state = applyEvent(this.eventStore, this.projection, this.state, document.payload, document.metadata);
        this.position = number;
        this.changed(previous);
    }

    /**
     * @private
     * @param {any} previous The state before the change.
     */
    changed(previous) {
        if (this.state === previous) {
            return;
        }
        this.emit('change', this.state, this.position);
        this.schedulePersist();
    }

    /**
     * @private
     */
    schedulePersist() {
        if (!this.fileName || this.persisting) {
            return;
        }
        this.persisting = setImmediate(() => {
            this.persisting = null;
            this.persist();
        });
    }

    /**
     * Write state and position atomically.
     * @private
     */
    persist() {
        ensureDirectory(path.dirname(this.fileName));
        const snapshot = { version: this.version, query: this.projection.query, position: this.position, state: this.state };
        writeFileAtomic(this.fileName, JSON.stringify(snapshot));
        this.emit('persisted', this.position);
    }

    /**
     * Create a decision model from the current state, e.g. to decide on a narrow read model that is kept up to date
     * anyway instead of replaying its events per command.
     *
     * @api
     * @returns {DecisionModel}
     */
    decisionModel() {
        this.update();
        const { condition } = this.eventStore.query(this.projection.query);
        return new DecisionModel(this.eventStore, this.projection, this.state, condition);
    }

    /**
     * Stop following the store and write a pending persist.
     *
     * @api
     */
    close() {
        this.storage.removeListener(this.isReadOnly ? 'wrote' : 'index-add', this.onWrite);
        if (this.persisting) {
            clearImmediate(this.persisting);
            this.persisting = null;
            this.persist();
        }
    }
}

/**
 * Create a read model for the projection(s) that follows the store.
 *
 * @param {import('./EventStore.js').default} eventStore
 * @param {import('./Projection.js').Projection|Object<string, import('./Projection.js').Projection>} projectionOrMap
 * @param {{ name?: string, persist?: boolean, version?: string|number }} [options]
 * @returns {ReadModel}
 */
function createReadModel(eventStore, projectionOrMap, options) {
    return new ReadModel(eventStore, projectionOrMap, options);
}

export { ReadModel, createReadModel };
