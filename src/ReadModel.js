import events from 'events';
import { assert } from './utils/util.js';
import { applyEnvelope, resolveProjection, assertSource, readDecisionModel } from './Projection.js';

const DEFAULT_BATCH_SIZE = 1000;

/** Let other tasks of the event loop run, also outside of node. */
const nextTask = () => new Promise(resolve => (typeof setImmediate === 'function' ? setImmediate : setTimeout)(resolve));

/**
 * @typedef {object} Snapshot
 * @property {string|number|null} version
 * @property {object} query
 * @property {number} position
 * @property {any} state
 */

/**
 * @typedef {object} SnapshotStore Where a read model keeps its state between restarts, e.g. `fileSnapshotStore(dir)`.
 * @property {function(string): (Snapshot|null|Promise<Snapshot|null>)} load
 * @property {function(string, Snapshot): (void|Promise<void>)} save
 */

/**
 * A projection that follows an event source: it catches up on the matching events and then applies every new one.
 * With a snapshot store, its state and position are saved, so a restart only reads the events committed since.
 *
 * The catch-up yields to the event loop after every `batchSize` events, so a large store does not block it.
 *
 * Emits `'change'` with `(state, position)` whenever the state changed, `'progress'` with `(position)` after each
 * catch-up batch, `'caught-up'` with `(state, position)` once the catch-up reached the end of the store,
 * `'persisted'` with `(position)` after a snapshot was saved and `'error'` when reading or saving fails.
 */
class ReadModel extends events.EventEmitter {

    /**
     * @param {import('./Projection.js').EventSource} source
     * @param {import('./Projection.js').Projection|Object<string, import('./Projection.js').Projection>} projectionOrMap
     * @param {{ name?: string, snapshots?: SnapshotStore, version?: string|number, batchSize?: number }} [options]
     *   `snapshots` keeps state and position under `name` (required then); `version` invalidates a saved state when
     *   the handlers changed; `batchSize` is the number of events applied before yielding to the event loop.
     */
    constructor(source, projectionOrMap, { name = null, snapshots = null, version = null, batchSize = DEFAULT_BATCH_SIZE } = {}) {
        super();
        assertSource(source);
        assert(!snapshots || (typeof name === 'string' && name !== ''), 'A read model with snapshots needs a name.');
        assert(Number.isInteger(batchSize) && batchSize > 0, 'The batch size must be a positive integer.');
        this.source = source;
        this.projection = resolveProjection(projectionOrMap);
        this.name = name;
        this.snapshots = snapshots;
        this.version = version;
        this.batchSize = batchSize;
        this.state = this.projection.initialState;
        this.position = 0;
        this.caughtUp = false;
        this.saving = null;
        this.pendingSave = false;
        this.abort = new AbortController();
        this.ready = new Promise((resolve, reject) => {
            this.once('caught-up', () => resolve(this));
            this.once('closed', () => reject(new Error('The read model was closed before it caught up.')));
        });
        this.ready.catch(() => {});
        this.running = this.run().catch(error => {
            if (!this.abort.signal.aborted) {
                this.emit('error', error);
            }
        });
    }

    /**
     * @private
     */
    async run() {
        await this.restore();
        let { events: catchUp, condition } = await this.source.query(this.projection.query, { from: this.position + 1 });
        if (this.position > condition.noneMatchAfter) {
            // The store was truncated or replaced since the snapshot was saved; rebuild.
            this.state = this.projection.initialState;
            this.position = 0;
            ({ events: catchUp, condition } = await this.source.query(this.projection.query, { from: 1 }));
        }
        let count = 0;
        for await (const envelope of catchUp) {
            if (this.abort.signal.aborted) return;
            this.apply(envelope);
            if (++count % this.batchSize === 0) {
                this.emit('progress', this.position);
                await nextTask();
            }
        }
        if (this.abort.signal.aborted) return;
        this.caughtUp = true;
        this.emit('caught-up', this.state, this.position);

        const live = this.source.follow(condition, { from: condition.noneMatchAfter + 1, signal: this.abort.signal });
        for await (const envelope of live) {
            this.apply(envelope);
        }
    }

    /**
     * Load the saved state unless it was built by another version or from another query.
     * @private
     */
    async restore() {
        if (!this.snapshots) {
            return;
        }
        const snapshot = await this.snapshots.load(this.name);
        if (!snapshot || snapshot.version !== this.version || JSON.stringify(snapshot.query) !== JSON.stringify(this.projection.query)) {
            return;
        }
        this.state = snapshot.state;
        this.position = snapshot.position;
    }

    /**
     * @private
     * @param {import('./Projection.js').Envelope} envelope
     */
    apply(envelope) {
        const previous = this.state;
        this.state = applyEnvelope(this.projection, this.state, envelope);
        this.position = envelope.position;
        if (this.state === previous) {
            return;
        }
        this.emit('change', this.state, this.position);
        this.scheduleSave();
    }

    /**
     * Save at most once per tick, and never two snapshots at the same time.
     * @private
     */
    scheduleSave() {
        if (!this.snapshots) {
            return;
        }
        this.pendingSave = true;
        if (this.saving) {
            return;
        }
        this.saving = nextTask()
            .then(() => this.save())
            .finally(() => {
                this.saving = null;
                if (this.pendingSave) this.scheduleSave();
            });
    }

    /**
     * @private
     */
    async save() {
        if (!this.pendingSave) {
            return;
        }
        this.pendingSave = false;
        const snapshot = { version: this.version, query: this.projection.query, position: this.position, state: this.state };
        try {
            await this.snapshots.save(this.name, snapshot);
            this.emit('persisted', snapshot.position);
        } catch (error) {
            this.emit('error', error);
        }
    }

    /**
     * Build a decision model from the current state, e.g. to decide on a narrow read model that is kept up to date
     * anyway instead of replaying its events per command. Waits for the catch-up and only reads the events the
     * read model has not applied yet.
     *
     * @api
     * @returns {Promise<import('./Projection.js').DecisionModel>}
     */
    async decisionModel() {
        await this.ready;
        return readDecisionModel(this.source, this.projection, this.state, this.position + 1);
    }

    /**
     * Stop following the source and save a pending snapshot.
     *
     * @api
     * @returns {Promise<void>}
     */
    async close() {
        this.abort.abort();
        this.emit('closed');
        await this.running;
        while (this.saving) {
            await this.saving;
        }
        if (this.pendingSave) {
            await this.save();
        }
    }
}

/**
 * Create a read model for the projection(s) that follows the source.
 *
 * @param {import('./Projection.js').EventSource} source
 * @param {import('./Projection.js').Projection|Object<string, import('./Projection.js').Projection>} projectionOrMap
 * @param {{ name?: string, snapshots?: SnapshotStore, version?: string|number, batchSize?: number }} [options]
 * @returns {ReadModel}
 */
function createReadModel(source, projectionOrMap, options) {
    return new ReadModel(source, projectionOrMap, options);
}

export { ReadModel, createReadModel };
