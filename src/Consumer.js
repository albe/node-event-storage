import stream from 'stream';
import { assert } from './utils/util.js';
import { normalizeConsumerStateArgs } from './utils/apiHelpers.js';
import Storage from './Storage/ReadableStorage.js';
import StorageSubscription from './Consumer/StorageSubscription.js';
import FlatFilePersistence from './Consumer/FlatFilePersistence.js';
import ConsumerPersistence from './Consumer/ConsumerPersistence.js';

/**
 * Implements an event-driven durable Consumer that provides at-least-once delivery semantics or exactly-once processing semantics if only using setState().
 */
class Consumer extends stream.Readable {

    /**
     * @private
     * @param {unknown} value
     * @param {string} methodName
     * @returns {Promise<unknown>}
     */
    static requirePromise(value, methodName) {
        assert(value && typeof value.then === 'function', `Consumer persistence method ${methodName}() must return a Promise.`);
        return value;
    }

    /**
     * @param {Storage} storage The storage to create the consumer for.
     * @param {string} indexName The name of the index to consume.
     * @param {string} identifier The unique name to identify this consumer.
     * @param {object} [initialState={}]
     * @param {number} [startFrom=0]
     * @param {{ persistence?: ConsumerPersistence, subscription?: StorageSubscription }} [options={}]
     */
    constructor(storage, indexName, identifier, initialState = {}, startFrom = 0, options = {}) {
        super({ objectMode: true });

        assert(storage instanceof Storage, 'Must provide a storage for the consumer.');
        assert(typeof indexName === 'string' && indexName !== '', 'Must specify an index name for the consumer.');
        assert(typeof identifier === 'string' && identifier !== '', 'Must specify an identifier name for the consumer.');

        this.initializeSubscription(storage, indexName, options.subscription);
        this.initializePersistence(storage, indexName, identifier, options.persistence);
        this.restoreState(initialState, startFrom);
        this.on('error', () => (this.handleDocument = false));
    }

    /**
     * @private
     * @param {Storage} storage
     * @param {string} indexName
     * @param {StorageSubscription} [subscription]
     */
    initializeSubscription(storage, indexName, subscription) {
        this.subscription = subscription || new StorageSubscription(storage, indexName);
        assert(this.subscription instanceof StorageSubscription, 'Consumer subscription must be a StorageSubscription instance.');

        this.storage = storage;
        this.index = this.subscription.index;
        this.indexName = indexName;
    }

    /**
     * @private
     * @param {Storage} storage
     * @param {string} indexName
     * @param {string} identifier
     * @param {ConsumerPersistence} [persistence]
     */
    initializePersistence(storage, indexName, identifier, persistence) {
        this.persistence = persistence || new FlatFilePersistence(storage.indexDirectory, storage.storageFile, indexName, identifier);
        assert(this.persistence instanceof ConsumerPersistence,
            'Consumer persistence must be an instance of ConsumerPersistence (or a subclass).');
    }

    /**
     * @private
     * @param {object} initialState
     * @param {number} startFrom
     */
    restoreState(initialState, startFrom) {
        ({ initialState, startFrom } = normalizeConsumerStateArgs(initialState, startFrom));
        this.position = startFrom;
        this.state = Object.freeze(initialState);
        this.persisting = null;
        this.consuming = false;
        this.ready = this.openPersistence(initialState, startFrom);
        this.ready.catch(() => undefined);
    }

    /**
     * @private
     * @param {object} initialState
     * @param {number} startFrom
     * @returns {Promise<void>}
     */
    openPersistence(initialState, startFrom) {
        return Consumer.requirePromise(this.persistence.open(initialState, startFrom), 'open')
            .then(() => Consumer.requirePromise(this.persistence.load(initialState, startFrom), 'load'))
            .then((restoredState) => {
                this.position = restoredState.position;
                this.state = Object.freeze(restoredState.state);
                this.emit('ready', this.position, this.state);
            });
    }

    /**
     * Update the state of this consumer transactionally with the position.
     * May only be called from within the document handling callback.
     *
     * @param {object|function(object):object} newState
     * @param {boolean} [persist=true] Set to false if this state update should not be persisted yet
     * @api
     */
    setState(newState, persist = true) {
        assert(this.handleDocument, 'Called setState outside of document handler!');

        if (typeof newState === 'function') {
            newState = newState(this.state);
        }
        this.state = Object.freeze(newState);
        this.doPersist = persist;
    }

    /**
     * @private
     * @param {number} position
     * @param {object} document
     */
    handleNewDocument(position, document) {
        this.handleDocument = true;
        this.once('data', () => (this.handleDocument = false));
        if (!this.push(document)) {
            this.stop();
        }
        this.position = position;
        if (this.doPersist) {
            this.persist();
        }
        this.emit('progress', this.position, this.state);
    }

    /**
     * @private
     * @param {Array|Generator} documents
     * @param {function(): void} done
     */
    handleCatchUpBatch(documents, done) {
        this.consumeDocuments(documents);
        this.emit('progress', this.position, this.state);
        this.once('persisted', done);
        this.persist();
    }

    /**
     * Persist current state of this consumer.
     *
     * @private
     */
    persist() {
        if (this.persisting) {
            return;
        }
        this.persisting = setImmediate(() => {
            let persistCall;
            try {
                persistCall = Consumer.requirePromise(this.persistence.persist(this.position, this.state, this.name), 'persist');
            } catch (error) {
                this.persisting = null;
                this.emit('error', error);
                return;
            }
            persistCall
                .then((persistedState) => {
                    this.persisting = null;
                    this.emit('persisted', persistedState);
                })
                .catch(() => {
                    this.persisting = null;
                });
        });
    }

    /**
     * Consume (push) a number of documents and update the position record.
     *
     * @private
     * @param {Array|Generator} documents
     */
    consumeDocuments(documents) {
        for (let documentEntry of documents) {
            const hasPosition = documentEntry &&
                typeof documentEntry === 'object' &&
                Object.prototype.hasOwnProperty.call(documentEntry, 'position') &&
                Object.prototype.hasOwnProperty.call(documentEntry, 'document');
            const document = hasPosition ? documentEntry.document : documentEntry;
            if (!this.push(document)) {
                this.stop();
                break;
            }
            this.position = hasPosition ? documentEntry.position : this.position + 1;
        }
    }

    /**
     * Start consuming documents.
     *
     * This will also catch up from the last position in case new documents were added.
     * @api
     */
    start() {
        if (this.isPaused()) {
            this.resume();
        }
        if (this.consuming) {
            return;
        }
        this.consuming = true;
        this.handleDocument = true;

        this.ready
            .then(() => {
                if (!this.consuming) {
                    return;
                }
                this.subscription.start({
                    getPosition: () => this.position,
                    onDocument: this.handleNewDocument.bind(this),
                    onCatchUpBatch: this.handleCatchUpBatch.bind(this),
                    onCaughtUp: () => {
                        this.handleDocument = false;
                        this.emit('caught-up');
                    }
                });
            })
            .catch((error) => {
                this.consuming = false;
                this.handleDocument = false;
                this.emit('error', error);
            });
    }

    /**
     * Stop consuming new documents. Consuming can be started again at any time.
     * @api
     */
    stop() {
        if (this.consuming) {
            this.pause();
        }
        this.subscription.stop();
        this.consuming = false;
        this.handleDocument = false;
    }

    /**
     * Reset this projection to restart processing all documents again.
     * NOTE: This will overwrite the current state of the projection and hence be destructive.
     * @param {object} [initialState={}]
     * @param {number} [startFrom=0]
     * @api
     */
    reset(initialState = {}, startFrom = 0) {
        ({ initialState, startFrom } = normalizeConsumerStateArgs(initialState, startFrom));
        const restart = this.consuming;
        this.stop();
        this.state = Object.freeze(initialState);
        this.position = startFrom;
        this.persist();
        if (restart) {
            this.start();
        }
    }

    // noinspection JSUnusedGlobalSymbols
    /**
     * Readable stream implementation.
     * @private
     */
    _read() {
        if (this.isPaused()) {
            return;
        }
        this.start();
    }
}

export default Consumer;
