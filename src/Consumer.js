const stream = require('stream');
const fs = require('fs');
const path = require('path');
const mkdirpSync = require('mkdirp').sync;

const Storage = require('./Storage');
const MAX_CATCHUP_BATCH = 10;

/**
 * Implements an event-driven durable Consumer that provides at-least-once delivery semantics.
 */
class Consumer extends stream.Readable {

    /**
     * @param {Storage} storage The storage to create the consumer for.
     * @param {string} indexName The name of the index to consume.
     * @param {string} identifier The unique name to identify this consumer.
     * @param {number} [startFrom] The revision to start from within the index to consume.
     */
    constructor(storage, indexName, identifier, startFrom = 0) {
        super({ objectMode: true });

        if (!(storage instanceof Storage)) {
            throw new Error('Must provide a storage for the consumer.');
        }
        if (!indexName) {
            throw new Error('Must specify an index name for the consumer.');
        }
        if (!identifier) {
            throw new Error('Must specify an identifier name for the consumer.');
        }

        this.storage = storage;
        this.index = this.storage.openIndex(indexName);
        this.indexName = indexName;
        const consumerDirectory = path.join(this.storage.indexDirectory, 'consumers');
        if (!fs.existsSync(consumerDirectory)) {
            mkdirpSync(consumerDirectory);
        }

        this.fileName = path.join(consumerDirectory, this.storage.storageFile + '.' + indexName + '.' + identifier);
        try {
            const consumerData = fs.readFileSync(this.fileName);
            this.position = consumerData.readInt32LE(0);
            this.state = JSON.parse(consumerData.toString('utf8', 4));
        } catch (e) {
            this.position = startFrom;
            this.state = {};
        }

        this.persisting = null;
        this.consuming = false;
        this.handler = this.handleNewDocument.bind(this);
        this.on('error', () => (this.handleDocument = false));
    }

    /**
     * Update the state of this consumer transactionally with the position.
     * May only be called from within the document handling callback.
     *
     * @param {Object} newState
     * @api
     */
    setState(newState) {
        if (!this.handleDocument) {
            throw new Error('Called setState outside of document handler!');
        }
        this.state = Object.freeze(newState);
    }

    /**
     * Handler method that is supposed to be triggered for each new document in the storage.
     *
     * @private
     * @param {string} name The name of the index the document was added for.
     * @param {number} position The 1-based position inside the index that the document was added to.
     * @param {Object} document The document that was added.
     */
    handleNewDocument(name, position, document) {
        if (name !== this.indexName) {
            return;
        }

        if (this.position !== position - 1) {
            return;
        }

        this.handleDocument = true;
        this.once('data', () => (this.handleDocument = false));
        if (!this.push(document)) {
            this.stop();
        }
        this.position = position;
        this.persist();
    }

    /**
     * Persist current state of this consumer.
     * This will write the current position and state to the consumer storage file.
     *
     * @private
     */
    persist() {
        if (this.persisting) {
            return;
        }
        this.persisting = setImmediate(() => {
            const consumerState = JSON.stringify(this.state);
            const consumerData = Buffer.allocUnsafe(4 + consumerState.length);
            consumerData.writeInt32LE(this.position, 0);
            consumerData.write(consumerState, 4, consumerState.length, 'utf-8');
            fs.writeFileSync(this.fileName, consumerData);
            this.persisting = null;
            this.emit('persisted');
        });
    }

    /**
     * Check if this consumer has caught up. If so, register a handler for the stream and emit a 'caught-up' event.
     *
     * @private
     * @return {boolean} True if this consumer has caught up and can
     */
    checkCaughtUp() {
        if (this.index.length <= this.position) {
            this.handleDocument = false;
            this.storage.on('index-add', this.handler);
            this.emit('caught-up');
            return true;
        }
        return (this.consuming === false);
    }

    /**
     * Consume (push) a number of documents and update the position record.
     *
     * @private
     * @param {Array|Generator} documents The list or a stream of documents to consume
     */
    consumeDocuments(documents) {
        for (let document of documents) {
            if (!this.push(document)) {
                this.stop();
                break;
            }
            ++this.position;
        }
        fs.writeFileSync(this.fileName, this.position);
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

        // Catch up to current index position
        const catchUpBatch = () => {
            setImmediate(() => {
                if (this.checkCaughtUp()) {
                    return;
                }

                const maxBatchPosition = Math.min(this.position + MAX_CATCHUP_BATCH + 1, this.index.length);
                const documents = this.storage.readRange(this.position + 1, maxBatchPosition, this.index);
                this.consumeDocuments(documents);
                this.once('persisted', () => catchUpBatch());
                this.persist();
            });
        };
        catchUpBatch();
    }

    /**
     * Stop consuming new documents. Consuming can be started again at any time.
     * @api
     */
    stop() {
        if (this.consuming) {
            this.pause();
        }
        this.storage.removeListener('index-add', this.handler);
        this.consuming = false;
        this.handleDocument = false;
    }

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

module.exports = Consumer;
