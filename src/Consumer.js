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
     */
    constructor(storage, indexName, identifier) {
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
        let consumerDirectory = path.join(this.storage.indexDirectory, 'consumers');
        if (!fs.existsSync(consumerDirectory)) {
            mkdirpSync(consumerDirectory);
        }

        this.fileName = path.join(consumerDirectory, this.storage.storageFile + '.' + indexName + '.' + identifier);
        try {
            this.position = fs.readFileSync(this.fileName);
        } catch (e) {
            this.position = 0;
        }

        this.consuming = false;
        this.handler = this.handleNewDocument.bind(this);
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

        if (this.position === position - 1) {
            if (!this.push(document)) {
                this.stop();
            }
            this.position = position;
            fs.writeFileSync(this.fileName, this.position);
        }
    }

    /**
     * Start consuming documents.
     *
     * This will also catch up from the last position in case new documents were added.
     * @api
     */
    start() {
        if (this.consuming) {
            return;
        }
        this.consuming = true;

        // Catch up to current index position
        const catchUpBatch = () => {
            setImmediate(() => {
                if (this.index.length <= this.position) {
                    this.storage.on('index-add', this.handler);
                    this.emit('caught-up');
                    return;
                }
                if (this.consuming === false) {
                    return;
                }

                let maxBatchPosition = Math.min(this.position + MAX_CATCHUP_BATCH + 1, this.index.length);
                let documents = this.storage.readRange(this.position + 1, maxBatchPosition, this.index);
                for (let document of documents) {
                    ++this.position;
                    if (!this.push(document)) {
                        this.stop();
                        break;
                    }
                }
                fs.writeFileSync(this.fileName, this.position);
                catchUpBatch();
            });
        };
        catchUpBatch();
    }

    /**
     * Stop consuming new documents. Consuming can be started again at any time.
     * @api
     */
    stop() {
        this.storage.removeListener('index-add', this.handler);
        this.consuming = false;
    }

    /**
     * Readable stream implementation.
     * @private
     */
    _read() {
        this.start();
    }
}

module.exports = Consumer;
