const fs = require('fs');
const mkdirpSync = require('mkdirp').sync;
const path = require('path');
const EventEmitter = require('events');
const Index = require('./Index');

// node-event-storage V01
const HEADER_MAGIC = "nestor01";

const DEFAULT_READ_BUFFER_SIZE = 4 * 1024;
const DEFAULT_WRITE_BUFFER_SIZE = 16 * 1024;

const pad = (str, len, char = ' ') => {
    return len > str.length ? char.repeat(len - str.length) + str : str;
};

/**
 * An append-only storage with highly performant positional range scans.
 * It's highly optimized for an event-store and hence does not support compaction or data-rewrite, nor any querying
 */
class Storage extends EventEmitter {

    /**
     * Config options:
     *   - serializer: A serializer object with methods serialize(document) and deserialize(data).
     *   - dataDirectory: The path where the storage data should reside. Default '.'.
     *   - indexDirectory: The path where the indexes should be stored. Defaults to dataDirectory.
     *   - indexFile: The name of the primary index. Default '{storageFile}.index'.
     *   - readBufferSize: Size of the read buffer in bytes. Default 4096.
     *   - writeBufferSize: Size of the write buffer in bytes. Default 16384.
     *   - maxWriteBufferDocuments: How many documents to have in the write buffer at max. 0 means as much as possible. Default 0.
     *   - syncOnFlush: If fsync should be called on write buffer flush. Set this if you need strict durability. Defaults to false.
     *
     * @param {string} [storageFile] The name of the storage.
     * @param {Object} [config] An object with storage parameters.
     */
    constructor(storageFile, config = {}) {
        super();
        if (typeof storageFile === 'object') {
            config = storageFile;
            storageFile = undefined;
        }

        this.serializer = config.serializer || { serialize: JSON.stringify, deserialize: JSON.parse };
        this.storageFile = storageFile || 'storage';

        this.dataDirectory = config.dataDirectory || '.';
        if (!fs.existsSync(this.dataDirectory)) {
            mkdirpSync(this.dataDirectory);
        }

        this.indexDirectory = config.indexDirectory || this.dataDirectory;
        if (!fs.existsSync(this.indexDirectory)) {
            mkdirpSync(this.indexDirectory);
        }

        let indexFile = config.indexFile || (this.storageFile + '.index');
        this.index = new Index(path.join(this.indexDirectory, indexFile));
        this.secondaryIndexes = {};

        this.readBufferSize = config.readBufferSize || DEFAULT_READ_BUFFER_SIZE;
        this.writeBufferSize = config.writeBufferSize || DEFAULT_WRITE_BUFFER_SIZE;
        this.maxWriteBufferDocuments = config.maxWriteBufferDocuments || 0;
        this.syncOnFlush = !!config.syncOnFlush;
    }

    /**
     * Open the storage and indexes and create read and write buffers.
     * Will emit an 'opened' event if finished.
     *
     * @api
     * @returns {boolean}
     */
    open() {
        if (this.fd) {
            return true;
        }

        this.index.open();
        for (let indexName of Object.getOwnPropertyNames(this.secondaryIndexes)) {
            this.secondaryIndexes[indexName].index.open();
        }
        this.size = this.index.lastEntry.position + this.index.lastEntry.size;

        this.fd = fs.openSync(this.storageFile, 'a+');

        this.readBuffer = Buffer.allocUnsafe(10 + this.readBufferSize);
        this.readBufferPos = -1;

        this.writeBuffer = Buffer.allocUnsafe(this.writeBufferSize);
        this.writeBufferPos = 0;
        this.writeBufferDocuments = 0;
        this.flushCallbacks = [];

        let stat = fs.statSync(this.storageFile);
        if (this.size > stat.size) {
            console.log('Corrupt index: storage is smaller than index');
            // The index is corrupted, needs to be rebuilt
            this.emit('index-corrupted');
        } else if (this.size < stat.size) {
            console.log('Outdated index: index is smaller than storage', this.size, stat.size);
            // Index is not up to date, update from last position
            this.emit('index-outdated');
        }
        this.size = stat.size;

        this.emit('opened');
        return true;
    }

    /**
     * Close the storage and frees up all resources.
     * Will emit a 'closed' event when finished.
     *
     * @api
     * @returns void
     */
    close() {
        if (this.fd) {
            this.flush();
            fs.closeSync(this.fd);
            this.fd = undefined;
        }
        if (this.readBuffer) {
            this.readBuffer = undefined;
            this.readBufferPos = 0;
        }
        if (this.writeBuffer) {
            this.writeBuffer = undefined;
            this.writeBufferPos = 0;
            this.writeBufferDocuments = 0;
        }
        this.index.close();
        for (let indexName of Object.getOwnPropertyNames(this.secondaryIndexes)) {
            this.secondaryIndexes[indexName].index.close();
        }
        this.emit('closed');
    }

    /**
     * Flush the write buffer to disk.
     * This is a sync method and will invoke all previously registered flush callbacks.
     * Will emit a 'flush' event when done.
     *
     * @private
     * @returns {boolean}
     */
    flush() {
        if (!this.fd) {
            console.log('File is already closed!');
            return false;
        }
        if (this.writeBufferPos === 0) return false;
        fs.writeSync(this.fd, this.writeBuffer, 0, this.writeBufferPos);
        if (this.syncOnFlush) {
            fs.fsyncSync(this.fd);
        }
        this.writeBufferPos = 0;
        this.writeBufferDocuments = 0;
        this.flushCallbacks.forEach(callback => callback());
        this.flushCallbacks = [];
        this.emit('flush');
        return true;
    }

    /**
     * Add an index entry for the given document at the position and size.
     *
     * @private
     * @param {int} position The file offset where the document is stored.
     * @param {int} size The size of the stored document.
     * @param {Object} document The document to add to the index.
     * @param {function} [callback] The callback to call when the index is written to disk.
     * @returns {Index.Entry} The index entry item.
     */
    addIndex(position, size, document, callback) {
        let entry = new Index.Entry(this.index.length + 1, position, size);
        this.index.add(entry, (indexPosition) => {
            if (typeof callback === 'function') callback(indexPosition);
            this.emit('wrote', document, entry, indexPosition);
        });
        return entry;
    }

    /**
     * Write the given data with given data size to disk.
     *
     * @private
     * @param {string} data The data to write to disk.
     * @param {number} dataSize The size of the data.
     * @param {function} [callback] A callback that will be called when the data is flushed to disk.
     * @returns void
     */
    _write(data, dataSize, callback) {
        if (dataSize > this.writeBuffer.byteLength) {
            this.flush();
            console.log('Sync write!');
            fs.write(this.fd, data, callback);
        } else {
            if (this.writeBufferPos === 0) {
                process.nextTick(() => this.flush());
            }
            this.writeBufferPos += this.writeBuffer.write(data, this.writeBufferPos, dataSize, 'utf8');
            this.writeBufferDocuments++;
            if (typeof callback === 'function') this.flushCallbacks.push(callback);
            if (this.maxWriteBufferDocuments > 0 && this.writeBufferDocuments >= this.maxWriteBufferDocuments) {
                this.flush();
            }
        }
    }

    /**
     * @api
     * @param {Object} document The document to write to storage.
     * @param {function} [callback] A function that will be called when the document is written to disk.
     * @returns {Number} The 1-based document sequence number in the storage.
     */
    write(document, callback) {
        let data = this.serializer.serialize(document).toString();

        let dataSize = Buffer.byteLength(data, 'utf8');
        let dataToWrite = pad(dataSize.toString(), 10) + data + "\n";
        dataSize += 11;
        let position = this.size;

        /*if (this.index.lastEntry.position + this.index.lastEntry.size !== position) {
            this.emit('index-corrupted');
            throw new Error('Corrupted index, needs to be rebuilt!');
        }*/
        // TODO: The index should never be running ahead of the storage.
        //       Only if the write succeeded should the indexes be written.
        //       However, we need to increase the in-memory index position before
        //       the next write occurs in order to guarantee correct sequence numbering.
        this._write(dataToWrite, dataSize, callback);
        let indexEntry = this.addIndex(position, dataSize, document);
        for (let indexName of Object.getOwnPropertyNames(this.secondaryIndexes)) {
            if (this.matches(document, this.secondaryIndexes[indexName].matcher)) {
                this.secondaryIndexes[indexName].index.add(indexEntry);
            }
        }
        this.size += dataSize;
        return this.index.length;
    }

    /**
     * Fill the internal read buffer starting from the given position.
     *
     * @private
     * @param {number} [from] The file position to start filling the read buffer from.
     */
    fillBuffer(from = 0) {
        fs.readSync(this.fd, this.readBuffer, 0, this.readBuffer.byteLength, from);
        this.readBufferPos = from;
    }

    /**
     * @private
     * @param {number} position The file position to read from.
     * @param {number} [size] The expected byte size of the document at the given position.
     * @returns {Object} The document stored at the given position.
     * @throws {Error} if the storage entry at the given position is corrupted.
     * @throws {Error} if the document size at the given position does not match the provided size.
     * @throws {Error} if the document at the given position can not be deserialized.
     */
    readFrom(position, size) {
        let bufferPosition = position - this.readBufferPos;
        if (this.readBufferPos < 0 || bufferPosition < 0 || bufferPosition + 10 > this.readBuffer.byteLength) {
            this.fillBuffer(position);
            bufferPosition = 0;
        }
        let dataPosition = bufferPosition + 10;
        let dataLength = parseInt(this.readBuffer.toString('utf8', bufferPosition, dataPosition), 10);
        if (!dataLength || isNaN(dataLength)) {
            throw new Error('Error reading document size from ' + position + ', got ' + dataLength + '.');
        }
        if (size && dataLength + 11 !== size) {
            throw new Error('Invalid document size ' + dataLength + ' at position ' + position + ', expected ' + size + '.');
        }

        let data;
        if (dataLength + 10 > this.readBuffer.byteLength) {
            //console.log('sync read for large document size', dataLength, 'at position', position);
            let tempReadBuffer = Buffer.allocUnsafe(dataLength);
            fs.readSync(this.fd, tempReadBuffer, 0, dataLength, position + 10);
            data = tempReadBuffer.toString('utf8');
        } else {
            if (bufferPosition > 0 && dataPosition + dataLength > this.readBuffer.byteLength) {
                this.fillBuffer(position);
                dataPosition = 10;
            }

            data = this.readBuffer.toString('utf8', dataPosition, dataPosition + dataLength);
        }

        try {
            return this.serializer.deserialize(data);
        } catch (e) {
            console.log('Error parsing document:', this.readBufferPos, position, data, dataPosition, dataLength);
            throw e;
        }
    }

    /**
     * Read a single document from the given position, in the full index or in the provided index.
     *
     * @api
     * @param {number} number The 1-based document number (inside the given index) to read.
     * @param {Index} [index] The index to use for finding the document position.
     * @returns {Object} The document at the given position inside the index.
     */
    read(number, index) {
        index = index || this.index;

        let entry = index.get(number);
        if (entry === false) return false;

        return this.readFrom(entry.position, entry.size);
    }

    /**
     * Read a range of documents from the given position range, in the full index or in the provided index.
     * Returns a generator in order to reduce memory usage and be able to read lots of documents with little latency.
     *
     * @api
     * @param {number} from The 1-based document number (inclusive) to start reading from.
     * @param {number} until The 1-based document number (inclusive) to read until.
     * @param {Index} [index] The index to use for finding the documents in the range.
     * @returns {Generator} A generator that will read each document in the range one by one.
     */
    *readRange(from, until, index) {
        index = index || this.index;

        let entries = index.range(from, until);
        if (entries === false) {
            throw new Error('Range scan error.');
        }
        for (let entry of entries) {
            let document = this.readFrom(entry.position, entry.size);
            yield document;
        }
    }

    /**
     * @private
     * @param {Object} document The document to check against the matcher.
     * @param {Object|function} matcher An object of properties and their values that need to match in the object.
     * @returns {boolean} True if the document matches the matcher or false otherwise.
     */
    matches(document, matcher) {
        if (typeof matcher === 'undefined') return true;
        if (typeof document === 'undefined') return false;
        if (typeof matcher === 'function') return matcher(document);
        for (let prop of Object.getOwnPropertyNames(matcher)) {
            if (typeof matcher[prop] === 'object') {
                if (!this.matches(document[prop], matcher[prop])) {
                    return false;
                }
            } else {
                if (document[prop] !== matcher[prop]) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Ensure that an index with the given name and document matcher exists.
     * Will create the index if it doesn't exist, otherwise return the existing index.
     *
     * @api
     * @param {string} name The index name.
     * @param {Object|function} [matcher] An object that describes the document properties that need to match to add it this index or a function that receives a document and returns true if the document should be indexed.
     * @returns {Index} The index containing all documents that match the query.
     */
    ensureIndex(name, matcher) {
        if (!this.fd) {
            throw new Error('Storage is not open yet.');
        }
        if (name in this.secondaryIndexes) {
            return this.secondaryIndexes[name].index;
        }
        if (fs.existsSync(name + '.index')) {
            let metadata;
            if (matcher) {
                metadata = { metadata: { matcher: typeof matcher === 'object' ? JSON.stringify(matcher) : matcher.toString() } };
            }
            let index = new Index(this.EntryClass, name + '.index', metadata);
            if (typeof index.metadata.matcher === 'object') {
                matcher = index.metadata.matcher;
            } else {
                matcher = eval('(' + index.metadata.matcher + ')');
            }
            this.secondaryIndexes[name] = { index, matcher };
            index.open();
            return index;
        }

        let entries = this.index.all();
        if (entries === false) {
            throw new Error('Range scan error.');
        }
        if (!matcher) {
            throw new Error('Need to specify a matcher.');
        }
        let serializedMatcher = typeof matcher === 'object' ? JSON.stringify(matcher) : matcher.toString();
        let newIndex = new Index(this.EntryClass, name + '.index', { metadata: { matcher: serializedMatcher } });
        for (let entry of entries) {
            let document = this.readFrom(entry.position);
            if (this.matches(document, matcher)) {
                newIndex.add(entry);
            }
        }

        this.secondaryIndexes[name] = { index: newIndex, matcher };
        this.emit('index-created', name);
        return newIndex;
    }

    /**
     * Rebuild the full index.
     * TODO: Not implemented yet
     */
    rebuildIndex() {
    }

    repairIndex() {
    }
}

module.exports = Storage;