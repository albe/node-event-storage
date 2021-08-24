const EventStream = require('./EventStream');
const JoinEventStream = require('./JoinEventStream');
const fs = require('fs');
const path = require('path');
const events = require('events');
const Storage = require('./Storage');
const Consumer = require('./Consumer');
const { assert } = require('./util');

const ExpectedVersion = {
    Any: -1,
    EmptyStream: 0
};

class OptimisticConcurrencyError extends Error {}

/**
 * An event store optimized for working with many streams.
 * An event stream is implemented as an iterator over an index on the storage, therefore indexes need to be lightweight
 * and highly performant in read-only mode.
 */
class EventStore extends events.EventEmitter {

    /**
     * @param {string} [storeName] The name of the store which will be used as storage prefix. Default 'eventstore'.
     * @param {object} [config] An object with config options.
     * @param {string} [config.storageDirectory] The directory where the data should be stored. Default './data'.
     * @param {string} [config.streamsDirectory] The directory where the streams should be stored. Default '{storageDirectory}/streams'.
     * @param {object} [config.storageConfig] Additional config options given to the storage backend. See `Storage`.
     * @param {boolean} [config.readOnly] If the storage should be mounted in read-only mode.
     */
    constructor(storeName = 'eventstore', config = {}) {
        super();
        if (typeof storeName !== 'string') {
            config = storeName;
            storeName = 'eventstore';
        }

        this.storageDirectory = path.resolve(config.storageDirectory || /* istanbul ignore next */ './data');
        let defaults = {
            dataDirectory: this.storageDirectory,
            indexDirectory: config.streamsDirectory || path.join(this.storageDirectory, 'streams'),
            partitioner: (event) => event.stream,
            readOnly: config.readOnly || false
        };
        const storageConfig = Object.assign(defaults, config.storageConfig);
        this.initialize(storeName, storageConfig);
    }

    /**
     * @private
     * @param {string} storeName
     * @param {object} storageConfig
     */
    initialize(storeName, storageConfig) {
        this.streamsDirectory = path.resolve(storageConfig.indexDirectory);

        this.storeName = storeName;
        this.storage = (storageConfig.readOnly === true) ?
                        new Storage.ReadOnly(storeName, storageConfig)
                        : new Storage(storeName, storageConfig);
        this.storage.open();
        this.streams = Object.create(null);
        this.streams._all = { index: this.storage.index };

        this.scanStreams((err) => {
            if (err) {
                this.storage.close();
                throw err;
            }
            this.emit('ready');
        });
    }

    /**
     * Scan the streams directory for existing streams so they are ready for `getEventStream()`.
     *
     * @private
     * @param {function} callback A callback that will be called when all existing streams are found.
     */
    scanStreams(callback) {
        /* istanbul ignore if */
        if (typeof callback !== 'function') {
            callback = () => {};
        }
        // Find existing streams by scanning dir for filenames starting with 'stream-'
        fs.readdir(this.streamsDirectory, (err, files) => {
            if (err) {
                return callback(err);
            }
            let matches;
            for (let file of files) {
                if ((matches = file.match(/(stream-.*)\.index$/)) !== null) {
                    this.registerStream(matches[1]);
                }
            }
            callback();
        });
        this.storage.on('index-created', this.registerStream.bind(this));
    }

    /**
     * @private
     * @param {string} name The full stream name, including the `stream-` prefix.
     */
    registerStream(name) {
        /* istanbul ignore if */
        if (!name.startsWith('stream-')) {
            return;
        }
        const streamName = name.substr(7, name.length - 7);
        /* istanbul ignore if */
        if (streamName in this.streams) {
            return;
        }
        const index = this.storage.openIndex('stream-'+streamName);
        // deepcode ignore PrototypePollutionFunctionParams: streams is a Map
        this.streams[streamName] = { index };
        this.emit('stream-available', streamName);
    }

    /**
     * Close the event store and free up all resources.
     *
     * @api
     */
    close() {
        this.storage.close();
    }

    /**
     * Get the number of events stored.
     *
     * @api
     * @returns {number}
     */
    get length() {
        return this.storage.length;
    }

    /**
     * This method makes it so the last three arguments can be given either as:
     *  - expectedVersion, metadata, callback
     *  - expectedVersion, callback
     *  - metadata, callback
     *  - callback
     *
     * @private
     * @param {Array<object>|object} events
     * @param {number} [expectedVersion]
     * @param {object|function} [metadata]
     * @param {function} [callback]
     * @returns {{events: Array<object>, metadata: object, callback: function, expectedVersion: number}}
     */
    static fixArgumentTypes(events, expectedVersion, metadata, callback) {
        if (!(events instanceof Array)) {
            events = [events];
        }
        if (typeof expectedVersion !== 'number') {
            callback = metadata;
            metadata = expectedVersion;
            expectedVersion = ExpectedVersion.Any;
        }
        if (typeof metadata !== 'object') {
            callback = metadata;
            metadata = {};
        }
        if (typeof callback !== 'function') {
            callback = () => {};
        }
        return { events, expectedVersion, metadata, callback };
    }

    /**
     * Commit a list of events for the given stream name, which is expected to be at the given version.
     * Note that the events committed may still appear in other streams too - the given stream name is only
     * relevant for optimistic concurrency checks with the given expected version.
     *
     * @api
     * @param {string} streamName The name of the stream to commit the events to.
     * @param {Array<object>|object} events The events to commit or a single event.
     * @param {number} [expectedVersion] One of ExpectedVersion constants or a positive version number that the stream is supposed to be at before commit.
     * @param {object} [metadata] The commit metadata to use as base. Useful for replication and adding storage metadata.
     * @param {function} [callback] A function that will be executed when all events have been committed.
     * @throws {OptimisticConcurrencyError} if the stream is not at the expected version.
     */
    commit(streamName, events, expectedVersion = ExpectedVersion.Any, metadata = {}, callback = null) {
        assert(!(this.storage instanceof Storage.ReadOnly), 'The storage was opened in read-only mode. Can not commit to it.');
        assert(typeof streamName === 'string' && streamName !== '', 'Must specify a stream name for commit.');
        assert(typeof events !== 'undefined' && events !== null, 'No events specified for commit.');

        ({ events, expectedVersion, metadata, callback } = EventStore.fixArgumentTypes(events, expectedVersion, metadata, callback));

        if (!(streamName in this.streams)) {
            this.createEventStream(streamName, { stream: streamName });
        }
        let streamVersion = this.streams[streamName].index.length;
        if (expectedVersion !== ExpectedVersion.Any && streamVersion !== expectedVersion) {
            throw new OptimisticConcurrencyError(`Optimistic Concurrency error. Expected stream "${streamName}" at version ${expectedVersion} but is at version ${streamVersion}.`);
        }

        if (events.length > 1) {
            delete metadata.commitVersion;
        }

        const commitId = this.length;
        let commitVersion = 0;
        const commitSize = events.length;
        const committedAt = Date.now();
        const commit = Object.assign({
            commitId,
            committedAt
        }, metadata, {
            streamName,
            streamVersion,
            events: []
        });
        const commitCallback = () => {
            this.emit('commit', commit);
            callback(commit);
        };
        for (let event of events) {
            const eventMetadata = Object.assign({ commitId, committedAt, commitVersion, commitSize }, metadata, { streamVersion });
            const storedEvent = { stream: streamName, payload: event, metadata: eventMetadata };
            commitVersion++;
            streamVersion++;
            commit.events.push(event);
            this.storage.write(storedEvent, commitVersion !== events.length ? undefined : commitCallback);
        }
    }

    /**
     * @api
     * @param {string} streamName The name of the stream to get the version for.
     * @returns {number} The version that the given stream is at currently, or -1 if the stream does not exist.
     */
    getStreamVersion(streamName) {
        if (!(streamName in this.streams)) {
            return -1;
        }
        return this.streams[streamName].index.length;
    }

    /**
     * Get an event stream for the given stream name within the revision boundaries.
     *
     * @api
     * @param {string} streamName The name of the stream to get.
     * @param {number} [minRevision] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The 1-based maximum revision to include in the events (inclusive).
     * @returns {EventStream|boolean} The event stream or false if a stream with the name doesn't exist.
     */
    getEventStream(streamName, minRevision = 1, maxRevision = -1) {
        if (!(streamName in this.streams)) {
            return false;
        }
        return new EventStream(streamName, this, minRevision, maxRevision);
    }

    /**
     * Get a stream for all events within the revision boundaries.
     * This is the same as `getEventStream('_all', ...)`.
     *
     * @api
     * @param {number} [minRevision] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The 1-based maximum revision to include in the events (inclusive).
     * @returns {EventStream} The event stream.
     */
    getAllEvents(minRevision = 1, maxRevision = -1) {
        return this.getEventStream('_all', minRevision, maxRevision);
    }

    /**
     * Create a virtual event stream from existing streams by joining them.
     *
     * @param {string} streamName The (transient) name of the joined stream.
     * @param {Array<string>} streamNames An array of the stream names to join.
     * @param {number} [minRevision] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The 1-based maximum revision to include in the events (inclusive).
     * @returns {EventStream} The joined event stream.
     * @throws {Error} if any of the streams doesn't exist.
     */
    fromStreams(streamName, streamNames, minRevision = 1, maxRevision = -1) {
        assert(streamNames instanceof Array, 'Must specify an array of stream names.');

        for (let stream of streamNames) {
            assert(stream in this.streams, `Stream "${stream}" does not exist.`);
        }
        return new JoinEventStream(streamName, streamNames, this, minRevision, maxRevision);
    }

    /**
     * Get a stream for a category of streams. This will effectively return a joined stream of all streams that start
     * with the given `categoryName` followed by a dash.
     * If you frequently use this for a category consisting of a lot of streams (e.g. `users`), consider creating a
     * dedicated physical stream for the category:
     *
     *    `eventstore.createEventStream('users', e => e.stream.startsWith('users-'))`
     *
     * @api
     * @param {string} categoryName The name of the category to get a stream for. A category is a stream name prefix.
     * @param {number} [minRevision] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The 1-based maximum revision to include in the events (inclusive).
     * @returns {EventStream} The joined event stream for all streams of the given category.
     * @throws {Error} If no stream for this category exists.
     */
    getEventStreamForCategory(categoryName, minRevision = 1, maxRevision = -1) {
        if (categoryName in this.streams) {
            return this.getEventStream(categoryName, minRevision, maxRevision);
        }
        const categoryStreams = Object.keys(this.streams).filter(streamName => streamName.startsWith(categoryName + '-'));

        if (categoryStreams.length === 0) {
            throw new Error(`No streams for category '${categoryName}' exist.`);
        }
        return this.fromStreams(categoryName, categoryStreams, minRevision, maxRevision);
    }

    /**
     * Create a new stream with the given matcher.
     *
     * @api
     * @param {string} streamName The name of the stream to create.
     * @param {object|function(event)} matcher A matcher object, denoting the properties that need to match on an event a function that takes the event and returns true if the event should be added.
     * @returns {EventStream} The EventStream with all existing events matching the matcher.
     * @throws {Error} If a stream with that name already exists.
     * @throws {Error} If the stream could not be created.
     */
    createEventStream(streamName, matcher) {
        assert(!(this.storage instanceof Storage.ReadOnly), 'The storage was opened in read-only mode. Can not create new stream on it.');
        assert(!(streamName in this.streams), 'Can not recreate stream!');

        const streamIndexName = 'stream-' + streamName;
        const index = this.storage.ensureIndex(streamIndexName, matcher);
        assert(index !== null, `Error creating stream index ${streamName}.`);

        // deepcode ignore PrototypePollutionFunctionParams: streams is a Map
        this.streams[streamName] = { index, matcher };
        this.emit('stream-created', streamName);
        return new EventStream(streamName, this);
    }

    /**
     * Delete an event stream. Will do nothing if the stream with the name doesn't exist.
     *
     * Note that you can delete a write stream, but that will not delete the events written to it.
     * Also, on next write, that stream will be rebuilt from all existing events, which might take some time.
     *
     * @api
     * @param {string} streamName The name of the stream to delete.
     * @returns void
     */
    deleteEventStream(streamName) {
        assert(!(this.storage instanceof Storage.ReadOnly), 'The storage was opened in read-only mode. Can not delete a stream on it.');

        if (!(streamName in this.streams)) {
            return;
        }
        this.streams[streamName].index.destroy();
        delete this.streams[streamName];
        this.emit('stream-deleted', streamName);
    }

    /**
     * Get a durable consumer for the given stream that will keep receiving events from the last position.
     *
     * @param {string} streamName The name of the stream to consume.
     * @param {string} identifier The unique identifying name of this consumer.
     * @param {object} [initialState] The initial state of the consumer.
     * @param {number} [since] The stream revision to start consuming from.
     * @returns {Consumer} A durable consumer for the given stream.
     */
    getConsumer(streamName, identifier, initialState = {}, since = 0) {
        const consumer = new Consumer(this.storage, streamName === '_all' ? '_all' : 'stream-' + streamName, identifier, initialState, since);
        consumer.streamName = streamName;
        return consumer;
    }

    /**
     * Scan the existing consumers on this EventStore and asynchronously return a list of their names.
     * @param {function(error: Error, consumers: array)} callback A callback that will receive an error as first and the list of consumers as second argument.
     */
    scanConsumers(callback) {
        const consumersPath = path.join(this.storage.indexDirectory, 'consumers');
        if (!fs.existsSync(consumersPath)) {
            callback(null, []);
            return;
        }
        fs.readdir(consumersPath, (err, files) => {
            /* istanbul ignore if */
            if (err) {
                return callback(err, []);
            }
            let matches;
            const regex = new RegExp(`^${this.storage.storageFile}\.([^.]*\..*)$`);
            const consumers = [];
            for (let file of files) {
                if ((matches = file.match(regex)) !== null) {
                    consumers.push(matches[1]);
                }
            }
            callback(null, consumers);
        });
    }
}

module.exports = EventStore;
module.exports.ExpectedVersion = ExpectedVersion;
module.exports.OptimisticConcurrencyError = OptimisticConcurrencyError;
module.exports.LOCK_THROW = Storage.LOCK_THROW;
module.exports.LOCK_RECLAIM = Storage.LOCK_RECLAIM;
