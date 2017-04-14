const EventStream = require('./EventStream');
const JoinEventStream = require('./JoinEventStream');
const uuid = require('uuid').v4;
const fs = require('fs');
const path = require('path');
const EventEmitter = require('events');
const Storage = require('./Storage');
const Consumer = require('./Consumer');
const stream = require('stream');

const ExpectedVersion = {
    Any: -1,
    EmptyStream: 0
};

class OptimisticConcurrencyError extends Error {}

class EventUnwrapper extends stream.Transform {

    constructor() {
        super({ objectMode: true });
    }

    _transform(data, encoding, callback) {
        if (data.stream && data.payload) {
            this.push(data.payload);
        } else {
            this.push(data);
        }
        callback();
    }

}

/**
 * An event store optimized for working with many streams.
 * An event stream is implemented as an iterator over an index on the storage, therefore indexes need to be lightweight
 * and highly performant in read-only mode.
 */
class EventStore extends EventEmitter {

    /**
     * @param {string} [storeName] The name of the store which will be used as storage prefix. Default 'eventstore'.
     * @param {Object} [config] An object with config options.
     * @param {string} [config.storageDirectory] The directory where the data should be stored. Default './data'.
     * @param {string} [config.streamsDirectory] The directory where the streams should be stored. Default '{storageDirectory}/streams'.
     * @param {Object} [config.storageConfig] Additional config options given to the storage backend. See `Storage`.
     */
    constructor(storeName = 'eventstore', config = {}) {
        super();
        if (typeof storeName === 'object') {
            config = storeName;
            storeName = undefined;
        }

        this.storageDirectory = path.resolve(config.storageDirectory || './data');
        let defaults = {
            dataDirectory: this.storageDirectory,
            indexDirectory: config.streamsDirectory || path.join(this.storageDirectory, 'streams'),
            partitioner: (event) => event.stream
        };
        let storageConfig = Object.assign(defaults, config.storageConfig);
        this.streamsDirectory = path.resolve(storageConfig.indexDirectory);

        this.streams = {};
        this.storeName = storeName || 'eventstore';
        this.storage = new Storage(this.storeName, storageConfig);
        this.storage.open();
        this.streams['_all'] = { index: this.storage.index };

        this.scanStreams(() => this.emit('ready'));
    }

    /**
     * Scan the streams directory for existing streams so they are ready for `getEventStream()`.
     *
     * @private
     * @param {function} callback A callback that will be called when all existing streams are found.
     */
    scanStreams(callback) {
        // Find existing streams by scanning dir for filenames starting with 'stream-'
        fs.readdir(this.streamsDirectory, (err, files) => {
            if (err) {
                if (typeof callback === 'function') {
                    return callback(err);
                }
                throw err;
            }
            let matches;
            for (let file of files) {
                if ((matches = file.match(/(stream-(.*))\.index$/)) !== null) {
                    let streamName = matches[2];
                    let index = this.storage.ensureIndex(matches[1]);
                    this.streams[streamName] = { index };
                    this.emit('stream-available', streamName);
                }
            }
            if (typeof callback === 'function') return callback();
        });
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
        return this.storage.index.length;
    }

    /**
     * Commit a list of events for the given stream name, which is expected to be at the given version.
     * Note that the events committed may still appear in other streams too - the given stream name is only
     * relevant for optimistic concurrency checks with the given expected version.
     *
     * @api
     * @param {string} streamName The name of the stream to commit the events to.
     * @param {Array<Object>|Object} events The events to commit or a single event.
     * @param {number} [expectedVersion] One of ExpectedVersion constants or a positive version number that the stream is supposed to be at before commit.
     * @param {Object} [metadata] The commit metadata to use as base. Useful for replication and adding storage metadata.
     * @param {function} [callback] A function that will be executed when all events have been committed.
     * @throws {OptimisticConcurrencyError} if the stream is not at the expected version.
     */
    commit(streamName, events, expectedVersion = ExpectedVersion.Any, metadata, callback) {
        if (!streamName) {
            throw new Error('Must specify a stream name for commit.');
        }
        if (!events) {
            throw new Error('No events specified for commit.');
        }
        if (!(events instanceof Array)) {
            events = [events];
        }
        if (typeof expectedVersion === 'object') {
            callback = metadata;
            metadata = expectedVersion;
            expectedVersion = ExpectedVersion.Any;
        }
        if (typeof expectedVersion === 'function') {
            callback = expectedVersion;
            metadata = undefined;
            expectedVersion = ExpectedVersion.Any;
        } else if (typeof metadata === 'function') {
            callback = metadata;
            metadata = undefined;
        }

        if (!(streamName in this.streams)) {
            this.createEventStream(streamName, { stream: streamName });
        }
        let streamVersion = this.streams[streamName].index.length;
        if (expectedVersion !== ExpectedVersion.Any && streamVersion !== expectedVersion) {
            throw new OptimisticConcurrencyError(`Optimistic Concurrency error. Expected stream "${streamName}" at version ${expectedVersion} but is at version ${streamVersion}.`);
        }

        let commitId = uuid();
        let commitVersion = 0;
        let committedAt = Date.now();
        let commit = Object.assign({
            commitId,
            committedAt
        }, metadata, {
            streamName,
            streamVersion,
            events: []
        });
        let commitCallback = () => {
            this.emit('commit', commit);
            if (typeof callback === 'function') return callback(commit);
        };
        for (let event of events) {
            let eventMetadata = Object.assign({ commitId, committedAt }, metadata, { commitVersion, streamVersion });
            let storedEvent = { stream: streamName, payload: event, metadata: eventMetadata };
            commitVersion++;
            streamVersion++;
            commit.events.push(event);
            this.storage.write(storedEvent, commitVersion !== events.length ? undefined : commitCallback);
        }
    }

    /**
     * Get an event stream for the given stream name within the revision boundaries.
     *
     * @api
     * @param {string} streamName The name of the stream to get.
     * @param {number} [minRevision] The minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The maximum revision to include in the events (inclusive).
     * @returns {EventStream|boolean} The event stream or false if a stream with the name doesn't exist.
     */
    getEventStream(streamName, minRevision = 0, maxRevision = -1) {
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
     * @param {number} [minRevision] The minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The maximum revision to include in the events (inclusive).
     * @returns {EventStream} The event stream.
     */
    getAllEvents(minRevision = 0, maxRevision = -1) {
        return this.getEventStream('_all', minRevision, maxRevision);
    }

    /**
     * Create a new event stream from existing streams by joining them.
     *
     * @param {string} streamName The (transient) name of the joined stream.
     * @param {Array<string>} streamNames An array of the stream names to join.
     * @param {number} [minRevision] The minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The maximum revision to include in the events (inclusive).
     * @return {EventStream|boolean} The joined event stream or false if any of the streams doesn't exist.
     */
    fromStreams(streamName, streamNames, minRevision = 0, maxRevision = -1) {
        if (!(streamNames instanceof Array)) {
            throw new Error('Must specify an array of stream names.');
        }
        if (!streamNames.every(stream => stream in this.streams)) {
            return false;
        }
        return new JoinEventStream(streamName, streamNames, this, minRevision, maxRevision);
    }

    /**
     * Create a new stream with the given matcher.
     *
     * @api
     * @param {string} streamName The name of the stream to create.
     * @param {Object|function(event)} matcher A matcher object, denoting the properties that need to match on an event a function that takes the event and returns true if the event should be added.
     * @returns {EventStream} The EventStream with all existing events matching the matcher.
     * @throws {Error} If a stream with that name already exists.
     * @throws {Error} If the stream could not be created.
     */
    createEventStream(streamName, matcher) {
        if (streamName in this.streams) {
            throw new Error('Can not recreate stream!');
        }
        let streamIndexName = 'stream-' + streamName;
        let index = this.storage.ensureIndex(streamIndexName, matcher);
        if (!index) {
            throw new Error(`Error creating stream index ${streamName}.`);
        }
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
     * @returns {Consumer} A durable consumer for the given stream.
     */
    getConsumer(streamName, identifier) {
        let consumer = new Consumer(this.storage, 'stream-' + streamName, identifier);
        return consumer.pipe(new EventUnwrapper());
    }

    /**
     * Get all commits that happened since the given store revision.
     *
     * @param {number} [since] The event revision since when to return commits (inclusive). If since is within a commit, the full commit will be returned.
     * @returns {Generator<Object>} A generator of commit objects, each containing the commit metadata and the array of events.
     */
    *getCommits(since = 0) {
        let commit;
        let eventStream = this.getAllEvents(since);
        let storedEvent;
        while ((storedEvent = eventStream.next()) !== false) {
            let { metadata, stream, payload } = storedEvent;

            if (!commit && metadata.commitVersion > 0) {
                eventStream = this.getAllEvents(since - metadata.commitVersion);
                continue;
            }

            if (!commit || commit.commitId !== metadata.commitId) {
                if (commit) {
                    yield commit;
                }
                commit = {
                    commitId: metadata.commitId,
                    committedAt: metadata.committedAt,
                    streamName: stream,
                    streamVersion: metadata.streamVersion,
                    events: []
                };
            }
            commit.events.push(payload);
        }
        if (commit) {
            yield commit;
        }
    }
}

module.exports = EventStore;
module.exports.ExpectedVersion = ExpectedVersion;
module.exports.OptimisticConcurrencyError = OptimisticConcurrencyError;