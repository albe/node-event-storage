const EventStream = require('./EventStream');
const uuid = require('uuid').v4;
const fs = require('fs');
const EventEmitter = require('events');
const Storage = require('./Storage');

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
class EventStore extends EventEmitter {

    /**
     * Available config options:
     *  - storageDirectory: The directory where the data should be stored
     *  - storageConfig: Additional config options given to the storage backend. See `Storage`
     *
     * @param {string} [storeName] The name of the store which will be used as storage prefix. Default 'eventstore'.
     * @param {Object} [config] An object with config options.
     */
    constructor(storeName = 'eventstore', config = {}) {
        super();
        if (typeof storeName === 'object') {
            config = storeName;
            storeName = undefined;
        }
        this.streams = {};
        this.storeName = storeName || 'eventstore';
        this.storageDirectory = config.storageDirectory || './data';
        let storageConfig = Object.assign({ dataDirectory: this.storageDirectory, partitioner: (event) => event.stream }, config.storageConfig);
        this.storage = new Storage(this.storeName, storageConfig);
        this.storage.open();
        this.streams['_all'] = { index: this.storage.index };

        // Find existing streams by scanning dir for filenames starting with 'stream-'
        fs.readdir(this.storageDirectory, (err, files) => {
            if (err) throw err;
            let matches;
            for (let file of files) {
                if (matches = file.match(/(stream-(.*))\.index$/)) {
                    let streamName = matches[2];
                    let index = this.storage.ensureIndex(matches[1]);
                    this.streams[streamName] = { index };
                    this.emit('stream-available', streamName);
                }
            }
            this.emit('ready');
        });
    }

    /**
     * Close the event store and free up all resources.
     */
    close() {
        this.storage.close();
    }

    /**
     * Commit a list of events for the given stream name, which is expected to be at the given version.
     * Note that the events committed may still appear in other streams too - the given stream name is only
     * relevant for optimistic concurrency checks with the given expected version.
     *
     * @param {string} streamName The name of the stream to commit the events to.
     * @param {Array<Object>|Object} events The events to commit or a single event.
     * @param {number} [expectedVersion] One of ExpectedVersion constants or a positive version number that the stream is supposed to be at before commit.
     * @param {function} [callback] A function that will be executed when all events have been committed.
     * @throws {OptimisticConcurrencyError} if the stream is not at the expected version.
     */
    commit(streamName, events, expectedVersion = ExpectedVersion.Any, callback) {
        if (!streamName) {
            throw new Error('Must specify a stream name for commit.');
        }
        if (!events) {
            throw new Error('No events specified for commit.');
        }
        if (!(events instanceof Array)) {
            if (typeof events !== 'object') {
                throw new Error('Event must be an object.');
            }
            events = [events];
        }
        if (typeof expectedVersion === 'function' && typeof callback === 'undefined') {
            callback = expectedVersion;
            expectedVersion = ExpectedVersion.Any;
        }

        if (!(streamName in this.streams)) {
            this.createEventStream(streamName, { stream: streamName });
        }
        let streamVersion = this.streams[streamName].index.length;
        if (expectedVersion !== ExpectedVersion.Any) {
            if (streamVersion !== expectedVersion) {
                throw new OptimisticConcurrencyError(`Optimistic Concurrency error. Expected stream "${streamName}" at version ${expectedVersion} but is at version ${streamVersion}.`);
            }
        }

        let commitId = uuid();
        let commitVersion = 0;
        let committedAt = Date.now();
        let commit = {
            commitId,
            committedAt,
            streamName,
            streamVersion,
            events: []
        };
        for (let event of events) {
            let storedEvent = { stream: streamName, payload: event, metadata: { committedAt, commitVersion, commitId, streamVersion } };
            commitVersion++;
            streamVersion++;
            commit.events.push(event);
            this.storage.write(storedEvent, commitVersion !== events.length ? undefined : () => {
                this.emit('commit', commit);
                if (typeof callback === 'function') callback();
            });
        }
    }

    /**
     * Get an event stream for the given stream name within the revision boundaries.
     *
     * @param {string} streamName The name of the stream to get.
     * @param {number} [minRevision] The minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision] The maximum revision to include in the events (inclusive).
     * @returns {EventStream|boolean} The event stream or false if a stream with the name doesn't exist.
     */
    getEventStream(streamName, minRevision = 0, maxRevision = -1) {
        if (!(streamName in this.streams)) {
            return false;
        }
        let streamIndex = this.streams[streamName].index;
        return new EventStream(streamName, this.storage.readRange(minRevision + 1, maxRevision + 1, streamIndex));
    }

    /**
     * Get a stream for all events within the revision boundaries.
     * This is the same as `getEventStream('_all', ...)`.
     *
     * @param {number} minRevision
     * @param {number} maxRevision
     * @returns {EventStream} The event stream.
     */
    getAllEvents(minRevision = 0, maxRevision = -1) {
        return this.getEventStream('_all', minRevision, maxRevision);
    }

    /**
     * Create a new event stream from existing streams by joining them.
     *
     * @param {Array<string>} streamNames
     * @param {number} minRevision
     * @param {number} maxRevision
     * @return {EventStream}
     */
    fromStreams(streamNames, minRevision = 0, maxRevision = -1) {
        if (!(streamNames instanceof Array)) {
            throw new Error('Must specify an array of stream names.');
        }
        let joinStream = [];
        for (let streamName of streamNames) {
            if (!(streamName in this.streams)) {
                continue;
            }
            let streamIndex = this.streams[streamName].index;
            joinStream = joinStream.concat(streamIndex.all());
        }
        joinStream.sort((a, b) => b.number - a.number);
        if (minRevision > 0 || maxRevision > -1) {
            joinStream = joinStream.slice(minRevision, maxRevision > -1 ? maxRevision : undefined);
        }
        // TODO: Create iterator over joinStream
    }

    /**
     * Create a new stream with the given matcher.
     *
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
            throw new Error('Error creating stream index ' + streamName);
        }
        this.streams[streamName] = { index, matcher };
        this.emit('stream-created', streamName);
        return new EventStream(streamName, this.storage.readRange(1, 0, index));
    }

    /**
     * Delete an event stream. Will do nothing if the stream with the name doesn't exist.
     *
     * Note that you can delete a write stream, but that will not delete the events written to it.
     * Also, on next write, that stream will be rebuilt from all existing events, which might take some time.
     *
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
}

module.exports = EventStore;
module.exports.ExpectedVersion = ExpectedVersion;
