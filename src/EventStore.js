import EventStream from './EventStream.js';
import JoinEventStream from './JoinEventStream.js';
import fs from 'fs';
import path from 'path';
import events from 'events';
import Storage, { ReadOnly as ReadOnlyStorage, LOCK_THROW, LOCK_RECLAIM } from './Storage.js';
import Index from './Index.js';
import Consumer from './Consumer.js';
import { assert, scanForFiles } from './util.js';
import { matches } from './metadataUtil.js';

const ExpectedVersion = {
    Any: -1,
    EmptyStream: 0
};

class OptimisticConcurrencyError extends Error {}

/**
 * A consistency token that captures the versions of a set of event-type streams at a point in time.
 * It is obtained from {@link EventStore#getConsistencyToken} and passed as the `expectedVersion`
 * argument to {@link EventStore#commit} to implement DCB-style (Dynamic Consistency Boundary) optimistic
 * concurrency: the commit only fails if events that match the original query actually appeared since the
 * token was created.
 *
 * @property {string[]} types     The event types whose streams were snapshotted.
 * @property {Object.<string,number>} versions  A map from each event type to the stream length at snapshot time.
 * @property {Matcher|null} matcher An optional additional matcher used to qualify the conflict check.
 *   A new event of a listed type only causes a conflict when this matcher also matches it.
 *   When `null`, any new event of a listed type causes a conflict.
 */
class ConsistencyToken {
    /**
     * @param {string[]} types
     * @param {Object.<string,number>} versions
     * @param {object|function|null} [matcher]
     */
    constructor(types, versions, matcher = null) {
        this.types = types;
        this.versions = versions;
        this.matcher = matcher;
    }
}

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
     * @param {boolean} [config.dcbMode] Enable Dynamic Consistency Boundary (DCB) mode. In this mode events are
     *   physically partitioned by their `payload.type` value and a lightweight stream index is automatically
     *   maintained for every distinct event type encountered. This makes {@link EventStore#getConsistencyToken}
     *   and token-based commits efficient without requiring a full store scan.
     * @param {object|function(string): object} [config.streamMetadata] A metadata object or a function `(streamName) => object`
     *   that is called whenever a new stream partition is created. The returned object is stored once in the partition
     *   file header and surfaced to `preCommit` / `preRead` hooks. Takes precedence only when
     *   `config.storageConfig.metadata` is not also set.
     */
    constructor(storeName = 'eventstore', config = {}) {
        super();
        if (typeof storeName !== 'string') {
            config = storeName;
            storeName = 'eventstore';
        }

        this.dcbMode = config.dcbMode || false;

        this.storageDirectory = path.resolve(config.storageDirectory || /* istanbul ignore next */ './data');
        let defaults = {
            dataDirectory: this.storageDirectory,
            indexDirectory: config.streamsDirectory || path.join(this.storageDirectory, 'streams'),
            partitioner: this.dcbMode
                ? (event) => event.payload?.type || ''
                : (event) => event.stream,
            readOnly: config.readOnly || false
        };
        const storageConfig = Object.assign(defaults, config.storageConfig);

        // Translate the high-level streamMetadata option into the storage-level metadata function,
        // but only when the caller has not already provided a lower-level storageConfig.metadata.
        if (config.streamMetadata !== undefined && storageConfig.metadata === undefined) {
            if (typeof config.streamMetadata === 'function') {
                storageConfig.metadata = config.streamMetadata;
            } else {
                storageConfig.metadata = (streamName) => config.streamMetadata[streamName] || {};
            }
        }

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
                        new ReadOnlyStorage(storeName, storageConfig)
                        : new Storage(storeName, storageConfig);
        this.storage.open();
        this.streams = Object.create(null);
        this.streams._all = { index: this.storage.index };

        this.scanStreams((err) => {
            if (err) {
                this.storage.close();
                throw err;
            }
            this.checkUnfinishedCommits();
            this.emit('ready');
        });
    }

    /**
     * Check if the last commit in the store was unfinished, which is the case if not all events of the commit have been written.
     * Torn writes are handled at the storage level, so this method only deals with unfinished commits.
     * @private
     */
    checkUnfinishedCommits() {
        let position = this.storage.length;
        let lastEvent;
        let truncateIndex = false;
        while (position > 0) {
            try {
                lastEvent = this.storage.read(position);
            } catch (e) {
                // A preRead hook may throw (e.g. access control). Stop repair check.
                return;
            }
            if (lastEvent !== false) break;
            truncateIndex = true;
            position--;
        }

        if (lastEvent && lastEvent.metadata.commitSize && lastEvent.metadata.commitVersion !== lastEvent.metadata.commitSize - 1) {
            this.emit('unfinished-commit', lastEvent);
            // commitId = global sequence number at which the commit started
            this.storage.truncate(lastEvent.metadata.commitId);
        } else if (truncateIndex) {
            // The index contained items that are not in the storage file; truncate everything
            // after `position`, the last sequence number that was successfully read.
            this.storage.truncate(position);
        }
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
        scanForFiles(this.streamsDirectory, /(stream-.*)\.index$/, this.registerStream.bind(this), callback);
        this.storage.on('index-created', this.registerStream.bind(this));
    }

    /**
     * @private
     * @param {string} name The full stream name, including the `stream-` prefix (and optional `.closed` suffix).
     */
    registerStream(name) {
        /* istanbul ignore if */
        if (!name.startsWith('stream-')) {
            return;
        }
        let streamName = name.slice(7);
        // Detect the `.closed` suffix — present both in the initial scan and when the directory
        // watcher emits 'index-created' after a writer renames the file (e.g. 'stream-foo-bar.closed').
        let isClosed = false;
        if (streamName.endsWith('.closed')) {
            streamName = streamName.slice(0, -7);
            isClosed = true;
        }
        if (streamName in this.streams) {
            if (isClosed && !this.streams[streamName].closed) {
                // The stream was renamed to .closed while this instance had it open.
                // The old ReadOnlyIndex was already closed via onRename, so we open the new one.
                const closedIndexName = 'stream-' + streamName + '.closed';
                const closedIndex = this.storage.openReadonlyIndex(closedIndexName);
                // deepcode ignore PrototypePollutionFunctionParams: streams is a Map
                this.streams[streamName] = { index: closedIndex, closed: true };
                this.emit('stream-closed', streamName);
            }
            return;
        }
        const index = isClosed
            ? this.storage.openReadonlyIndex(name)
            : this.storage.openIndex(name);
        // deepcode ignore PrototypePollutionFunctionParams: streams is a Map
        this.streams[streamName] = { index, closed: isClosed };
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
     * Override EventEmitter.on() to delegate 'preCommit' and 'preRead' event registrations
     * to the underlying storage, so that `eventstore.on('preCommit', handler)` works naturally.
     * All other events are handled by the default EventEmitter.
     *
     * @param {string} event
     * @param {function} listener
     * @returns {this}
     */
    on(event, listener) {
        if (event === 'preCommit' || event === 'preRead') {
            if (event === 'preCommit') {
                assert(!(this.storage instanceof ReadOnlyStorage), 'The storage was opened in read-only mode. Can not register a preCommit handler on it.');
            }
            this.storage.on(event, listener);
            return this;
        }
        return super.on(event, listener);
    }

    /**
     * @inheritDoc
     */
    addListener(event, listener) {
        return this.on(event, listener);
    }

    /**
     * Override EventEmitter.once() to delegate 'preCommit' and 'preRead' to the underlying storage.
     *
     * @param {string} event
     * @param {function} listener
     * @returns {this}
     */
    once(event, listener) {
        if (event === 'preCommit' || event === 'preRead') {
            if (event === 'preCommit') {
                assert(!(this.storage instanceof ReadOnlyStorage), 'The storage was opened in read-only mode. Can not register a preCommit handler on it.');
            }
            this.storage.once(event, listener);
            return this;
        }
        return super.once(event, listener);
    }

    /**
     * Override EventEmitter.off() / removeListener() to delegate 'preCommit' and 'preRead'
     * to the underlying storage.
     *
     * @param {string} event
     * @param {function} listener
     * @returns {this}
     */
    off(event, listener) {
        if (event === 'preCommit' || event === 'preRead') {
            this.storage.off(event, listener);
            return this;
        }
        return super.off(event, listener);
    }

    /**
     * @inheritDoc
     */
    removeListener(event, listener) {
        return this.off(event, listener);
    }

    /**
     * Convenience method to register a handler called before an event is committed to storage.
     * Equivalent to `eventstore.on('preCommit', hook)`.
     * The handler receives `(event, partitionMetadata)` and may throw to abort the write.
     * Multiple handlers can be registered; all run on every write in registration order.
     * The handler is invoked on every write, so its logic should be cheap, fast, and synchronous.
     *
     * @api
     * @param {function(object, object): void} hook A function receiving (event, partitionMetadata).
     * @throws {Error} If the storage was opened in read-only mode.
     */
    preCommit(hook) {
        this.on('preCommit', hook);
    }

    /**
     * Convenience method to register a handler called before an event is read from storage.
     * Equivalent to `eventstore.on('preRead', hook)`.
     * The handler receives `(position, partitionMetadata)` and may throw to abort the read.
     * Multiple handlers can be registered; all run on every read in registration order.
     * The handler is invoked on every read, so its logic should be cheap, fast, and synchronous.
     *
     * @api
     * @param {function(number, object): void} hook A function receiving (position, partitionMetadata).
     */
    preRead(hook) {
        this.on('preRead', hook);
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
     * @param {number|ConsistencyToken} [expectedVersion]
     * @param {object|function} [metadata]
     * @param {function} [callback]
     * @returns {{events: Array<object>, metadata: object, callback: function, expectedVersion: number|ConsistencyToken}}
     */
    static fixArgumentTypes(events, expectedVersion, metadata, callback) {
        if (!(events instanceof Array)) {
            events = [events];
        }
        if (typeof expectedVersion !== 'number' && !(expectedVersion instanceof ConsistencyToken)) {
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
     * Verify a {@link ConsistencyToken} against the current state of the store.
     * Throws an {@link OptimisticConcurrencyError} when one or more new events have appeared in
     * any of the token's event-type streams since the token was obtained and those events also
     * satisfy the token's optional matcher.
     *
     * @private
     * @param {ConsistencyToken} token
     * @throws {OptimisticConcurrencyError}
     */
    _checkConsistencyToken(token) {
        for (const type of token.types) {
            const currentVersion = this.streams[type]?.index.length ?? 0;
            const tokenVersion = token.versions[type] ?? 0;
            if (currentVersion === tokenVersion) {
                continue;
            }
            // New events of this type appeared since the token was captured.
            if (!token.matcher) {
                // No additional filter – any new event of the type is a conflict.
                throw new OptimisticConcurrencyError(
                    `Optimistic Concurrency error. New events of type "${type}" were committed since the consistency token was obtained.`
                );
            }
            // Apply the additional matcher: conflict only if a new event matches it.
            const typeStream = this.getEventStream(type, tokenVersion + 1, currentVersion);
            if (typeStream) {
                let stored;
                while ((stored = typeStream.next()) !== false) {
                    if (matches(stored, token.matcher)) {
                        throw new OptimisticConcurrencyError(
                            `Optimistic Concurrency error. A conflicting event of type "${type}" was committed since the consistency token was obtained.`
                        );
                    }
                }
            }
        }
    }

    /**
     * Commit a list of events for the given stream name, which is expected to be at the given version.
     * Note that the events committed may still appear in other streams too - the given stream name is only
     * relevant for optimistic concurrency checks with the given expected version.
     *
     * @api
     * @param {string} streamName The name of the stream to commit the events to.
     * @param {Array<object>|object} events The events to commit or a single event.
     * @param {number|ConsistencyToken} [expectedVersion] One of the `ExpectedVersion` constants, a positive
     *   stream version number, or a {@link ConsistencyToken} obtained from {@link EventStore#getConsistencyToken}.
     * @param {object} [metadata] The commit metadata to use as base. Useful for replication and adding storage metadata.
     * @param {function} [callback] A function that will be executed when all events have been committed.
     * @throws {OptimisticConcurrencyError} if the stream is not at the expected version, or if a
     *   {@link ConsistencyToken} was provided and conflicting events have been committed since it was obtained.
     */
    commit(streamName, events, expectedVersion = ExpectedVersion.Any, metadata = {}, callback = null) {
        assert(!(this.storage instanceof ReadOnlyStorage), 'The storage was opened in read-only mode. Can not commit to it.');
        assert(typeof streamName === 'string' && streamName !== '', 'Must specify a stream name for commit.');
        assert(typeof events !== 'undefined' && events !== null, 'No events specified for commit.');

        ({ events, expectedVersion, metadata, callback } = EventStore.fixArgumentTypes(events, expectedVersion, metadata, callback));

        // In DCB mode, ensure a lightweight type-stream index exists for every event type in this commit.
        if (this.dcbMode) {
            for (const event of events) {
                const type = event?.type;
                if (type && !(type in this.streams)) {
                    this.createEventStream(type, { payload: { type } }, false);
                }
            }
        }

        // Perform DCB-style concurrency check when a ConsistencyToken is provided.
        if (expectedVersion instanceof ConsistencyToken) {
            this._checkConsistencyToken(expectedVersion);
            expectedVersion = ExpectedVersion.Any;
        }

        if (!(streamName in this.streams)) {
            this.createEventStream(streamName, { stream: streamName }, false);
        }
        assert(!this.streams[streamName].closed, `Stream "${streamName}" is closed and cannot be written to.`);
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
     * Obtain a {@link ConsistencyToken} that snapshots the current length of each of the given event-type
     * streams.  The token can later be passed as the `expectedVersion` argument of {@link EventStore#commit}
     * to implement DCB-style (Dynamic Consistency Boundary) optimistic concurrency.
     *
     * **What a conflict means**
     * A conflict is detected when at least one new event has appeared in one of the named type streams
     * *and* that event also satisfies the optional `matcher` (if provided).  When no `matcher` is given
     * any new event of a listed type is treated as a conflict.
     *
     * **Stream availability**
     * A per-type stream index is created automatically the first time a type is requested:
     * - In **DCB mode** the index starts empty (`reindex=false`).  This is correct when the store was
     *   created in DCB mode from the beginning, because every event has been routed to the right type
     *   partition.  Do not use DCB mode with a pre-existing store that was not created in DCB mode.
     * - In **standard mode** the index is populated by scanning all existing documents (`reindex=true`).
     *   This one-time scan can be slow on a large store, but subsequent calls are O(1).
     *
     * @api
     * @param {string[]} types A non-empty array of event-type names to include in the token.
     * @param {object|function|null} [matcher] An optional additional matcher.  A new event of a listed
     *   type is only treated as a conflict when this matcher also matches the stored event document
     *   (i.e. the full `{ stream, payload, metadata }` object).
     * @returns {{ token: ConsistencyToken, stream: EventStream }} An object with:
     *   - `token` — the {@link ConsistencyToken} to pass to {@link EventStore#commit}.
     *   - `stream` — a read-only event stream covering all events of the given types, ordered by global
     *     sequence number.  Iterate it to build the transaction context before calling `commit`.
     * @throws {Error} if `types` is not a non-empty array.
     * @throws {Error} if the storage was opened in read-only mode and a type stream does not exist yet.
     */
    getConsistencyToken(types, matcher = null) {
        assert(Array.isArray(types) && types.length > 0, 'Must specify a non-empty array of event types for getConsistencyToken.');

        const versions = {};
        for (const type of types) {
            if (!(type in this.streams)) {
                assert(!(this.storage instanceof ReadOnlyStorage), `The storage was opened in read-only mode and the type stream "${type}" does not exist yet.`);
                // In DCB mode the type partition only holds events written while DCB mode was active,
                // so reindexing is not needed (and would produce wrong results for older data).
                const reindex = !this.dcbMode;
                this.createEventStream(type, { payload: { type } }, reindex);
            }
            versions[type] = this.streams[type].index.length;
        }

        const token = new ConsistencyToken(types, versions, matcher);

        const stream = types.length === 1
            ? this.getEventStream(types[0])
            : this.fromStreams('_dcb_' + types.join('_'), types);

        return { token, stream };
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
     * @param {boolean} [reindex=true] Whether to scan existing documents and populate the new index. Set to false when it is known that no existing documents can match the matcher (e.g. when creating a brand-new write stream).
     * @returns {EventStream} The EventStream with all existing events matching the matcher.
     * @throws {Error} If a stream with that name already exists.
     * @throws {Error} If the stream could not be created.
     */
    createEventStream(streamName, matcher, reindex = true) {
        assert(!(this.storage instanceof ReadOnlyStorage), 'The storage was opened in read-only mode. Can not create new stream on it.');
        assert(!(streamName in this.streams), 'Can not recreate stream!');

        const streamIndexName = 'stream-' + streamName;
        const index = this.storage.ensureIndex(streamIndexName, matcher, reindex);
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
        assert(!(this.storage instanceof ReadOnlyStorage), 'The storage was opened in read-only mode. Can not delete a stream on it.');

        if (!(streamName in this.streams)) {
            return;
        }
        this.streams[streamName].index.destroy();
        delete this.streams[streamName];
        this.emit('stream-deleted', streamName);
    }

    /**
     * Close a stream so that no new events are indexed into it.
     * The stream will still be readable, but any attempt to write to it will throw an error.
     * A closed stream is persisted by renaming its index file to include a `.closed` marker
     * (e.g. `stream-X.closed.index`), so it will be recognized as closed when the store is reopened.
     *
     * @api
     * @param {string} streamName The name of the stream to close.
     * @returns void
     * @throws {Error} If the storage is read-only.
     * @throws {Error} If the stream does not exist.
     * @throws {Error} If the stream is already closed.
     */
    closeEventStream(streamName) {
        assert(!(this.storage instanceof ReadOnlyStorage), 'The storage was opened in read-only mode. Can not close a stream on it.');
        assert(streamName in this.streams, `Stream "${streamName}" does not exist.`);
        assert(!this.streams[streamName].closed, `Stream "${streamName}" is already closed.`);

        const indexName = 'stream-' + streamName;
        const { index } = this.streams[streamName];

        // Flush and close the index before renaming the file
        index.close();

        // Rename the index file to mark it as closed (e.g. stream-foo.index -> stream-foo.closed.index)
        const closedFileName = index.fileName.replace(/\.index$/, '.closed.index');
        fs.renameSync(index.fileName, closedFileName);

        // Remove from secondary indexes so that new writes are no longer indexed into this stream
        this.storage.removeSecondaryIndex(indexName);

        // Reopen the renamed index for read access, outside the secondary indexes write path
        const closedIndexName = indexName + '.closed';
        const closedIndex = this.storage.openReadonlyIndex(closedIndexName);

        // deepcode ignore PrototypePollutionFunctionParams: streams is a Map
        this.streams[streamName] = { index: closedIndex, closed: true };
        this.emit('stream-closed', streamName);
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
        const regex = new RegExp(`^${this.storage.storageFile}\\.([^.]*\\..*)$`);
        const consumers = [];
        scanForFiles(consumersPath, regex, consumers.push.bind(consumers), /* istanbul ignore next */ (err) => {
            if (err) {
                return callback(err, []);
            }
            callback(null, consumers);
        });
    }
}

EventStore.Storage = Storage;
EventStore.Index = Index;

export default EventStore;
export { ExpectedVersion, OptimisticConcurrencyError, ConsistencyToken, LOCK_THROW, LOCK_RECLAIM };
