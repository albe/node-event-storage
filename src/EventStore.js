import EventStream from './EventStream.js';
import JoinEventStream from './JoinEventStream.js';
import fs from 'fs';
import path from 'path';
import events from 'events';
import Storage, { ReadOnly as ReadOnlyStorage, LOCK_THROW, LOCK_RECLAIM } from './Storage.js';
import Index from './Index.js';
import Consumer from './Consumer.js';
import { assert, getPropertyAtPath } from './utils/util.js';
import { ensureDirectory, resolvePath, scanForFiles } from './utils/fsUtil.js';
import { buildTypeMatcherFn } from './utils/metadataUtil.js';
import { fixCommitArgumentTypes, parseStreamFromIndexName, normalizePredicateRaw } from './utils/apiHelpers.js';
import { normalizeSelector } from "./utils/indexUtil.js";
import { isDcbQuery, compileDcbQuery } from "./utils/dcbUtil.js";

const ExpectedVersion = {
    Any: -1,
    EmptyStream: 0
};

/**
 * Default matcher property paths mirroring the Storage default, used for index optimization.
 */
const DEFAULT_MATCHER_PROPERTIES = ['stream', 'payload.type'];
const STREAM_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_]*(?:[\/:@~+=\-#.][A-Za-z0-9_]+)*$/;
const STORAGE_HOOK_EVENTS = new Set(['preCommit', 'preRead']);

class OptimisticConcurrencyError extends Error {}

/**
 * @typedef {string | SelectorNode[]} SelectorNode
 */

/**
 * @typedef {object} QueryItem
 * @property {string[]} [types] Event type names to match (OR within the group).
 * @property {string[]} [tags] Tag values to match (each tag is ANDed with the others and with types).
 */

/**
 * @typedef {object} DcbQuery
 * @property {QueryItem[]} items Non-empty array of query items combined with OR semantics at the top level.
 */

/**
 * An accept condition that captures the global event-log position at the time a {@link EventStore#query}
 * call was made.  Pass it as the `expectedVersion` argument to {@link EventStore#commit} to enforce
 * DCB-style (Dynamic Consistency Boundary) optimistic concurrency: the commit is rejected only when
 * one or more events that match the original query (selector + optional matcher) have been appended to
 * the store between the `query` call and the `commit` call.
 *
 * @property {SelectorNode[]|string} selector The normalized stream selector used in the query.
 * @property {string[]} types   Backwards-compatible alias for the leaf stream names of the selector.
 * @property {function(object, object): boolean|null} matcher An optional function `(payload, metadata) => boolean`
 *   used to narrow the conflict check.  When `null`, any new event of a listed type causes a conflict.
 * @property {number}   noneMatchAfter The global store length (total event count) at the time the query was made.
 */
class CommitCondition {
    /**
     * @param {SelectorNode[]|string} selector The (normalized) stream selector.
     * @param {(function(object, object): boolean)|object|null} [matcher]
     * @param {number} noneMatchAfter
     * @param {boolean} [raw=false]
     */
    constructor(selector, matcher = null, noneMatchAfter, raw = false) {
        this.selector = selector;
        this.matcher = matcher;
        this.raw = raw;
        this.noneMatchAfter = noneMatchAfter;
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
     * @param {string|function(object): string} [config.typeAccessor] Dot-notation path (e.g. `'type'`) or
     *   function `(event) => string` identifying the event type. Enables type-based queries via
     *   {@link EventStore#query} and ensures proper index routing for those queries.
     * @param {string|function(object): string[]} [config.tagsAccessor] Dot-notation path (e.g. `'tags'`) or
     *   function `(event) => string[]` extracting tag values from an event payload on write. On each
     *   {@link EventStore#commit}, one stream per tag is created under `tags/{tag}`. Enables tag-scoped
     *   {@link DcbQuery} items via {@link EventStore#query}. Tag values are recommended to use `/` as a
     *   hierarchy separator (e.g. `'course/jdsj4'` → stream `tags/course/jdsj4`).
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

        if (typeof config.typeAccessor === 'string' && config.typeAccessor) {
            const accessorPath = config.typeAccessor;
            this.typeAccessor = (event) => getPropertyAtPath(event, accessorPath);
            this.typeMatcherFn = buildTypeMatcherFn(accessorPath);
        } else {
            this.typeAccessor = typeof config.typeAccessor === 'function' ? config.typeAccessor : null;
            this.typeMatcherFn = null;
        }

        if (typeof config.tagsAccessor === 'string' && config.tagsAccessor) {
            const accessorPath = config.tagsAccessor;
            this.tagsAccessor = (event) => getPropertyAtPath(event, accessorPath) ?? [];
            this.tagsMatcherFn = buildTypeMatcherFn(accessorPath);
        } else {
            this.tagsAccessor = typeof config.tagsAccessor === 'function' ? config.tagsAccessor : null;
            this.tagsMatcherFn = null;
        }

        this.storageDirectory = resolvePath(config.storageDirectory || /* istanbul ignore next */ './data');
        let defaults = {
            dataDirectory: this.storageDirectory,
            indexDirectory: config.streamsDirectory || path.join(this.storageDirectory, 'streams'),
            partitioner: (event) => event.stream,
            readOnly: config.readOnly || false
        };
        const storageConfig = Object.assign(defaults, config.storageConfig);

        // When typeAccessor/tagsAccessor is a string path, ensure the corresponding full document
        // path (payload.<path>) is in matcherProperties so the IndexMatcher discriminant table
        // can route those stream lookups in O(1) on every write.
        if (this.typeMatcherFn) {
            const fullPath = `payload.${config.typeAccessor}`;
            const currentProps = storageConfig.matcherProperties || DEFAULT_MATCHER_PROPERTIES;
            if (!currentProps.includes(fullPath)) {
                storageConfig.matcherProperties = [...currentProps, fullPath];
            }
        }
        if (this.tagsMatcherFn) {
            const fullPath = `payload.${config.tagsAccessor}`;
            const currentProps = storageConfig.matcherProperties || DEFAULT_MATCHER_PROPERTIES;
            if (!currentProps.includes(fullPath)) {
                storageConfig.matcherProperties = [...currentProps, fullPath];
            }
        }

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
        this.storageConfig = storageConfig;
        this.streamsDirectory = resolvePath(storageConfig.indexDirectory);
        this.storeName = storeName;
        this.consumers = new Map();

        const storage = storageConfig.readOnly === true
            ? new ReadOnlyStorage(storeName, storageConfig)
            : new Storage(storeName, storageConfig);

        this.mountStorage(storage, () => {
            if (storageConfig.readOnly !== true) {
                this.checkUnfinishedCommits();
            }
            this.emit('ready');
        });
    }

    /**
     * Wire up a storage instance: reset the streams map, attach the index-created listener,
     * register a one-shot opened handler, then open the storage.
     * @private
     * @param {ReadableStorage} storage
     * @param {function} [onOpened] Called once when the storage is opened.
     */
    mountStorage(storage, onOpened) {
        this.storage = storage;
        this.streams = Object.create(null);
        this.streams._all = { index: this.storage.index };
        this.storage.on('index-created', this.registerStream.bind(this));
        this.storage.open(onOpened);
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
     * @private
     * @param {string} name The full stream name, including the `stream-` prefix (and optional `.closed` suffix).
     */
    registerStream(name) {
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
     * Stops all registered consumers before closing storage.
     *
     * @api
     */
    close() {
        for (const consumer of this.consumers.values()) {
            consumer.stop();
        }
        this.consumers.clear();
        this.storage.close();
    }

    /**
     * Flush all pending writes, then re-open the store in read-only mode.
     * Any registered consumers are stopped. The `callback` is invoked once the
     * new read-only storage has finished opening.
     *
     * Does not re-emit `'ready'` — use the callback to react to the transition.
     *
     * @api
     * @param {function} [callback] Called when the store is ready in read-only mode.
     */
    makeReadOnly(callback) {
        if (this.storage instanceof ReadOnlyStorage) {
            callback?.();
            return
        }
        for (const consumer of this.consumers.values()) {
            consumer.stop();
        }
        this.consumers.clear();

        this.storage.flush();
        this.storage.close();

        const readOnlyConfig = Object.assign({}, this.storageConfig, { readOnly: true });
        this.storageConfig = readOnlyConfig;

        this.mountStorage(new ReadOnlyStorage(this.storeName, readOnlyConfig), callback);
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
        if (this.isStorageHookEvent(event)) {
            this.delegateStorageHookEvent('on', event, listener);
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
        if (this.isStorageHookEvent(event)) {
            this.delegateStorageHookEvent('once', event, listener);
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
        if (this.isStorageHookEvent(event)) {
            this.storage.off(event, listener);
            return this;
        }
        return super.off(event, listener);
    }

    isStorageHookEvent(event) {
        return STORAGE_HOOK_EVENTS.has(event);
    }

    delegateStorageHookEvent(method, event, listener) {
        if (event === 'preCommit') {
            assert(!(this.storage instanceof ReadOnlyStorage), 'The storage was opened in read-only mode. Can not register a preCommit handler on it.');
        }
        this.storage[method](event, listener);
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
     * Check a {@link CommitCondition} against the current state of the store.
     * Iterates a join stream over all condition type streams starting from
     * `condition.noneMatchAfter` (the global position captured at query time), and throws an
     * {@link OptimisticConcurrencyError} when a new event of a listed type satisfies
     * `condition.matcher(payload, metadata)` (or any such event when no matcher is provided).
     *
     * @private
     * @param {CommitCondition} condition
     * @throws {OptimisticConcurrencyError}
     */
    checkCondition(condition) {
        if (this.storage.length <= condition.noneMatchAfter) return; // no new events since condition was obtained

        const selector = condition.selector || condition.types;

        // Only events after condition.noneMatchAfter can be conflicts.
        // Pass the original matcher and raw flag so the stream filters at the source.
        const stream = this.fromStreams(
            '_check_' + Date.now(),
            selector,
            condition.noneMatchAfter + 1,
            -1,
            condition.matcher,
            condition.raw
        );

        assert(stream.next() === false, `Optimistic Concurrency error. A conflicting event was committed since the condition was obtained.`, OptimisticConcurrencyError);
    }

    /**
     * Ensure a dedicated type stream exists for each event's type, creating it if needed.
     * Must be called before the entity stream is created to guarantee correct index routing.
     *
     * @private
     * @param {Array<object>} events The events to process.
     */
    ensureTypeStreams(events) {
        if (!this.typeAccessor) return;
        for (const event of events) {
            const type = this.resolveValidatedTypeStreamName(event);
            if (type && !(type in this.streams)) {
                const matcher = this.typeMatcherFn
                    ? this.typeMatcherFn(type)
                    : (doc) => this.typeAccessor(doc.payload) === type;
                this.createEventStream(type, matcher, false);
            }
        }
    }

    /**
     * Ensure a dedicated tag stream exists for each tag on each event, creating it if needed.
     * Must be called before the entity stream is created (same ordering requirement as ensureTypeStreams).
     *
     * @private
     * @param {Array<object>} events The events to process.
     */
    ensureTagStreams(events) {
        if (!this.tagsAccessor) return;
        const tagsAccessor = this.tagsAccessor;
        const tagsMatcherFn = this.tagsMatcherFn;
        for (const event of events) {
            const tags = tagsAccessor(event);
            if (!Array.isArray(tags)) continue;
            for (const tag of tags) {
                if (typeof tag !== 'string' || !tag) continue;
                const streamName = 'tags/' + tag;
                if (!(streamName in this.streams)) {
                    // When tagsMatcherFn is set (string path form), build an object matcher so
                    // IndexMatcher can classify it into the discriminant table for O(1) routing.
                    // Fall back to a function matcher when tagsAccessor is a function.
                    const matcher = tagsMatcherFn
                        ? tagsMatcherFn(tag)
                        : (doc) => {
                            const docTags = tagsAccessor(doc.payload);
                            return Array.isArray(docTags) && docTags.includes(tag);
                        };
                    this.createEventStream(streamName, matcher, false);
                }
            }
        }
    }

    /**
     * @private
     * @param {object} event
     * @returns {string|null}
     */
    resolveValidatedTypeStreamName(event) {
        const type = this.typeAccessor(event);
        if (type === undefined || type === null || type === '') {
            return null;
        }
        assert(typeof type === 'string', 'typeAccessor must return a string.');
        assert(STREAM_NAME_PATTERN.test(type), `typeAccessor must return a valid stream name. Got: "${type}"`);
        return type;
    }

    /**
     * Commit a list of events for the given stream name, which is expected to be at the given version.
     * Note that the events committed may still appear in other streams too - the given stream name is only
     * relevant for optimistic concurrency checks with the given expected version.
     *
     * @api
     * @param {string} streamName The name of the stream to commit the events to.
     * @param {Array<object>|object} events The events to commit or a single event.
     * @param {number|CommitCondition} [expectedVersion] One of the `ExpectedVersion` constants, a positive
     *   stream version number, or a {@link CommitCondition} obtained from {@link EventStore#query}.
     * @param {object} [metadata] The commit metadata to use as base. Useful for replication and adding storage metadata.
     * @param {function} [callback] A function that will be executed when all events have been committed.
     * @throws {OptimisticConcurrencyError} if the stream is not at the expected version, or if a
     *   {@link CommitCondition} was provided and conflicting events have been committed since it was obtained.
     */
    commit(streamName, events, expectedVersion = ExpectedVersion.Any, metadata = {}, callback = null) {
        assert(!(this.storage instanceof ReadOnlyStorage), 'The storage was opened in read-only mode. Can not commit to it.');
        assert(typeof streamName === 'string' && streamName !== '', 'Must specify a stream name for commit.');
        assert(typeof events !== 'undefined' && events !== null, 'No events specified for commit.');

        ({ events, expectedVersion, metadata, callback } = fixCommitArgumentTypes(
            events,
            expectedVersion,
            metadata,
            callback,
            ExpectedVersion.Any,
            CommitCondition
        ));

        // Perform DCB-style concurrency check when a CommitCondition is provided.
        if (expectedVersion instanceof CommitCondition) {
            this.checkCondition(expectedVersion);
            expectedVersion = ExpectedVersion.Any;
        }

        // When typeAccessor/tagsAccessor is configured, ensure dedicated streams exist for each
        // event before the entity stream write so those indexes are never incomplete.
        this.ensureTypeStreams(events);
        this.ensureTagStreams(events);

        if (!(streamName in this.streams)) {
            this.createEventStream(streamName, { stream: streamName }, false);
        }
        assert(!this.streams[streamName].closed, `Stream "${streamName}" is closed and cannot be written to.`);
        let streamVersion = this.streams[streamName].index.length;
        assert(expectedVersion === ExpectedVersion.Any || streamVersion === expectedVersion,
            `Optimistic Concurrency error. Expected stream "${streamName}" at version ${expectedVersion} but is at version ${streamVersion}.`,
            OptimisticConcurrencyError
        );

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
     * Query the event store for events matching a set of event types and an optional filter function.
     * Returns a pre-filtered event stream and a {@link CommitCondition} that can be passed to
     * {@link EventStore#commit} to enforce optimistic concurrency.
     *
     * A conflict occurs when at least one event appended between the `query` call and the `commit` call
     * belongs to one of the listed types and (when `matcher` is provided) also satisfies
     * `matcher(payload, metadata)`.  Events written before the `query` call are never treated as conflicts.
     *
     * Missing stream references are treated as empty sets. In OR groups they have no effect;
     * in AND groups they collapse that branch to an empty result.
     *
     * @api
     * @param {SelectorNode[]|object} selectorOrDcbQuery A non-empty selector array, or a {@link DcbQuery} object
     *   with an `items` property (compiled automatically to selector algebra).
     * @param {function|object|null} [matcher] Optional matcher used for stream pre-filtering.
     *   In object mode, function predicates receive `(payload, metadata)`.
     * @param {number} [minRevision=1] The 1-based minimum global revision to include in the returned stream (inclusive).
     * @param {boolean} [raw=false] If true, return NDJSON buffers from the query stream.
     * @returns {{ condition: CommitCondition, stream: EventStream }} An object with:
     *   - `condition` — the {@link CommitCondition} to pass to {@link EventStore#commit}.
     *   - `stream` — a read-only event stream containing all matching events.
     *   Nested selector example (items=OR, per-item=AND, inner=OR):
     *   `[[courseTag, ['CourseCreated', 'CourseCapacityChanged']], [studentTag, ['StudentCreated']]]`
     * @throws {Error} if `selector` is not a non-empty array.
     */
    query(selectorOrDcbQuery, matcher = null, minRevision = 1, raw = false) {
        let selector = selectorOrDcbQuery;
        if (isDcbQuery(selectorOrDcbQuery)) {
            selector = compileDcbQuery(
                selectorOrDcbQuery,
                (type) => this.resolveTypeStream(type),
                (tag) => this.resolveTagStream(tag)
            );
        }
        assert(Array.isArray(selector) && selector.length > 0, 'Must specify a non-empty array of stream selectors for query.');
        // Normalize the selector once here; both the condition and the stream use
        // the normalized form.  Wrap a string result (single-stream reduction) in
        // an array so fromStreams always receives an array.
        const normalized = normalizeSelector(selector);
        const querySelector = Array.isArray(normalized) ? normalized : [normalized];
        const condition = new CommitCondition(querySelector, matcher, this.storage.length, raw);
        const stream = this.fromStreams('_query_' + Date.now(), querySelector, minRevision, -1, matcher, raw);
        return { stream, condition };
    }

    /**
     * @private
     * @param {string} type
     * @returns {string} Stream name for the given event type.
     * @throws {Error} When typeAccessor is not configured.
     */
    resolveTypeStream(type) {
        assert(this.typeAccessor !== null, 'DcbQuery references "types" but typeAccessor not configured.');
        return type;
    }

    /**
     * @private
     * @param {string} tag
     * @returns {string} Stream name for the given tag value.
     * @throws {Error} When tagsAccessor is not configured.
     */
    resolveTagStream(tag) {
        assert(this.tagsAccessor !== null, 'DcbQuery references "tags" but tagsAccessor not configured.');
        return 'tags/' + tag;
    }

    /**
     * Get an event stream for the given stream name within the revision boundaries.
     *
     * @api
     * @param {string} streamName The name of the stream to get.
     * @param {number} [minRevision=1] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision=-1] The 1-based maximum revision to include in the events (inclusive).
     * @param {function|object|null} [predicate] Optional matcher (see {@link EventStream}).
     * @param {boolean} [raw=false] If true, return NDJSON buffers.
     * @returns {EventStream|boolean} The event stream or false if a stream with the name doesn't exist.
     */
    getEventStream(streamName, minRevision = 1, maxRevision = -1, predicate = null, raw = false) {
        ({ predicate, raw } = normalizePredicateRaw(predicate, raw));
        if (!(streamName in this.streams)) {
            return false;
        }
        return new EventStream(streamName, this, minRevision, maxRevision, predicate, raw);
    }

    /**
     * Get a stream for all events within the revision boundaries.
     * This is the same as `getEventStream('_all', ...)`.
     *
     * @api
     * @param {number} [minRevision=1] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision=-1] The 1-based maximum revision to include in the events (inclusive).
     * @param {function|object|null} [predicate] Optional matcher (see {@link EventStream}).
     * @param {boolean} [raw=false] If true, return NDJSON buffers.
     * @returns {EventStream} The event stream.
     */
    getAllEvents(minRevision = 1, maxRevision = -1, predicate = null, raw = false) {
        ({ predicate, raw } = normalizePredicateRaw(predicate, raw));
        return this.getEventStream('_all', minRevision, maxRevision, predicate, raw);
    }

    /**
     * Create a virtual event stream from existing streams.
     *
     * Uses selector algebra with alternating operator levels (depth 0 = OR, depth 1 = AND, ...).
     *
     * @param {string} streamName The (transient) name of the joined stream.
     * @param {Array<string|Array<string>>} streamNames Stream selector input.
     * @param {number} [minRevision=1] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision=-1] The 1-based maximum revision to include in the events (inclusive).
     * @param {function|object|null} [predicate] Optional matcher (see {@link EventStream}).
     * @param {boolean} [raw=false] If true, return NDJSON buffers.
     * @returns {EventStream|JoinEventStream}
     * @throws {Error} if any selected stream doesn't exist.
     */
    fromStreams(streamName, streamNames, minRevision = 1, maxRevision = -1, predicate = null, raw = false) {
        ({ predicate, raw } = normalizePredicateRaw(predicate, raw));
        assert(streamNames instanceof Array, 'Must specify an array of stream names.');

        return new JoinEventStream(streamName, streamNames, this, minRevision, maxRevision, predicate, raw);
    }

    /**
     * Get a stream for a category of streams. This will effectively return a joined stream of all streams that start
     * with the given `categoryName` followed by a dash (flat layout, e.g. `users-123`) or a slash (hierarchical
     * layout, e.g. `users/123`).
     * If you frequently use this for a category consisting of a lot of streams (e.g. `users`), consider creating a
     * dedicated physical stream for the category:
     *
     *    `eventstore.createEventStream('users', e => e.stream.startsWith('users-') || e.stream.startsWith('users/'))`
     *
     * @api
     * @param {string} categoryName The name of the category to get a stream for. A category is a stream name prefix.
     * @param {number} [minRevision=1] The 1-based minimum revision to include in the events (inclusive).
     * @param {number} [maxRevision=-1] The 1-based maximum revision to include in the events (inclusive).
     * @param {function|object|null} [predicate] Optional matcher (see {@link EventStream}).
     * @param {boolean} [raw=false] If true, return NDJSON buffers.
     * @returns {EventStream} The joined event stream for all streams of the given category.
     * @throws {Error} If no stream for this category exists.
     */
    getEventStreamForCategory(categoryName, minRevision = 1, maxRevision = -1, predicate = null, raw = false) {
        ({ predicate, raw } = normalizePredicateRaw(predicate, raw));
        if (categoryName in this.streams) {
            return this.getEventStream(categoryName, minRevision, maxRevision, predicate, raw);
        }
        const categoryStreams = Object.keys(this.streams).filter(streamName =>
            streamName.startsWith(categoryName + '-') ||
            streamName.startsWith(categoryName + '/')
        );

        assert(categoryStreams.length > 0, `No streams for category '${categoryName}' exist.`);

        return this.fromStreams(categoryName, categoryStreams, minRevision, maxRevision, predicate, raw);
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
        if (streamName.includes('/')) {
            const subDir = path.join(this.streamsDirectory, this.storeName + '.stream-' + path.dirname(streamName));
            ensureDirectory(subDir);
        }
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
     * Get a durable consumer for the given stream, or look up an existing consumer by identifier.
     *
     * When called with a single argument, returns the running consumer registered under that
     * identifier, or `null` if none is found — useful for read endpoints that need the live
     * in-memory instance without creating a new one.
     *
     * When called with two or more arguments, creates (or re-uses) a Consumer for the given
     * stream and identifier, registers it in `this.consumers`, and returns it.
     *
     * @param {string} streamNameOrIdentifier The stream name, or the consumer identifier when used as a registry lookup.
     * @param {string} [identifier] The unique identifying name of this consumer. Omit for registry-only lookup.
     * @param {object} [initialState] The initial state of the consumer.
     * @param {number} [since] The stream revision to start consuming from.
     * @returns {Consumer|null} A durable consumer, or `null` when looking up by identifier and none is registered.
     */
    getConsumer(streamNameOrIdentifier, identifier, initialState = {}, since = 0) {
        if (identifier === undefined) {
            return this.consumers.get(streamNameOrIdentifier) ?? null;
        }
        const streamName = streamNameOrIdentifier;
        if (this.consumers.has(identifier)) {
            const existingConsumer = this.consumers.get(identifier);
            if (existingConsumer.streamName === streamName) {
                return existingConsumer;
            }
            // Rebind identifier to the requested stream when a consumer with the same
            // identifier already exists for another stream.
            existingConsumer.stop();
        }
        const consumer = new Consumer(this.storage, streamName === '_all' ? '_all' : 'stream-' + streamName, identifier, initialState, since);
        consumer.streamName = streamName;
        this.consumers.set(identifier, consumer);
        return consumer;
    }

    /**
     * Scan the existing consumers on this EventStore and asynchronously invoke a callback with the parsed list.
     *
     * Each consumer entry provides `{ name, stream, identifier }` parsed from the on-disk filename.
     * Pass `autoStart = true` to eagerly open every discovered consumer and register it in
     * `this.consumers` so that it is immediately available for registry lookups.
     *
     * @param {function(error: Error|null, consumers: Array<{name: string, stream: string, identifier: string}>)} callback
     * @param {boolean} [autoStart=false] When true, calls `getConsumer(stream, identifier)` for each discovered consumer.
     */
    scanConsumers(callback, autoStart = false) {
        const consumersPath = path.join(this.storage.indexDirectory, 'consumers');
        if (!fs.existsSync(consumersPath)) {
            callback(null, []);
            return;
        }
        const regex = new RegExp(`^${this.storage.storageFile}\\.([^.]*\\..*)$`);
        const consumerNames = [];
        scanForFiles(consumersPath, regex, consumerNames.push.bind(consumerNames), /* istanbul ignore next */ (err) => {
            if (err) {
                return callback(err, []);
            }
            const consumers = consumerNames.map(name => {
                const splitIndex = name.lastIndexOf('.');
                const indexName = name.slice(0, splitIndex);
                const identifier = name.slice(splitIndex + 1);
                const stream = parseStreamFromIndexName(indexName);
                return { name, stream, identifier };
            });
            if (autoStart) {
                for (const { stream, identifier } of consumers) {
                    this.getConsumer(stream, identifier);
                }
            }
            callback(null, consumers);
        });
    }
}


EventStore.Storage = Storage;
EventStore.Index = Index;

export default EventStore;
export { ExpectedVersion, OptimisticConcurrencyError, CommitCondition, LOCK_THROW, LOCK_RECLAIM };
