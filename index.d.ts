import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';

export type EventMatcher = Record<string, unknown> | ((payload: any, metadata?: any) => boolean);
export type EventPredicate = EventMatcher | ((buffer: Buffer) => boolean) | null;

export interface IndexEntry {
    number: number;
    position: number;
    size: number;
    partition: number;
}
export interface StreamIndexInfo {
    length: number;
    metadata?: Record<string, unknown> | null;
    lastEntry: IndexEntry;
}

export interface StreamInfo {
    index: StreamIndexInfo;
    matcher?: EventMatcher;
    closed?: boolean;
}

/** Stream selector tree: depth 0 = OR, depth 1 = AND, depth 2 = OR, ... */
export type StreamSelector = Array<string | StreamSelector>;

export interface DcbQueryItem {
    types?: string[];
    tags?: string[];
}

export interface DcbQuery {
    items: DcbQueryItem[];
}

export interface Commit {
    commitId: number;
    committedAt: number;
    streamName: string;
    streamVersion: number;
    events: any[];
    [metadata: string]: unknown;
}

export type CommitCallback = (commit: Commit) => void;

export interface Serializer {
    serialize(document: any): string | Buffer;
    deserialize(data: string | Buffer): any;
}

export interface LockOwner {
    pid: number;
    hostname: string;
    pidNamespace: string | null;
    lockedAt: string;
}

export interface StorageConfig {
    /** Directory for partition (event) files. Defaults to `storageDirectory`. */
    dataDirectory?: string;
    /** Directory for index files. Defaults to `streamsDirectory`. */
    indexDirectory?: string;
    /** File name of the primary index. Default `'{storeName}.index'`. */
    indexFile?: string;
    /** Returns the partition (file) name for a document. */
    partitioner?: (document: any, sequenceNumber: number) => string;
    /** Custom serialization or compression. Default JSON. */
    serializer?: Serializer;
    /** Read buffer size in bytes. Default 4096. */
    readBufferSize?: number;
    /** Write buffer size in bytes. Default 16384. */
    writeBufferSize?: number;
    /** Flush after this many buffered documents. Default 0 (unlimited). */
    maxWriteBufferDocuments?: number;
    /** Call fsync on every flush. Default false. */
    syncOnFlush?: boolean;
    /** Group commit: sync all changed partitions together at most every this many ms and only then invoke the commit callbacks. Default 0 (off). */
    syncInterval?: number;
    /** Let the writer read events that are still in the write buffer. Default true. */
    dirtyReads?: boolean;
    /** How to handle an existing lock on open: `LOCK_AUTO` (default), `LOCK_THROW` or `LOCK_RECLAIM`. */
    lock?: number;
    /** Secret used to verify persisted function matchers. */
    hmacSecret?: string;
    /** Metadata written to each partition header. */
    metadata?: Record<string, unknown> | ((partitionName: string) => Record<string, unknown>);
    /** Document paths used for O(1) routing of writes to indexes. Default `['stream', 'payload.type']`. */
    matcherProperties?: string[];
    /** Maximum open partition file descriptors (0 = unlimited). Default 1024. */
    maxOpenPartitions?: number;
    /** Maximum open index file descriptors (0 = unlimited). Default 1024. */
    maxOpenIndexes?: number;
    /** Options passed to every index on construction. */
    indexOptions?: Record<string, unknown>;
}

export interface StreamSource {
    /** Dot-notation path into the event payload. */
    path: string;
    /** Builds the stream name from the value at `path`. */
    nameBuilder: (value: string) => string;
}

export interface EventStoreConfig {
    /** Directory for data files. Default `'./data'`. */
    storageDirectory?: string;
    /** Directory for stream index files. Default `'{storageDirectory}/streams'`. */
    streamsDirectory?: string;
    /** Open the store in read-only mode. Default false. */
    readOnly?: boolean;
    /** Dot-notation path to the event type; maintains one stream per type value. */
    typeAccessor?: string;
    /** Dot-notation path to an array of tag strings; maintains one stream `tags/<tag>` per tag value. */
    tagsAccessor?: string;
    /** Generic automatically maintained streams. */
    streamSources?: StreamSource[];
    /** Metadata stored once per stream at creation. */
    streamMetadata?: Record<string, Record<string, unknown>> | ((streamName: string) => Record<string, unknown>);
    /** Storage options. */
    storageConfig?: StorageConfig;
}

export class CommitCondition {
    constructor(selector: StreamSelector | string, matcher: EventMatcher | null | undefined, noneMatchAfter: number, raw?: boolean);
    selector: StreamSelector | string;
    matcher: EventMatcher | null;
    raw: boolean;
    noneMatchAfter: number;
    /** A narrowed condition at the same position that ignores new events in the given streams (e.g. event types). */
    without(...streams: Array<string | string[]>): CommitCondition;
}

export class OptimisticConcurrencyError extends Error {}

export const ExpectedVersion: {
    Any: -1;
    EmptyStream: 0;
};

export const VERSION: string;

export class EventStream extends Readable {
    constructor(name: string, eventStore: EventStore, minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean);
    name: string;
    raw: boolean;
    version: number;
    minRevision: number;
    maxRevision: number;
    streamIndex: StreamIndexInfo;
    predicate: EventPredicate;

    from(revision: number): this;
    until(revision: number): this;
    fromStart(): this;
    fromEnd(): this;
    previous(amount: number): this;
    following(amount: number): this;
    backwards(): this;
    forwards(): this;
    where(predicate?: EventPredicate): this;
    filter(predicate?: EventPredicate): this;
    filter(
        predicate: (chunk: any, options?: { signal?: AbortSignal }) => boolean | Promise<boolean>,
        options: { concurrency?: number; signal?: AbortSignal }
    ): Readable;
    next(): any | false;
    [Symbol.iterator](): Iterator<any>;
}

export class Consumer extends Readable {
    constructor(storage: Storage, indexName: string, identifier: string, initialState?: Record<string, unknown>, startFrom?: number);
    streamName: string;
    position: number;
    state: Record<string, unknown>;
    start(): this;
    stop(): void;
    reset(initialState?: Record<string, unknown>, startFrom?: number): this;
    setState(state: Record<string, unknown> | ((state: Record<string, unknown>) => Record<string, unknown>), persist?: boolean): void;
}

export class Storage extends EventEmitter {
    static ReadOnly: typeof Storage;
    static StorageLockedError: typeof StorageLockedError;
    static LOCK_THROW: number;
    static LOCK_RECLAIM: number;
    static LOCK_AUTO: number;

    constructor(storageName?: string, config?: StorageConfig);
    constructor(config: StorageConfig);

    initialized: boolean | null;
    length: number;
    indexDirectory: string;
    storageFile: string;

    /** The process holding the lock, or null if the storage is not locked or the lock has no owner information. */
    readLockOwner(): LockOwner | null;

    /** Close the storage. A writer calls back once it synced everything and released the lock. */
    close(callback?: (err?: Error | null) => void): void;
}

export class StorageLockedError extends Error {}

export class Index {
    static ReadOnly: typeof Index;
    static Entry: unknown;

    length: number;
    metadata?: Record<string, unknown> | null;
}

export class EventStore extends EventEmitter {
    static Storage: typeof Storage;
    static Index: typeof Index;
    static VERSION: string;

    storage: Storage;
    streams: Record<string, StreamInfo>;
    consumers: Map<string, Consumer>;
    length: number;

    constructor(storeName?: string, config?: EventStoreConfig);
    constructor(config: EventStoreConfig);

    open(callback?: (err?: Error | null) => void): void;
    close(callback?: (err?: Error | null) => void): void;

    getStreamVersion(streamName: string): number;
    getEventStream(streamName: string, minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream | false;
    getAllEvents(minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream;
    fromStreams(streamName: string, streamNames: StreamSelector, minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream;
    getEventStreamForCategory(categoryName: string, minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream;

    query(selectorOrDcbQuery: StreamSelector | DcbQuery, matcher?: EventMatcher | null, minRevision?: number, raw?: boolean): { stream: EventStream; condition: CommitCondition };
    createEventStream(streamName: string, matcher: EventMatcher, reindex?: boolean): EventStream;

    commit(
        streamName: string,
        events: any | any[],
        expectedVersion?: number | CommitCondition,
        metadata?: Record<string, unknown>,
        callback?: CommitCallback
    ): Commit;
    commit(streamName: string, events: any | any[], expectedVersion: number | CommitCondition, callback: CommitCallback): Commit;
    commit(streamName: string, events: any | any[], metadata: Record<string, unknown>, callback?: CommitCallback): Commit;
    commit(streamName: string, events: any | any[], callback: CommitCallback): Commit;

    getConsumer(streamName: string, identifier: string, initialState?: Record<string, unknown>, since?: number): Consumer;
    getConsumer(identifier: string): Consumer | null;
    scanConsumers(
        callback: (error: Error | null, consumers: Array<{ name: string; stream: string; identifier: string }>) => void,
        autoStart?: boolean
    ): void;
}

export const LOCK_THROW: number;
export const LOCK_RECLAIM: number;
export const LOCK_AUTO: number;

export function matches(document: Record<string, unknown>, matcher: Record<string, unknown>): boolean;
export function buildRawBufferMatcher(matcher: Record<string, unknown>): (buffer: Buffer, startOffset?: number) => boolean;

export default EventStore;




