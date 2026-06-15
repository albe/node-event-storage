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

export class CommitCondition {
    constructor(types: string[], matcher: EventMatcher | null | undefined, noneMatchAfter: number, raw?: boolean);
    types: string[];
    matcher: EventMatcher | null;
    raw: boolean;
    noneMatchAfter: number;
}

export class OptimisticConcurrencyError extends Error {}

export const ExpectedVersion: {
    Any: -1;
    EmptyStream: 0;
};

// New class-based expected values for commit() Phase A
export class ExpectedStreamVersion {
    constructor(streamVersion: number);
    streamVersion: number;
}
export class ExpectedGlobalSequenceNumber {
    constructor(sequenceNumber: number);
    sequenceNumber: number;
}
export class ExpectedAny extends ExpectedStreamVersion {
    streamVersion: -1;
}
export class ExpectedEmpty extends ExpectedStreamVersion {
    streamVersion: 0;
}
export class ExpectedCondition extends CommitCondition {
    constructor(condition: CommitCondition);
}
export type ExpectedVersionLike = number | ExpectedStreamVersion | ExpectedGlobalSequenceNumber | ExpectedAny | ExpectedEmpty | ExpectedCondition | CommitCondition;

export const ExpectStream: {
    AtVersion: (streamVersion: number) => ExpectedStreamVersion;
    AtGlobalSequence: (sequenceNumber: number) => ExpectedGlobalSequenceNumber;
    Any: () => ExpectedAny;
    Empty: () => ExpectedEmpty;
    MatchCondition: (condition: CommitCondition) => ExpectedCondition;
};

// Stream range options for Phase A
export interface StreamVersionRangeOptions {
    fromStreamVersion?: number;
    toStreamVersion?: number;
    predicate?: EventPredicate;
    raw?: boolean;
}
export interface SequenceNumberRangeOptions {
    fromSequenceNumber?: number;
    toSequenceNumber?: number;
    predicate?: EventPredicate;
    raw?: boolean;
}
export type StreamRangeOptions = StreamVersionRangeOptions | SequenceNumberRangeOptions;
export interface StreamReadOptions extends StreamVersionRangeOptions, SequenceNumberRangeOptions {}

export const VERSION: string;

export class EventStream extends Readable {
    constructor(name: string, eventStore: EventStore, minStreamVersion?: number, maxStreamVersion?: number, predicate?: EventPredicate, raw?: boolean);
    name: string;
    raw: boolean;
    version: number;
    minRevision: number;
    maxRevision: number;
    streamIndex: StreamIndexInfo;
    predicate: EventPredicate;

    from(streamVersion: number): this;
    until(streamVersion: number): this;
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
}

export class Consumer extends Readable {
    constructor(storage: Storage, indexName: string, identifier: string, initialState?: Record<string, unknown>, startFrom?: number);
    streamName: string;
    position: number;
    state: Record<string, unknown>;
    start(): this;
    stop(): void;
    reset(initialState?: Record<string, unknown>, startFrom?: number): this;
    setState(state: Record<string, unknown>): this;
}

export class Storage extends EventEmitter {
    static ReadOnly: typeof Storage;
    static StorageLockedError: typeof StorageLockedError;
    static LOCK_THROW: number;
    static LOCK_RECLAIM: number;

    initialized: boolean | null;
    length: number;
    indexDirectory: string;
    storageFile: string;
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

    constructor(storeName?: string, config?: Record<string, unknown>);

    open(callback?: (err?: Error | null) => void): void;
    close(callback?: (err?: Error | null) => void): void;

    getStreamVersion(streamName: string): number;

    // Unified stream API:
    // - regular stream name => single stream
    // - `_all` => all events
    // - category selector ending with '-' or '/' => joined category stream
    getStream(streamName: string | string[], options?: StreamReadOptions): EventStream;

    // Legacy positional API (keep for BC)
    getEventStream(streamName: string, minStreamVersion?: number, maxStreamVersion?: number, predicate?: EventPredicate, raw?: boolean): EventStream | false;

    // New options-based API (Phase A)
    getEventStream(streamName: string, options: StreamRangeOptions): EventStream | false;

    getAllEvents(minSequenceNumber?: number, maxSequenceNumber?: number, predicate?: EventPredicate, raw?: boolean): EventStream;
    getAllEvents(options: SequenceNumberRangeOptions): EventStream;
    fromStreams(streamName: string, streamNames: string[], fromSequenceNumber?: number, toSequenceNumber?: number, predicate?: EventPredicate, raw?: boolean): EventStream;
    fromStreams(streamName: string, streamNames: string[], options: SequenceNumberRangeOptions): EventStream;
    getEventStreamForCategory(categoryName: string, fromSequenceNumber?: number, toSequenceNumber?: number, predicate?: EventPredicate, raw?: boolean): EventStream;
    getEventStreamForCategory(categoryName: string, options: SequenceNumberRangeOptions): EventStream;

    query(types: string[], matcher?: EventMatcher | null, minSequenceNumber?: number, raw?: boolean): { stream: EventStream; condition: CommitCondition };
    query(types: string[], matcher: EventMatcher | null | undefined, options: SequenceNumberRangeOptions): { stream: EventStream; condition: CommitCondition };
    createEventStream(streamName: string, matcher: EventMatcher, reindex?: boolean): EventStream;

    commit(
        streamName: string,
        events: any[],
        expectedVersion?: ExpectedVersionLike,
        metadata?: Record<string, unknown>,
        callback?: (error: Error | null, commit?: Record<string, unknown>) => void
    ): void;

    commit(
        streamName: string,
        events: any[],
        metadata?: Record<string, unknown>,
        callback?: (error: Error | null, commit?: Record<string, unknown>) => void
    ): void;

    getConsumer(streamName: string, identifier: string, initialState?: Record<string, unknown>, since?: number): Consumer;
    getConsumer(identifier: string): Consumer | null;
    scanConsumers(
        callback: (error: Error | null, consumers: Array<{ name: string; stream: string; identifier: string }>) => void,
        autoStart?: boolean
    ): void;
}

export const LOCK_THROW: number;
export const LOCK_RECLAIM: number;

export function matches(document: Record<string, unknown>, matcher: Record<string, unknown>): boolean;
export function buildRawBufferMatcher(matcher: Record<string, unknown>): (buffer: Buffer, startOffset?: number) => boolean;

export default EventStore;




