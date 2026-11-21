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

export type CommitCallback = (commit: Record<string, unknown>) => void;

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
    ): void;
    commit(streamName: string, events: any | any[], expectedVersion: number | CommitCondition, callback: CommitCallback): void;
    commit(streamName: string, events: any | any[], metadata: Record<string, unknown>, callback?: CommitCallback): void;
    commit(streamName: string, events: any | any[], callback: CommitCallback): void;

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




