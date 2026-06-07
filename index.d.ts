import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';

export type EventMatcher = Record<string, unknown> | ((payload: any, metadata?: any) => boolean);
export type EventPredicate = EventMatcher | ((buffer: Buffer) => boolean) | null;

export interface StreamIndexInfo {
    length: number;
    metadata?: Record<string, unknown> | null;
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
    fromStreams(streamName: string, streamNames: string[], minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream;
    getEventStreamForCategory(categoryName: string, minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream;

    query(types: string[], matcher?: EventMatcher | null, minRevision?: number, raw?: boolean): { stream: EventStream; condition: CommitCondition };
    createEventStream(streamName: string, matcher: EventMatcher, reindex?: boolean): EventStream;

    commit(
        streamName: string,
        events: any[],
        expectedVersion: number | CommitCondition,
        metadata: Record<string, unknown>,
        callback: (error: Error | null, commit?: Record<string, unknown>) => void
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




