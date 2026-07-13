import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';

// ─── Shared types ─────────────────────────────────────────────────────────────

export type Scalar = string | number | boolean;

/** Recursive field matcher (subset of MongoDB query syntax). */
export type ObjectMatcher = {
    [field: string]:
        | Scalar
        | Scalar[]
        | { $ne?: Scalar; $gt?: number; $gte?: number; $lt?: number; $lte?: number }
        | ObjectMatcher;
};

export type EventPredicate = ObjectMatcher | ((payload: any, metadata?: any) => boolean) | ((buffer: Buffer) => boolean) | null;

/** Nested selector for join algebra: flat `['a','b']` = OR; `[['a','b'],'c']` = AND/OR alternating depth. */
export type SelectorNode = string | SelectorNode[];

export interface StoredEvent<P = Record<string, unknown>> {
    stream: string;
    type?: string;
    payload: P;
    metadata: Record<string, unknown>;
    /** 1-based global position: `commitId + commitVersion + 1`. */
    globalPosition: number;
    /** 1-based revision within the originating stream. */
    streamVersion: number;
}

export interface CommitResult {
    commitId: number;
    committedAt: number;
    streamName: string;
    streamVersion: number;
    events: object[];
    [key: string]: unknown;
}

export const ExpectedVersion: { readonly Any: -1; readonly EmptyStream: 0 };
export const LOCK_THROW: number;
export const LOCK_RECLAIM: number;
export const VERSION: string;

export class OptimisticConcurrencyError extends Error {}

export class CommitCondition {
    constructor(selector: SelectorNode[] | string, matcher: ObjectMatcher | null | undefined, noneMatchAfter: number, raw?: boolean);
    /** The (normalized) stream selector passed at construction. */
    selector: SelectorNode[] | string;
    /** Backwards-compatible alias: flat leaf stream names of the selector. */
    types: string[];
    matcher: ObjectMatcher | null;
    noneMatchAfter: number;
    raw: boolean;
}

// ─── Fluent 2.0 query API ─────────────────────────────────────────────────────

export interface SubscribeOptions {
    signal?: AbortSignal;
}

export class RangeQuery<P = Record<string, unknown>> implements AsyncIterable<StoredEvent<P>> {
    from(position: number): this;
    matching(matcher: ObjectMatcher): this;
    until(position: number): this;
    to(position: number): this;
    first(n: number): this;
    limit(n: number): this;
    backwards(n?: number): this;
    raw(): this;
    [Symbol.iterator](): Iterator<StoredEvent<P>>;
    [Symbol.asyncIterator](): AsyncIterator<StoredEvent<P>>;
    toArray(): Promise<StoredEvent<P>[]>;
}

export class EventStreamResult<P = Record<string, unknown>> implements AsyncIterable<StoredEvent<P>> {
    readonly condition: CommitCondition | null;
    readonly range: { from: number; until: number };
    [Symbol.asyncIterator](): AsyncIterator<StoredEvent<P>>;
    toArray(): Promise<StoredEvent<P>[]>;
}

export class EventStreamQuery<P = Record<string, unknown>> implements AsyncIterable<StoredEvent<P>> {
    from(position: number): this;
    matching(matcher: ObjectMatcher): this;
    raw(): this;
    range(): RangeQuery<P>;
    query(): Promise<EventStreamResult<P>>;
    subscribe(options?: SubscribeOptions): AsyncGenerator<StoredEvent<P>, void, undefined>;
    [Symbol.iterator](): Iterator<StoredEvent<P>>;
    [Symbol.asyncIterator](): AsyncIterator<StoredEvent<P>>;
    toArray(): Promise<StoredEvent<P>[]>;
}

/** Fluent query for a single stream; adds stream-relative bounded read. */
export class StreamQuery<P = Record<string, unknown>> extends EventStreamQuery<P> {
    /** Bounded read by stream-relative revision (faster; aggregate consistency). */
    streamRange(): RangeQuery<P>;
}

export interface ConsumerOptions<S> {
    source: EventStreamQuery;
    apply?: (state: S | null, event: StoredEvent) => S;
    initialState?: S;
}

export class Consumer<S = unknown> extends EventEmitter {
    constructor(name: string, options: ConsumerOptions<S>);
    readonly name: string;
    readonly state: S | null;
    readonly checkpoint: number;
    setState(newState?: S | ((prev: S | null) => S)): void;
    run(options?: { signal?: AbortSignal; persistEvery?: number | 'idle' }): Promise<void>;
    stop(): void;
    reset(options?: { initialState?: S; from?: number }): Promise<void>;
    consume(options?: { signal?: AbortSignal }): AsyncGenerator<StoredEvent, void, undefined>;
    whenReached(position: number, options?: { timeoutMs?: number; signal?: AbortSignal }): Promise<number>;
}

// ─── Classic API (pre-2.0 compatibility layer) ────────────────────────────────

export class EventStream extends Readable {
    name: string;
    raw: boolean;
    version: number;
    minRevision: number;
    maxRevision: number;
    from(revision: number): this;
    until(revision: number): this;
    fromStart(): this;
    fromEnd(): this;
    backwards(amount?: number): this;
    forwards(amount?: number): this;
    first(amount: number): this;
    last(amount: number): this;
    where(predicate?: EventPredicate): this;
    next(): object | false;
    forEach(callback: (payload: any, metadata: any, stream: string) => void): void;
    reset(): this;
    get events(): object[];
    [Symbol.iterator](): Iterator<object>;
}

export class LegacyConsumer extends Readable {
    constructor(storage: Storage, indexName: string, identifier: string, initialState?: object, startFrom?: number);
    position: number;
    state: object;
    start(): void;
    stop(): void;
    reset(initialState?: object, startFrom?: number): void;
    setState(state: object | ((prev: object) => object), persist?: boolean): void;
}

export class Storage extends EventEmitter {
    static ReadOnly: typeof Storage;
    initialized: boolean | null;
    length: number;
    indexDirectory: string;
    storageFile: string;
    serializer: { serialize: (d: object) => string; deserialize: (s: string) => object };
}

export class StorageLockedError extends Error {}

export class Index {
    static ReadOnly: typeof Index;
    length: number;
    metadata?: Record<string, unknown> | null;
}

// ─── EventStore ───────────────────────────────────────────────────────────────

export class EventStore extends EventEmitter {
    constructor(storeName?: string, config?: {
        storageDirectory?: string;
        streamsDirectory?: string;
        storageConfig?: Record<string, unknown>;
        readOnly?: boolean;
        typeAccessor?: string | ((event: any) => string);
        streamMetadata?: object | ((streamName: string) => object);
        useLegacyConsumer?: boolean;
    });

    static Storage: typeof Storage;
    static Index: typeof Index;
    static VERSION: string;

    storage: Storage;
    streams: Record<string, { index: Index; closed?: boolean }>;
    length: number;
    typeAccessor: ((event: any) => string) | null;
    useLegacyConsumer: boolean;

    // ─── Fluent 2.0 query API ───────────────────────────────────────────────

    /** Single stream addressed by global position; `.streamRange()` for stream-relative. */
    stream(name: string): StreamQuery;
    /** Whole event log in global-position order. */
    all(): EventStreamQuery;
    /** Category: all streams with this prefix (dash or slash separator). */
    category(name: string): EventStreamQuery;
    /**
     * Join over an explicit stream selector. Accepts flat OR nested algebra:
     *   `['a','b']` → a OR b
     *   `[['a','b'],'c']` → (a AND b) OR c
     */
    join(streams: SelectorNode[]): EventStreamQuery;

    // ─── Commit ─────────────────────────────────────────────────────────────

    /** Unified 2.0 form. */
    commit(streamName: string, events: object | object[], options?: {
        expectedVersion?: number;
        condition?: CommitCondition;
        metadata?: object;
    }): Promise<CommitResult>;

    /** Legacy positional form (b/c). */
    commit(streamName: string, events: object | object[], expectedVersion?: number | CommitCondition, metadata?: object, callback?: (commit: CommitResult) => void): Promise<CommitResult> | void;

    // ─── Fluent 2.0 consumer registry ───────────────────────────────────────
    createConsumer<S = unknown>(name: string, options: ConsumerOptions<S>): Consumer<S>;
    findConsumer<S = unknown>(name: string): Consumer<S> | null;

    // ─── Classic API ─────────────────────────────────────────────────────────

    getStreamVersion(streamName: string): number;
    getEventStream(streamName: string, minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream | false;
    getAllEvents(minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream;
    fromStreams(streamName: string, selector: SelectorNode[], minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream;
    query(selector: SelectorNode[], matcher?: EventPredicate, minRevision?: number, raw?: boolean): { stream: EventStream; condition: CommitCondition };
    getEventStreamForCategory(categoryName: string, minRevision?: number, maxRevision?: number, predicate?: EventPredicate, raw?: boolean): EventStream;
    createEventStream(streamName: string, matcher: EventPredicate, reindex?: boolean): EventStream;
    deleteEventStream(streamName: string): void;
    closeEventStream(streamName: string): void;
    preCommit(hook: (event: any, partitionMetadata: any) => void): void;
    preRead(hook: (position: number, partitionMetadata: any) => void): void;
    makeReadOnly(callback?: () => void): void;
    close(): void;

    /** @deprecated Use createConsumer() instead. */
    getConsumer(identifier: string): LegacyConsumer | null;
    /** @deprecated Use createConsumer() instead. */
    getConsumer(streamName: string, identifier: string, initialState?: object, since?: number): LegacyConsumer;
    scanConsumers(callback: (error: Error | null, consumers: Array<{ name: string; stream: string; identifier: string }>) => void, autoStart?: boolean): void;
}

export function matches(document: Record<string, unknown>, matcher: Record<string, unknown>): boolean;
export function buildRawBufferMatcher(matcher: Record<string, unknown>): (buffer: Buffer, startOffset?: number) => boolean;

export default EventStore;
