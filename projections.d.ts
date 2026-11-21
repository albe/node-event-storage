import { EventEmitter } from 'node:events';
import type { DcbQuery } from './index.js';

/** Pure reducer for one event type. Must not have side effects. */
export type ProjectionHandler<S, E = any> = (state: S, event: E, metadata: Record<string, any>) => S;

export interface ProjectionDefinition<S> {
    initialState: S;
    /** Reducers keyed by event type. The keys are the event types the projection reads. */
    when: Record<string, ProjectionHandler<S>>;
    /** Tags every handled event must carry (AND). */
    tagFilter?: string[];
}

export interface Projection<S> {
    readonly initialState: S;
    /** The DcbQuery selecting all events this projection needs. */
    readonly query: DcbQuery;
    /** Apply one event. Events that do not match `query` return `state` unchanged. */
    apply(state: S, event: any, metadata: Record<string, any>, context: { type: string; tags: string[] }): S;
}

export type ProjectionState<P> = P extends Projection<infer S> ? S : never;
export type ComposedState<M extends Record<string, Projection<any>>> = { [K in keyof M]: ProjectionState<M[K]> };

export function createProjection<S>(definition: ProjectionDefinition<S>): Projection<S>;

export function composeProjections<M extends Record<string, Projection<any>>>(projections: M): Projection<ComposedState<M>>;

/** One event as delivered by an event source. */
export interface Envelope<E = any> {
    event: E;
    metadata: Record<string, any>;
    /** Global position of the event. */
    position: number;
    /** Event type, read from the stored event. */
    type: string;
    /** Event tags, read from the stored event. */
    tags: string[];
}

/** The events to read: a compiled selector and an optional matcher, e.g. a commit condition. */
export interface Selection {
    selector: string | Array<string | any[]>;
    matcher?: any;
}

/** A commit condition as returned by an event source. */
export interface SourceCondition extends Selection {
    noneMatchAfter: number;
}

/** The read side projections need from a store: `localSource(store)` or `httpSource(client)`. */
export interface EventSource<C extends SourceCondition = SourceCondition> {
    /** The events matching the query from `from` up to `condition.noneMatchAfter`, and the condition at the time of the query. */
    query(query: DcbQuery, options?: { from?: number }): Promise<{ events: AsyncIterable<Envelope>; condition: C }>;
    /** The events of a selection from `from` to the end of the store. */
    read(selection: Selection, options?: { from?: number }): AsyncIterable<Envelope>;
    /** The events of a selection from `from`, then every new one until `signal` aborts. */
    follow(selection: Selection, options?: { from?: number; signal?: AbortSignal }): AsyncIterable<Envelope>;
}

/** Projection state at a known store position plus the condition protecting a decision on it. Immutable. */
export class DecisionModel<S, C extends SourceCondition = SourceCondition> {
    readonly state: S;
    readonly commitCondition: C;
    /** Events matching `condition` (default: `commitCondition`) committed after this model was built, i.e. those that block a commit. */
    since(condition?: SourceCondition): AsyncIterable<Envelope>;
    /** Apply only the events committed since this model was built. Resolves to this model when nothing is new. */
    update(): Promise<DecisionModel<S, C>>;
}

export function buildDecisionModel<S, C extends SourceCondition>(source: EventSource<C>, projection: Projection<S>): Promise<DecisionModel<S, C>>;
export function buildDecisionModel<M extends Record<string, Projection<any>>, C extends SourceCondition>(source: EventSource<C>, projections: M): Promise<DecisionModel<ComposedState<M>, C>>;

/** Wrap a source so that projections see the transformed events. Type and tags are read before the transformation. */
export function transformEvents<C extends SourceCondition>(source: EventSource<C>, transform: (event: any, metadata: Record<string, any>) => any | Promise<any>): EventSource<C>;

export interface ReadModelSnapshot<S = any> {
    version: string | number | null;
    query: DcbQuery;
    position: number;
    state: S;
}

/** Where a read model keeps its state between restarts. */
export interface SnapshotStore {
    load(name: string): ReadModelSnapshot | null | Promise<ReadModelSnapshot | null>;
    save(name: string, snapshot: ReadModelSnapshot): void | Promise<void>;
}

export interface ReadModelOptions {
    /** Name of the read model; required with `snapshots`. */
    name?: string;
    /** Save state and position so a restart only reads the events committed since. State must be JSON-serializable. */
    snapshots?: SnapshotStore;
    /** Change it when the handlers change; a saved state of another version is rebuilt. */
    version?: string | number;
    /** Events applied during the catch-up before yielding to the event loop. Default 1000. */
    batchSize?: number;
}

/** A projection that follows an event source. Emits `'change'`, `'progress'`, `'caught-up'`, `'persisted'` and `'error'`. */
export class ReadModel<S> extends EventEmitter {
    readonly state: S;
    /** Global position of the last event this read model applied. */
    readonly position: number;
    /** Whether the catch-up reached the end of the store and new commits are applied live. */
    readonly caughtUp: boolean;
    /** Resolves once the catch-up reached the end of the store; rejects when closed before. */
    readonly ready: Promise<this>;
    /** A decision model from the current state, reading only the events not applied yet. */
    decisionModel(): Promise<DecisionModel<S>>;
    /** Stop following the source and save a pending snapshot. */
    close(): Promise<void>;
}

export function createReadModel<S>(source: EventSource, projection: Projection<S>, options?: ReadModelOptions): ReadModel<S>;
export function createReadModel<M extends Record<string, Projection<any>>>(source: EventSource, projections: M, options?: ReadModelOptions): ReadModel<ComposedState<M>>;
