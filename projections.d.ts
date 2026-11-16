import { EventEmitter } from 'node:events';
import type { CommitCondition, DcbQuery, EventStore, EventStream } from './index.js';

/** Pure reducer for one event type. Must not have side effects. */
export type ProjectionHandler<S, E = any> = (state: S, event: E, metadata: Record<string, any>) => S;

export interface ProjectionDefinition<S> {
    initialState: S;
    /** Reducers keyed by event type. The keys are the event types the projection reads. */
    handlers: Record<string, ProjectionHandler<S>>;
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

/** Projection state at a known store position plus the condition protecting a decision on it. Immutable. */
export class DecisionModel<S> {
    readonly state: S;
    readonly appendCondition: CommitCondition;
    /** Events matching `condition` (default: `appendCondition`) committed after this model was built, i.e. those that block a commit. */
    since(condition?: CommitCondition): EventStream;
    /** Apply only the events committed since this model was built. Returns this model when nothing is new. */
    update(): DecisionModel<S>;
}

export function buildDecisionModel<S>(eventStore: EventStore, projection: Projection<S>): DecisionModel<S>;
export function buildDecisionModel<M extends Record<string, Projection<any>>>(eventStore: EventStore, projections: M): DecisionModel<ComposedState<M>>;

export interface ReadModelOptions {
    /** Name of the read model; required when `persist` is set. */
    name?: string;
    /** Persist state and position so a restart only reads the events committed since. State must be JSON-serializable. */
    persist?: boolean;
    /** Change it when the handlers change; a persisted state of another version is rebuilt. */
    version?: string | number;
}

/** A projection that follows the store. Emits `'change'` with `(state, position)` and, when persisted, `'persisted'`. */
export class ReadModel<S> extends EventEmitter {
    readonly state: S;
    /** Global position of the last event this read model has seen. */
    readonly position: number;
    /** Synchronously apply all matching events committed since `position`. */
    update(until?: number): this;
    /** A decision model from the current state and position. */
    decisionModel(): DecisionModel<S>;
    /** Stop following the store and write a pending persist. */
    close(): void;
}

export function createReadModel<S>(eventStore: EventStore, projection: Projection<S>, options?: ReadModelOptions): ReadModel<S>;
export function createReadModel<M extends Record<string, Projection<any>>>(eventStore: EventStore, projections: M, options?: ReadModelOptions): ReadModel<ComposedState<M>>;
